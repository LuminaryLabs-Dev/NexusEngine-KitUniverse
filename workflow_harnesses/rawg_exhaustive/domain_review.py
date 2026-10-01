from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash
from workflow_harnesses.rawg_matrix_optimizer.workflow_rawg_matrix_optimizer import ShardedJsonlWriter

from .codex_runner import DEFAULT_CODEX_MODEL, DEFAULT_REASONING_EFFORT, peak_overlap, run_codex_lane
from .domain_architecture import is_architectural_domain


DOMAIN_REVIEW_SCHEMA = "kituniverse.domain-canonical-decision.v1"
DOMAIN_RECONCILIATION_SCHEMA = "kituniverse.domain-canonical-revision.v1"
DOMAIN_REVIEW_PROMPT_VERSION = "domain-pillar-review-v2-global-reconciliation"


def organize_accepted_domains(
    node: Dict[str, Any], controls: Dict[str, Any], workspace: Path, max_batches_override: Optional[int]
) -> Dict[str, Any]:
    config = node.get("config") or {}
    model = str(config.get("model") or DEFAULT_CODEX_MODEL)
    reasoning_effort = str(config.get("reasoning_effort") or DEFAULT_REASONING_EFFORT)
    max_concurrency = max(1, min(16, int(config.get("max_concurrency") or 1)))
    batch_size = max(1, min(100, int(config.get("batch_size") or 40)))
    retries = max(0, min(2, int(config.get("retries") or 1)))
    timeout_seconds = int(config.get("timeout_seconds") or 900)
    batch_limit = max_batches_override if max_batches_override is not None else int(config.get("max_batches") or 0)
    shards = workspace / "shards"
    prior = load_active_domain_mappings(shards)
    refined = {
        str(item["master_kit_id"]): item
        for item in _read_jsonl(shards.glob("refined-master-kits-*.jsonl"))
        if item.get("master_kit_id")
    }
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for decision in _read_jsonl(shards.glob("master-codex-decisions-*.jsonl")):
        if decision.get("accepted") is not True:
            continue
        master_id = str(decision["master_kit_id"])
        fallback = refined.get(master_id, {}).get("contract")
        contract = decision.get("contract") if isinstance(decision.get("contract"), dict) else fallback
        if not isinstance(contract, dict):
            continue
        source_domain = slug(contract.get("domain"))
        if not source_domain:
            continue
        grouped.setdefault(source_domain, []).append({
            "master_kit_id": master_id,
            "name": contract.get("name"),
            "owns": contract.get("owns"),
            "does_not_own": contract.get("does_not_own"),
            "subdomain": contract.get("subdomain"),
        })
    source_domains = sorted(domain for domain in grouped if domain not in prior)
    batches = [source_domains[index : index + batch_size] for index in range(0, len(source_domains), batch_size)]
    if batch_limit:
        batches = batches[:batch_limit]
    review_root = workspace / "codex-domain-review"
    jobs = []
    epoch = str(controls.get("pipeline_epoch") or "epoch")
    for batch in batches:
        batch_id = f"domains-{epoch[:12]}-{stable_hash(batch)[:20]}"
        packet = {
            "schema_version": "kituniverse.domain-review-packet.v1",
            "assigned_source_domains": batch,
            "all_current_source_domains": source_domains,
            "samples": {domain: grouped[domain][:5] for domain in batch},
        }
        packet_path = review_root / "packets" / f"{batch_id}.json"
        _write_json(packet_path, packet)
        jobs.append((batch_id, batch, packet_path))
    reports: List[Dict[str, Any]] = []
    stage_started = time.monotonic()
    with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
        futures = {
            executor.submit(
                _review_batch,
                Path.cwd(),
                review_root,
                workspace / ".codex-scratch" / "domain-review",
                batch_id,
                batch,
                packet_path,
                model,
                reasoning_effort,
                retries,
                timeout_seconds,
            ): batch_id
            for batch_id, batch, packet_path in jobs
        }
        for future in as_completed(futures):
            try:
                reports.append(future.result())
            except Exception as exc:
                reports.append({"ok": False, "batch_id": futures[future], "error": str(exc)})
    writer = ShardedJsonlWriter(shards, "domain-canonical-decisions", int(controls["shard_max_bytes"]))
    mapped = 0
    for report in sorted(reports, key=lambda item: str(item.get("batch_id") or "")):
        if report.get("ok") is not True:
            continue
        for mapping in report.get("mappings") or []:
            writer.append({
                "schema_version": DOMAIN_REVIEW_SCHEMA,
                "pipeline_epoch": controls.get("pipeline_epoch"),
                "source_domain": mapping["source_domain"],
                "target_domain": mapping["target_domain"],
                "reason": mapping["reason"],
                "model": model,
                "reasoning_effort": reasoning_effort,
                "prompt_version": DOMAIN_REVIEW_PROMPT_VERSION,
                "batch_id": report["batch_id"],
                "packet_path": report.get("packet_path"),
                "raw_output": report.get("raw_output"),
            })
            prior[mapping["source_domain"]] = mapping
            mapped += 1
    reconciliation = _reconcile_active_targets(
        repo_root=Path.cwd(),
        review_root=review_root,
        scratch_root=workspace / ".codex-scratch" / "domain-review",
        shards=shards,
        grouped=grouped,
        active=prior,
        controls=controls,
        model=model,
        reasoning_effort=reasoning_effort,
        retries=retries,
        timeout_seconds=timeout_seconds,
    )
    prior = load_active_domain_mappings(shards)
    elapsed = max(0.001, time.monotonic() - stage_started)
    failed = [item for item in reports if item.get("ok") is not True]
    reconciliation_ok = reconciliation.get("ok") is True
    summary = {
        "schema_version": "kituniverse.domain-review-stage.v1",
        "pipeline_epoch": controls.get("pipeline_epoch"),
        "ok": reconciliation_ok,
        "status": (
            "hold"
            if not reconciliation_ok
            else ("complete-with-retries-needed" if failed else ("limit-complete" if batch_limit else "complete"))
        ),
        "reason": None if reconciliation_ok else "domain-global-reconciliation-failed",
        "model": model,
        "reasoning_effort": reasoning_effort,
        "max_concurrency": max_concurrency,
        "peak_active_lanes": peak_overlap(reports),
        "source_domains": len(grouped),
        "pending_source_domains": len(source_domains),
        "new_mappings": mapped,
        "total_mappings": len(prior),
        "canonical_domains": len({str(item.get("target_domain")) for item in prior.values()}),
        "failed_batches": len(failed),
        "reconciliation": reconciliation,
        "elapsed_seconds": round(elapsed, 3),
        "reports": [
            {key: item.get(key) for key in ("batch_id", "ok", "attempts", "elapsed_seconds", "error", "packet_path", "raw_output", "execution_report")}
            for item in reports
        ],
    }
    _write_json(review_root / "reports" / f"{epoch}.json", summary)
    return summary


def load_active_domain_mappings(shards: Path) -> Dict[str, Dict[str, Any]]:
    """Project append-only canonical decisions plus later corrective revisions."""
    active: Dict[str, Dict[str, Any]] = {}
    for item in _read_jsonl(shards.glob("domain-canonical-decisions-*.jsonl")):
        if item.get("source_domain") and item.get("target_domain"):
            active[str(item["source_domain"])] = item
    for item in _read_jsonl(shards.glob("domain-canonical-revisions-*.jsonl")):
        if item.get("source_domain") and item.get("target_domain"):
            active[str(item["source_domain"])] = item
    return active


def _reconcile_active_targets(
    *,
    repo_root: Path,
    review_root: Path,
    scratch_root: Path,
    shards: Path,
    grouped: Dict[str, List[Dict[str, Any]]],
    active: Dict[str, Dict[str, Any]],
    controls: Dict[str, Any],
    model: str,
    reasoning_effort: str,
    retries: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    if not active:
        return {"ok": True, "status": "not-needed", "revisions": 0}
    targets = sorted({slug(item.get("target_domain")) for item in active.values() if item.get("target_domain")})
    source_groups: Dict[str, List[str]] = {}
    for source, item in active.items():
        source_groups.setdefault(slug(item.get("target_domain")), []).append(source)
    identity = stable_hash({"prompt_version": DOMAIN_REVIEW_PROMPT_VERSION, "active": sorted(
        (source, slug(item.get("target_domain"))) for source, item in active.items()
    )})
    report_path = review_root / "reconciliations" / f"{identity}.json"
    if report_path.is_file():
        try:
            prior_report = json.loads(report_path.read_text(encoding="utf-8"))
            if prior_report.get("ok") is True:
                return {**prior_report, "status": "already-reconciled"}
        except (OSError, json.JSONDecodeError):
            pass
    packet = {
        "schema_version": "kituniverse.domain-global-reconciliation-packet.v1",
        "identity": identity,
        "assigned_target_domains": targets,
        "source_groups": {target: sorted(source_groups.get(target) or []) for target in targets},
        "samples": {
            target: [sample for source in source_groups.get(target, []) for sample in grouped.get(source, [])][:8]
            for target in targets
        },
    }
    packet_path = review_root / "packets" / f"domain-reconcile-{identity}.json"
    _write_json(packet_path, packet)
    batch_id = f"domain-reconcile-{identity[:20]}"
    report = _review_target_batch(
        repo_root,
        review_root,
        scratch_root,
        batch_id,
        targets,
        packet_path,
        model,
        reasoning_effort,
        retries,
        timeout_seconds,
    )
    if report.get("ok") is not True:
        result = {
            "ok": False,
            "status": "reconciliation-failed",
            "identity": identity,
            "revisions": 0,
            "error": report.get("error"),
            "execution_report": report.get("execution_report"),
        }
        _write_json(report_path, result)
        return result
    target_map = {item["source_domain"]: item for item in report["mappings"]}
    writer = ShardedJsonlWriter(shards, "domain-canonical-revisions", int(controls["shard_max_bytes"]))
    revised = 0
    for source, current in sorted(active.items()):
        old_target = slug(current.get("target_domain"))
        final = target_map[old_target]
        new_target = slug(final["target_domain"])
        if new_target == old_target:
            continue
        writer.append({
            "schema_version": DOMAIN_RECONCILIATION_SCHEMA,
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "source_domain": source,
            "target_domain": new_target,
            "supersedes_target": old_target,
            "reason": final["reason"],
            "model": model,
            "reasoning_effort": reasoning_effort,
            "prompt_version": DOMAIN_REVIEW_PROMPT_VERSION,
            "reconciliation_id": identity,
            "packet_path": str(packet_path),
            "raw_output": report.get("raw_output"),
        })
        revised += 1
    result = {
        "ok": True,
        "status": "complete",
        "identity": identity,
        "input_targets": len(targets),
        "output_targets": len({item["target_domain"] for item in report["mappings"]}),
        "revisions": revised,
        "attempts": report.get("attempts"),
        "elapsed_seconds": report.get("elapsed_seconds"),
        "execution_report": report.get("execution_report"),
        "scratch_cleaned": report.get("scratch_cleaned"),
    }
    _write_json(report_path, result)
    return result


def _review_batch(
    repo_root: Path,
    review_root: Path,
    scratch_root: Path,
    batch_id: str,
    source_domains: List[str],
    packet_path: Path,
    model: str,
    reasoning_effort: str,
    retries: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    prompt = f"""
Act as the final read-only KitUniverse domain-taxonomy organizer.
Read {packet_path} and inspect NexusEngine plus NexusEngine-ProtoKits domain paths only when useful. Map every assigned source_domain exactly once to a durable canonical top-level technical ownership domain.
Keep a source domain only when it owns a distinct reusable state/control concern. Collapse spelling variants, plural variants, and modifier aliases into their natural parent: for example combat-control, combat-controls, combat-actions, and combat-resolution normally belong under combat, with the narrower concern retained later as a subdomain. Do not collapse genuinely independent concerns merely because they interact. Do not use game brands, object piles, `mechanics`, `general`, `misc`, `other`, `unknown`, or `unclassified`. Ignore obvious test-fixture domains. Do not invent labels to increase the domain count. Do not edit files.
Return only JSON with every assigned source exactly once:
{{"ok":true,"mappings":[{{"source_domain":"exact-source","target_domain":"canonical-domain","reason":"specific ownership or merge reason"}}],"systemic_errors":[]}}
""".strip()
    last_error = "domain review did not run"
    executions = []
    for attempt in range(retries + 1):
        repair = f"\nPrior output was invalid: {last_error}. Correct the complete mapping envelope." if attempt else ""
        execution = run_codex_lane(
            repo_root=repo_root,
            artifact_root=review_root,
            scratch_root=scratch_root,
            job_id=batch_id,
            prompt=prompt + repair,
            model=model,
            reasoning_effort=reasoning_effort,
            timeout_seconds=timeout_seconds,
            attempt=attempt,
        )
        executions.append(execution)
        if not execution.get("ok") or not execution.get("response"):
            last_error = str(execution.get("error") or "Codex domain review failed")
            continue
        try:
            value = _parse_object(Path(str(execution["response"])).read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            last_error = str(exc)
            continue
        mappings = [item for item in value.get("mappings") or [] if isinstance(item, dict)]
        sources = [slug(item.get("source_domain")) for item in mappings]
        targets_valid = all(is_architectural_domain(item.get("target_domain")) for item in mappings)
        reasoned = all(isinstance(item.get("reason"), str) and item["reason"].strip() for item in mappings)
        complete = sorted(sources) == sorted(source_domains) and len(sources) == len(set(sources))
        if value.get("ok") is not True or not complete or not targets_valid or not reasoned or not isinstance(value.get("systemic_errors"), list):
            last_error = f"invalid-domain-envelope:complete={complete}:targets={targets_valid}:reasoned={reasoned}"
            continue
        normalized = [
            {"source_domain": slug(item["source_domain"]), "target_domain": slug(item["target_domain"]), "reason": item["reason"].strip()}
            for item in mappings
        ]
        return {
            "ok": True,
            "batch_id": batch_id,
            "mappings": normalized,
            "systemic_errors": value.get("systemic_errors") or [],
            "model": model,
            "reasoning_effort": reasoning_effort,
            "attempts": len(executions),
            "elapsed_seconds": round(sum(float(item.get("elapsed_seconds") or 0) for item in executions), 3),
            "packet_path": str(packet_path),
            "raw_output": execution.get("response"),
            "execution_report": execution.get("execution_report"),
            "scratch_cleaned": all(item.get("scratch_cleaned") is True for item in executions),
            "_started_monotonic": min(float(item["_started_monotonic"]) for item in executions),
            "_finished_monotonic": max(float(item["_finished_monotonic"]) for item in executions),
        }
    last = executions[-1] if executions else {}
    return {
        "ok": False,
        "batch_id": batch_id,
        "error": last_error,
        "attempts": len(executions),
        "elapsed_seconds": round(sum(float(item.get("elapsed_seconds") or 0) for item in executions), 3),
        "packet_path": str(packet_path),
        "raw_output": last.get("response"),
        "execution_report": last.get("execution_report"),
        "scratch_cleaned": all(item.get("scratch_cleaned") is True for item in executions),
        "_started_monotonic": min((float(item["_started_monotonic"]) for item in executions), default=0.0),
        "_finished_monotonic": max((float(item["_finished_monotonic"]) for item in executions), default=0.0),
    }


def _review_target_batch(
    repo_root: Path,
    review_root: Path,
    scratch_root: Path,
    batch_id: str,
    target_domains: List[str],
    packet_path: Path,
    model: str,
    reasoning_effort: str,
    retries: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    prompt = f"""
Act as the final read-only KitUniverse cross-batch taxonomy reconciler.
Read {packet_path}. Every assigned_target_domain was proposed independently, so reconcile the complete set globally. Map every assigned target exactly once to a durable canonical top-level technical ownership domain. Collapse singular/plural aliases and roots that merely prepend their context to another existing ownership concern: for example cards to card, world-navigation to navigation, world-progression to progression, and world-resources to resources. Keep genuinely independent broad concerns such as content generation, content distribution, and world generation separate. A top-level root must be broad enough to own multiple independently usable atomic kits; narrower details remain subdomains. Do not invent extra roots, collapse unrelated concerns, use game brands/object piles/catch-alls, or edit files.
Return only JSON with every assigned target exactly once, using source_domain for the exact assigned target:
{{"ok":true,"mappings":[{{"source_domain":"exact-assigned-target","target_domain":"final-canonical-domain","reason":"specific ownership or merge reason"}}],"systemic_errors":[]}}
""".strip()
    last_error = "domain target reconciliation did not run"
    executions = []
    for attempt in range(retries + 1):
        repair = f"\nPrior output was invalid: {last_error}. Correct the complete mapping envelope." if attempt else ""
        execution = run_codex_lane(
            repo_root=repo_root,
            artifact_root=review_root,
            scratch_root=scratch_root,
            job_id=batch_id,
            prompt=prompt + repair,
            model=model,
            reasoning_effort=reasoning_effort,
            timeout_seconds=timeout_seconds,
            attempt=attempt,
        )
        executions.append(execution)
        if not execution.get("ok") or not execution.get("response"):
            last_error = str(execution.get("error") or "Codex domain target reconciliation failed")
            continue
        try:
            value = _parse_object(Path(str(execution["response"])).read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            last_error = str(exc)
            continue
        mappings = [item for item in value.get("mappings") or [] if isinstance(item, dict)]
        sources = [slug(item.get("source_domain")) for item in mappings]
        complete = sorted(sources) == sorted(target_domains) and len(sources) == len(set(sources))
        targets_valid = all(is_architectural_domain(item.get("target_domain")) for item in mappings)
        reasoned = all(isinstance(item.get("reason"), str) and item["reason"].strip() for item in mappings)
        if value.get("ok") is not True or not complete or not targets_valid or not reasoned or not isinstance(value.get("systemic_errors"), list):
            last_error = f"invalid-domain-reconciliation:complete={complete}:targets={targets_valid}:reasoned={reasoned}"
            continue
        return {
            "ok": True,
            "batch_id": batch_id,
            "mappings": [
                {"source_domain": slug(item["source_domain"]), "target_domain": slug(item["target_domain"]), "reason": item["reason"].strip()}
                for item in mappings
            ],
            "attempts": len(executions),
            "elapsed_seconds": round(sum(float(item.get("elapsed_seconds") or 0) for item in executions), 3),
            "packet_path": str(packet_path),
            "raw_output": execution.get("response"),
            "execution_report": execution.get("execution_report"),
            "scratch_cleaned": all(item.get("scratch_cleaned") is True for item in executions),
        }
    last = executions[-1] if executions else {}
    return {
        "ok": False,
        "batch_id": batch_id,
        "error": last_error,
        "attempts": len(executions),
        "elapsed_seconds": round(sum(float(item.get("elapsed_seconds") or 0) for item in executions), 3),
        "packet_path": str(packet_path),
        "raw_output": last.get("response"),
        "execution_report": last.get("execution_report"),
        "scratch_cleaned": all(item.get("scratch_cleaned") is True for item in executions),
    }


def apply_domain_mapping(contract: Dict[str, Any], target_domain: str, reason: str) -> Dict[str, Any]:
    source_domain = slug(contract.get("domain"))
    target = slug(target_domain)
    result = json.loads(json.dumps(contract))
    if source_domain != target:
        modifier = source_domain
        if source_domain.startswith(f"{target}-"):
            modifier = source_domain[len(target) + 1 :]
        elif source_domain.endswith(f"-{target}"):
            modifier = source_domain[: -(len(target) + 1)]
        subdomain = slug(result.get("subdomain"))
        if modifier and modifier != target and modifier not in subdomain.split("-"):
            subdomain = slug(f"{modifier}-{subdomain}")
        result["subdomain"] = subdomain
    result["domain"] = target
    result["domain_organization"] = {
        "source_domain": source_domain,
        "target_domain": target,
        "reason": reason,
        "prompt_version": DOMAIN_REVIEW_PROMPT_VERSION,
    }
    return result


def _parse_object(raw: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else raw[raw.find("{") : raw.rfind("}") + 1]
    value = json.loads(candidate)
    if not isinstance(value, dict):
        raise ValueError("domain-review-output-not-object")
    return value


def _read_jsonl(paths: Iterable[Path]) -> Iterator[Dict[str, Any]]:
    for path in sorted(paths):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def _write_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)
