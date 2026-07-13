from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from workflow_harnesses.guided_kit_builder.codex_cli_review import CODEX_BINARY, CODEX_MODEL
from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash
from workflow_harnesses.rawg_capability_pipeline.inventory import build_capability_inventory, capability_status

from .workflow_rawg_matrix_optimizer import ShardedJsonlWriter


DEFAULT_WORKSPACE = Path("runs/rawg-881k/matrix-production")
DEFAULT_ENGINE_ROOT = Path("/Users/crimsonwheeler/Documents/GitHub/NexusEngine")
DEFAULT_PROTOKITS_ROOT = Path("/Users/crimsonwheeler/Documents/GitHub/NexusEngine-ProtoKits")
CLUSTER_BANNED_TOKENS = {
    "core", "hub", "module", "realm", "scene", "system", "technology", "world", "zone",
}
CLUSTER_METADATA_TOKENS = {
    "controller", "singleplayer", "steam", "support",
}


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    parser.add_argument("--engine-root", type=Path, default=DEFAULT_ENGINE_ROOT)
    parser.add_argument("--protokits-root", type=Path, default=DEFAULT_PROTOKITS_ROOT)
    parser.add_argument("--min-support", type=int, default=2)
    parser.add_argument("--max-candidates", type=int, default=25)
    parser.add_argument("--shard-max-mb", type=int, default=90)
    parser.add_argument("--skip-codex", action="store_true")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-rawg-matrix-cluster",
        description="Incrementally cluster grounded production nodes and ask Codex for kit proposals.",
    )
    configure_parser(parser)
    report = run_clustering(parser.parse_args(argv))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


def run_clustering(args: argparse.Namespace) -> Dict[str, Any]:
    if args.min_support < 1 or args.max_candidates < 1 or args.shard_max_mb > 90:
        raise ValueError("invalid cluster controls")
    workspace = args.workspace.resolve()
    source_shards = workspace / "shards"
    cluster_root = workspace / "clusters"
    cluster_shards = cluster_root / "shards"
    review_root = cluster_root / "reviews"
    cluster_root.mkdir(parents=True, exist_ok=True)
    review_root.mkdir(parents=True, exist_ok=True)
    max_bytes = args.shard_max_mb * 1_000_000
    event_writer = ShardedJsonlWriter(cluster_shards, "cluster-events", max_bytes)
    ledger_writer = ShardedJsonlWriter(cluster_shards, "cluster-source-ledger", max_bytes)
    review_writer = ShardedJsonlWriter(cluster_shards, "codex-review-ledger", max_bytes)
    feedback_writer = ShardedJsonlWriter(cluster_shards, "quality-feedback-events", max_bytes)
    proposal_writer = ShardedJsonlWriter(cluster_shards, "kit-build-requests", max_bytes)
    processed = _load_values(cluster_shards.glob("cluster-source-ledger-*.jsonl"), "identity")
    reviewed = _load_values(cluster_shards.glob("codex-review-ledger-*.jsonl"), "review_identity")
    proposed_ids = _load_values(cluster_shards.glob("kit-build-requests-*.jsonl"), "proposal_id")
    clusters = _read_cluster_events(cluster_shards.glob("cluster-events-*.jsonl"))
    new_results = 0
    new_events = 0
    malformed_source_lines = 0

    for path in sorted(source_shards.glob("production-results-*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    result = json.loads(line)
                except json.JSONDecodeError:
                    malformed_source_lines += 1
                    continue
                identity = stable_hash([result.get("source_hash"), result.get("profile_id")])
                if identity in processed:
                    continue
                node_map = {item["node_id"]: item for item in result.get("accepted_nodes") or []}
                for node in result.get("accepted_nodes") or []:
                    if not node.get("direct_evidence_source_ids"):
                        continue
                    parent = node_map.get(node.get("parent_id")) or {}
                    event = {
                        "schema_version": "rawg.capability-cluster-event.v1",
                        "cluster_key": _canonical_key(node.get("semantic_key") or node.get("label")),
                        "label": node.get("label"),
                        "semantic_key": node.get("semantic_key"),
                        "kind": node.get("kind"),
                        "level": node.get("level"),
                        "parent_label": parent.get("label"),
                        "parent_semantic_key": parent.get("semantic_key"),
                        "source_id": result.get("source_id"),
                        "source_hash": result.get("source_hash"),
                        "source_file": result.get("source_file"),
                        "source_line": result.get("source_line"),
                        "direct_evidence_source_ids": node.get("direct_evidence_source_ids"),
                        "result_identity": identity,
                    }
                    event_writer.append(event)
                    _apply_event(clusters, event)
                    new_events += 1
                ledger_writer.append({
                    "identity": identity,
                    "source_id": result.get("source_id"),
                    "source_hash": result.get("source_hash"),
                    "profile_id": result.get("profile_id"),
                    "cluster_events": sum(1 for node in result.get("accepted_nodes") or [] if node.get("direct_evidence_source_ids")),
                })
                processed.add(identity)
                new_results += 1

    inventory = build_capability_inventory(args.engine_root, args.protokits_root)
    _write_json(cluster_root / "nexus-capability-inventory.json", inventory)
    candidates = []
    filtered_candidates = Counter()
    for cluster in clusters.values():
        cluster = _finalize_cluster(cluster)
        comparison = capability_status(cluster["cluster_key"], inventory)
        cluster["inventory_status"] = comparison["status"]
        cluster["inventory_matches"] = comparison["matches"]
        cluster["support_bucket"] = _support_bucket(cluster["support_count"])
        cluster["review_identity"] = stable_hash([cluster["cluster_key"], cluster["support_bucket"]])
        cluster["score"] = cluster["support_count"] * 4 + cluster["parent_count"] * 2 + cluster["evidence_reference_count"]
        filter_reason = _candidate_filter_reason(cluster)
        cluster["candidate_filter_reason"] = filter_reason
        if (
            cluster["kind"] == "subdomain"
            and cluster["support_count"] >= args.min_support
            and cluster["inventory_status"] == "missing"
            and cluster["review_identity"] not in reviewed
            and not filter_reason
        ):
            candidates.append(cluster)
        elif filter_reason and cluster["kind"] == "subdomain" and cluster["support_count"] >= args.min_support:
            filtered_candidates[filter_reason] += 1
    candidates.sort(key=lambda value: (-value["score"], -value["support_count"], value["cluster_key"]))
    selected = candidates[: args.max_candidates]
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
    review_dir = review_root / run_id
    review_dir.mkdir(parents=True, exist_ok=True)
    _write_json(review_dir / "cluster-candidates.json", selected)
    cluster_snapshot_writer = ShardedJsonlWriter(review_dir / "cluster-snapshot", "clusters", max_bytes)
    for cluster in sorted((_finalize_cluster(value) for value in clusters.values()), key=lambda value: value["cluster_key"]):
        cluster_snapshot_writer.append(cluster)

    codex = {"ok": True, "skipped": True, "reason": "skip-codex", "decisions": [], "kits": []}
    if selected and not args.skip_codex:
        codex = _run_codex_review(review_dir, selected, args.engine_root, args.protokits_root)
    _write_json(review_dir / "codex-cluster-review.json", codex)
    cluster_map = {cluster["cluster_id"]: cluster for cluster in selected}
    repair_targets = [
        {
            "cluster": cluster_map[item["cluster_id"]],
            "repair_instruction": item.get("repair_instruction") or "Rewrite as one atomic evidence-backed domain capability.",
        }
        for item in codex.get("decisions") or []
        if not item.get("accepted") and item.get("repairable") and item.get("cluster_id") in cluster_map
    ]
    repair = {"ok": True, "skipped": True, "reason": "no-repair-targets", "decisions": [], "kits": []}
    if repair_targets and not args.skip_codex:
        repair = _run_codex_repair(review_dir, repair_targets, args.engine_root, args.protokits_root)
    _write_json(review_dir / "codex-cluster-repair.json", repair)
    new_proposals = 0
    if codex.get("ok") and not codex.get("skipped"):
        decision_map = {item["cluster_id"]: item for item in codex.get("decisions") or []}
        repair_decision_map = {item["cluster_id"]: item for item in repair.get("decisions") or []}
        for cluster in selected:
            decision = decision_map.get(cluster["cluster_id"], {"accepted": False, "reasons": ["missing-decision"]})
            repair_decision = repair_decision_map.get(cluster["cluster_id"])
            review_writer.append({
                "review_identity": cluster["review_identity"],
                "cluster_id": cluster["cluster_id"],
                "cluster_key": cluster["cluster_key"],
                "support_bucket": cluster["support_bucket"],
                "support_count": cluster["support_count"],
                "accepted": bool(decision.get("accepted")),
                "reasons": decision.get("reasons") or [],
                "repairable": bool(decision.get("repairable")),
                "repair_instruction": decision.get("repair_instruction"),
                "repair_accepted": bool(repair_decision and repair_decision.get("accepted")),
                "repair_reasons": (repair_decision or {}).get("reasons") or [],
                "review_run_id": run_id,
            })
            accepted = bool(decision.get("accepted"))
            repaired = bool(repair_decision and repair_decision.get("accepted"))
            feedback_writer.append({
                "schema_version": "rawg.quality-feedback-event.v1",
                "feedback_id": stable_hash([cluster["review_identity"], run_id, "quality-feedback"]),
                "cluster_id": cluster["cluster_id"],
                "cluster_key": cluster["cluster_key"],
                "review_identity": cluster["review_identity"],
                "support_count": cluster["support_count"],
                "support_bucket": cluster["support_bucket"],
                "outcome": "accepted" if accepted else ("repaired" if repaired else "deferred"),
                "utility_lanes": [
                    "source-evidence",
                    "semantic-alias",
                    *( ["kit-proposal"] if accepted or repaired else ["negative-filter-signal", "future-support-review"] ),
                ],
                "review_reasons": decision.get("reasons") or [],
                "repair_instruction": decision.get("repair_instruction"),
                "repair_reasons": (repair_decision or {}).get("reasons") or [],
                "systemic_errors": codex.get("systemic_errors") or [],
                "next_support_bucket": cluster["support_bucket"] * 2 if not accepted and not repaired else None,
                "review_run_id": run_id,
            })
        for kit in [*(codex.get("kits") or []), *(repair.get("kits") or [])]:
            cluster = cluster_map.get(kit.get("cluster_id"))
            kit_id = slug(kit.get("kit_id") or kit.get("name"))
            proposal_id = stable_hash([kit_id, cluster.get("review_identity") if cluster else None])
            if not cluster or not kit_id or proposal_id in proposed_ids:
                continue
            proposal_writer.append({
                "schema_version": "kit.build-request.v1",
                "proposal_id": proposal_id,
                "source_id": f"rawg-cluster-{cluster['cluster_key']}-{kit_id}",
                "name": str(kit.get("name") or kit_id.replace("-", " ").title()),
                "description": str(kit.get("owned_behavior") or ""),
                "constraints": ["render-agnostic", "idempotent", "snapshot-and-reset", "codex-cluster-reviewed"],
                "seed_domains": [slug(kit.get("domain")) or cluster["cluster_key"]],
                "source_context": {
                    "capability_cluster_id": cluster["cluster_id"],
                    "support_count": cluster["support_count"],
                    "rawg_source_ids": cluster["source_ids"][:50],
                    "evidence_hash": cluster["evidence_hash"],
                    "review_identity": cluster["review_identity"],
                    "promotion_level": "proposal-only",
                },
            })
            proposed_ids.add(proposal_id)
            new_proposals += 1

    shard_paths = list(cluster_shards.glob("*.jsonl")) + list((review_dir / "cluster-snapshot").glob("*.jsonl"))
    report = {
        "ok": malformed_source_lines == 0 and (codex.get("ok", False) or not selected) and repair.get("ok", False),
        "status": "complete",
        "run_id": run_id,
        "workspace": str(workspace),
        "source_results_seen": len(processed),
        "new_source_results": new_results,
        "new_cluster_events": new_events,
        "cluster_count": len(clusters),
        "eligible_candidates": len(candidates),
        "filtered_candidates": dict(sorted(filtered_candidates.items())),
        "selected_candidates": len(selected),
        "new_kit_proposals": new_proposals,
        "malformed_source_lines": malformed_source_lines,
        "malformed_output_lines": _count_malformed(shard_paths),
        "max_observed_shard_bytes": max((path.stat().st_size for path in shard_paths), default=0),
        "shard_max_bytes": max_bytes,
        "review_dir": str(review_dir),
        "codex": codex,
        "codex_repair": repair,
    }
    _write_json(review_dir / "report.json", report)
    _write_json(cluster_root / "latest-report.json", report)
    return report


def _canonical_key(value: Any) -> str:
    output = []
    for token in slug(value).split("-"):
        if token.endswith("ies") and len(token) > 4:
            token = token[:-3] + "y"
        elif token.endswith("s") and not token.endswith("ss") and len(token) > 4:
            token = token[:-1]
        output.append(token)
    return "-".join(output)


def _apply_event(clusters: Dict[str, Dict[str, Any]], event: Dict[str, Any]) -> None:
    key = event["cluster_key"]
    value = clusters.setdefault(key, {
        "schema_version": "rawg.capability-cluster.v1",
        "cluster_id": f"capability:{key}",
        "cluster_key": key,
        "kind": event.get("kind"),
        "aliases": set(),
        "source_ids": set(),
        "parent_labels": set(),
        "parent_semantic_keys": set(),
        "evidence_references": [],
    })
    value["aliases"].add(str(event.get("label") or ""))
    value["source_ids"].add(str(event.get("source_id") or ""))
    if event.get("parent_label"):
        value["parent_labels"].add(str(event["parent_label"]))
    if event.get("parent_semantic_key"):
        value["parent_semantic_keys"].add(str(event["parent_semantic_key"]))
    if len(value["evidence_references"]) < 100:
        value["evidence_references"].append({
            "source_id": event.get("source_id"),
            "source_hash": event.get("source_hash"),
            "source_file": event.get("source_file"),
            "source_line": event.get("source_line"),
            "evidence_source_ids": event.get("direct_evidence_source_ids") or [],
        })


def _read_cluster_events(paths: Iterable[Path]) -> Dict[str, Dict[str, Any]]:
    output: Dict[str, Dict[str, Any]] = {}
    for path in sorted(paths):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    _apply_event(output, json.loads(line))
    return output


def _finalize_cluster(value: Dict[str, Any]) -> Dict[str, Any]:
    aliases = sorted(item for item in value["aliases"] if item)
    source_ids = sorted(item for item in value["source_ids"] if item)
    parents = sorted(item for item in value["parent_labels"] if item)
    parent_keys = sorted(item for item in value["parent_semantic_keys"] if item)
    references = value["evidence_references"]
    return {
        "schema_version": value["schema_version"],
        "cluster_id": value["cluster_id"],
        "cluster_key": value["cluster_key"],
        "label": aliases[0] if aliases else value["cluster_key"].replace("-", " ").title(),
        "kind": value["kind"],
        "aliases": aliases,
        "source_ids": source_ids,
        "support_count": len(source_ids),
        "parent_labels": parents,
        "parent_semantic_keys": parent_keys,
        "parent_count": len(parent_keys),
        "evidence_references": references,
        "evidence_reference_count": len(references),
        "evidence_hash": stable_hash([value["cluster_key"], source_ids, references]),
    }


def _support_bucket(support: int) -> int:
    return 1 if support <= 1 else 2 ** int(math.floor(math.log2(support)))


def _candidate_filter_reason(cluster: Dict[str, Any]) -> Optional[str]:
    tokens = set(slug(cluster.get("cluster_key")).split("-"))
    if tokens & CLUSTER_BANNED_TOKENS:
        return "synthetic-container-token"
    if tokens & CLUSTER_METADATA_TOKENS:
        return "platform-or-mode-metadata"
    if len(tokens) < 2:
        return "non-atomic-single-token"
    return None


def _run_codex_review(review_dir: Path, clusters: List[Dict[str, Any]], engine_root: Path, protokits_root: Path) -> Dict[str, Any]:
    output = review_dir / "codex-cluster-review.raw.txt"
    ids = [value["cluster_id"] for value in clusters]
    prompt = f"""
Act as the KitUniverse cluster-to-kit architect.

READ:
- {review_dir / 'cluster-candidates.json'}
- use targeted `rg` checks in {engine_root} and {protokits_root}
- goal.md and memory.md

Decide every cluster exactly once. Accept only a mechanically coherent, atomic, reusable capability with adequate source evidence that is not already implemented or merely a genre, story, presentation detail, branded phrase, or renamed duplicate. When the evidence supports a useful mechanic but the generated label or abstraction is poor, mark it repairable and give one precise rewrite instruction. Evidence-insufficient clusters are not repairable. Produce zero to six proposal-only kits from accepted clusters. Do not edit files or promote anything.

Each kit needs cluster_id, kit_id, name, domain, owned_behavior, inputs, outputs, novelty_reason.
Return only:
{{"ok":true,"decisions":[{{"cluster_id":"exact-id","accepted":true,"reasons":[],"repairable":false,"repair_instruction":null}}],"kits":[{{"cluster_id":"exact-id","kit_id":"kebab-id","name":"Name","domain":"domain","owned_behavior":"one behavior","inputs":[],"outputs":[],"novelty_reason":"why"}}],"systemic_errors":[]}}

Decide these exact IDs once each: {json.dumps(ids)}
""".strip()
    command = [
        str(CODEX_BINARY), "exec", "--ephemeral", "--color", "never", "-C", str(Path.cwd()),
        "-s", "read-only", "-m", CODEX_MODEL, "-c", 'model_reasoning_effort="medium"', "-o", str(output), prompt,
    ]
    started = time.monotonic()
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=900, check=False)
        parsed = _parse_object(output.read_text(encoding="utf-8")) if result.returncode == 0 and output.exists() else {}
    except (OSError, subprocess.TimeoutExpired, ValueError, json.JSONDecodeError) as error:
        return {"ok": False, "error": str(error), "elapsed_seconds": round(time.monotonic() - started, 3)}
    decisions = [item for item in parsed.get("decisions") or [] if isinstance(item, dict)]
    decided = [item.get("cluster_id") for item in decisions]
    complete = sorted(decided) == sorted(ids) and len(decided) == len(set(decided))
    typed = all(isinstance(item.get("accepted"), bool) and isinstance(item.get("repairable"), bool) for item in decisions)
    return {
        **parsed,
        "ok": bool(parsed.get("ok")) and complete and typed,
        "complete": complete,
        "typed": typed,
        "model": CODEX_MODEL,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "returncode": result.returncode,
        "stderr_tail": result.stderr[-2000:],
        "raw_output": str(output),
    }


def _run_codex_repair(
    review_dir: Path, targets: List[Dict[str, Any]], engine_root: Path, protokits_root: Path,
) -> Dict[str, Any]:
    repair_input = review_dir / "cluster-repair-input.json"
    _write_json(repair_input, targets)
    output = review_dir / "codex-cluster-repair.raw.txt"
    ids = [value["cluster"]["cluster_id"] for value in targets]
    prompt = f"""
Act as the KitUniverse evidence-preserving cluster repairer.

READ:
- {repair_input}
- use targeted `rg` checks in {engine_root} and {protokits_root}
- goal.md and memory.md

For every listed cluster, attempt to convert the noisy label into exactly one atomic domain-like capability. Preserve the cited RAWG evidence and intended mechanic, but remove platform metadata, genres, branded words, and synthetic container nouns. Do not add any behavior not directly supported by the evidence references. Reject the repair when the evidence cannot define inputs, an owned state/rule transition, and outputs. Recheck novelty against Nexus Engine and ProtoKits. Do not edit files or promote anything.

Return only:
{{"ok":true,"decisions":[{{"cluster_id":"exact-id","accepted":true,"reasons":[]}}],"kits":[{{"cluster_id":"exact-id","kit_id":"kebab-id","name":"Name","domain":"domain","owned_behavior":"one explicit behavior","inputs":[],"outputs":[],"novelty_reason":"why the repaired capability is evidenced and missing"}}]}}

Decide these exact IDs once each: {json.dumps(ids)}
""".strip()
    command = [
        str(CODEX_BINARY), "exec", "--ephemeral", "--color", "never", "-C", str(Path.cwd()),
        "-s", "read-only", "-m", CODEX_MODEL, "-c", 'model_reasoning_effort="medium"', "-o", str(output), prompt,
    ]
    started = time.monotonic()
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=900, check=False)
        parsed = _parse_object(output.read_text(encoding="utf-8")) if result.returncode == 0 and output.exists() else {}
    except (OSError, subprocess.TimeoutExpired, ValueError, json.JSONDecodeError) as error:
        return {"ok": False, "error": str(error), "elapsed_seconds": round(time.monotonic() - started, 3)}
    decisions = [item for item in parsed.get("decisions") or [] if isinstance(item, dict)]
    decided = [item.get("cluster_id") for item in decisions]
    complete = sorted(decided) == sorted(ids) and len(decided) == len(set(decided))
    typed = all(isinstance(item.get("accepted"), bool) for item in decisions)
    return {
        **parsed,
        "ok": bool(parsed.get("ok")) and complete and typed,
        "complete": complete,
        "typed": typed,
        "model": CODEX_MODEL,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "returncode": result.returncode,
        "stderr_tail": result.stderr[-2000:],
        "raw_output": str(output),
    }


def _load_values(paths: Iterable[Path], key: str) -> set[str]:
    output = set()
    for path in sorted(paths):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    value = json.loads(line)
                    if value.get(key):
                        output.add(value[key])
    return output


def _parse_object(content: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", content, re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else content[content.find("{") : content.rfind("}") + 1]
    value = json.loads(candidate)
    if not isinstance(value, dict):
        raise ValueError("response is not an object")
    return value


def _count_malformed(paths: Iterable[Path]) -> int:
    bad = 0
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    json.loads(line)
                except json.JSONDecodeError:
                    bad += 1
    return bad


def _write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
