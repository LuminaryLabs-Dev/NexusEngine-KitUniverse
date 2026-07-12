from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from kituniverse_harness.providers import LMStudioProvider
from kituniverse_harness.smart_router import SmartRoutingService

from .contracts import (
    CAPABILITY_CLUSTER_SCHEMA,
    ENGINE_GAP_SCHEMA,
    KIT_BUILD_REQUEST_SCHEMA,
    source_identity,
    stable_hash,
)
from .cluster_review import run_cluster_review
from .extractor import extract_mechanics
from .inventory import build_capability_inventory, capability_status
from .source_adapter import discover_chunks, stream_rawg_records
from .workspace import Workspace, append_jsonl, code_epoch, now, read_json, write_json_atomic


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_ROOT = Path("/Users/crimsonwheeler/Documents/GitHub/NexusRealtime-Ideas/games/rawg/chunks")
DEFAULT_ENGINE_ROOT = Path("/Users/crimsonwheeler/Documents/GitHub/NexusEngine")
DEFAULT_PROTOKITS_ROOT = Path("/Users/crimsonwheeler/Documents/GitHub/NexusEngine-ProtoKits")
DEFAULT_SIMULATOR_CLI = Path("/Users/crimsonwheeler/Documents/GitHub/NexusSimulator/NexusSimulator-V1/src/cli.js")


def default_config() -> Dict[str, Any]:
    return {
        "schema_version": "rawg.pipeline-config.v1",
        "source_root": str(DEFAULT_SOURCE_ROOT),
        "engine_root": str(DEFAULT_ENGINE_ROOT),
        "protokits_root": str(DEFAULT_PROTOKITS_ROOT),
        "simulator_cli": str(DEFAULT_SIMULATOR_CLI),
        "base_url": "http://10.0.0.137:1234/v1",
        "model": "lfm2.5-350m-heretic-high-reasoning-i1",
        "context_length": 2048,
        "max_context_tokens": 768,
        "max_tokens": 240,
        "parallel": 4,
        "batch_size": 100,
        "max_records": 0,
        "min_cluster_support": 2,
        "min_free_gib": 10.0,
        "use_model": True,
        "auto_promote": False,
        "review_clusters": True,
        "max_promotions": 50,
        "universe_root": "runs/kit-universe-1000",
    }


def merge_config(value: Dict[str, Any]) -> Dict[str, Any]:
    config = {**default_config(), **(value or {})}
    for key in ["context_length", "max_context_tokens", "max_tokens", "parallel", "batch_size", "max_records", "min_cluster_support", "max_promotions"]:
        config[key] = int(config[key])
    config["min_free_gib"] = float(config["min_free_gib"])
    if config["parallel"] < 1 or config["batch_size"] < 1:
        raise ValueError("parallel and batch_size must be positive")
    if config["max_context_tokens"] > config["context_length"]:
        raise ValueError("max_context_tokens cannot exceed context_length")
    return config


def _existing_sets(
    workspace: Workspace,
) -> tuple[set[str], Dict[str, Dict[str, Any]], Counter[str]]:
    processed = set()
    statuses: Counter[str] = Counter()
    for item in workspace.read_sharded("source"):
        if item.get("source_identity"):
            processed.add(str(item["source_identity"]))
        statuses[str(item.get("status") or "unknown")] += 1
    evidence = {}
    for item in workspace.read_sharded("evidence"):
        fingerprint = item.get("evidence_fingerprint")
        if fingerprint:
            evidence[str(fingerprint)] = item
    return processed, evidence, statuses


def _cluster_mechanics(
    clusters: Dict[str, Dict[str, Any]], source: Dict[str, Any], evidence: Dict[str, Any]
) -> None:
    for mechanic in evidence.get("mechanics") or []:
        capability_id = str(mechanic["capability_id"])
        cluster_id = f"capability:{capability_id}"
        cluster = clusters.setdefault(
            cluster_id,
            {
                "schema_version": CAPABILITY_CLUSTER_SCHEMA,
                "cluster_id": cluster_id,
                "capability_id": capability_id,
                "label": mechanic.get("label") or capability_id.replace("-", " ").title(),
                "support_count": 0,
                "rawg_source_ids": [],
                "genre_counts": {},
                "evidence_fields": [],
                "extractors": [],
                "pipeline_epochs": [],
            },
        )
        cluster["support_count"] += 1
        if source.get("source_id") not in cluster["rawg_source_ids"] and len(cluster["rawg_source_ids"]) < 50:
            cluster["rawg_source_ids"].append(source.get("source_id"))
        genre_counts = Counter(cluster.get("genre_counts") or {})
        genre_counts.update(source.get("genres") or [])
        cluster["genre_counts"] = dict(genre_counts.most_common(40))
        cluster["evidence_fields"] = sorted(set(cluster.get("evidence_fields") or []) | set(mechanic.get("evidence_fields") or []))
        cluster["extractors"] = sorted(set(cluster.get("extractors") or []) | {mechanic.get("extractor", "unknown")})
        cluster["pipeline_epochs"] = sorted(set(cluster.get("pipeline_epochs") or []) | {source.get("pipeline_epoch")})
        cluster["evidence_hash"] = stable_hash(
            {
                "capability_id": capability_id,
                "support_count": cluster["support_count"],
                "source_ids": cluster["rawg_source_ids"],
                "genres": cluster["genre_counts"],
            }
        )


def _rank_gaps(clusters: Dict[str, Dict[str, Any]], inventory: Dict[str, Any]) -> List[Dict[str, Any]]:
    gaps = []
    for cluster in clusters.values():
        comparison = capability_status(cluster["capability_id"], inventory)
        genre_breadth = len(cluster.get("genre_counts") or {})
        score = cluster.get("support_count", 0) + genre_breadth * 2 + len(cluster.get("evidence_fields") or [])
        gaps.append(
            {
                "schema_version": ENGINE_GAP_SCHEMA,
                "gap_id": f"gap:{cluster['capability_id']}",
                "capability_cluster_id": cluster["cluster_id"],
                "capability_id": cluster["capability_id"],
                "status": comparison["status"],
                "inventory_matches": comparison["matches"],
                "support_count": cluster.get("support_count", 0),
                "genre_breadth": genre_breadth,
                "score": score,
                "evidence_hash": cluster.get("evidence_hash"),
            }
        )
    return sorted(gaps, key=lambda item: (-item["score"], item["capability_id"]))


def _build_requests(
    gaps: List[Dict[str, Any]], clusters: Dict[str, Dict[str, Any]], min_support: int
) -> List[Dict[str, Any]]:
    requests = []
    for gap in gaps:
        if gap["status"] != "missing" or gap["support_count"] < min_support:
            continue
        cluster = clusters[gap["capability_cluster_id"]]
        capability = cluster["capability_id"]
        genres = list((cluster.get("genre_counts") or {}).keys())[:8]
        source_ids = cluster.get("rawg_source_ids") or []
        context = {
            "schema_version": KIT_BUILD_REQUEST_SCHEMA,
            "capability_cluster_id": cluster["cluster_id"],
            "engine_gap_id": gap["gap_id"],
            "rawg_source_ids": source_ids,
            "evidence_hash": cluster["evidence_hash"],
            "support_count": cluster["support_count"],
            "genre_breadth": gap["genre_breadth"],
            "promotion_level": "proof-only",
        }
        requests.append(
            {
                "schema_version": KIT_BUILD_REQUEST_SCHEMA,
                "source_id": f"rawg-{capability}",
                "name": f"RAWG {cluster['label']}",
                "description": (
                    f"Build one reusable {cluster['label']} domain service kit supported by "
                    f"{cluster['support_count']} RAWG game records across {', '.join(genres) or 'multiple genres'}. "
                    "Own only the capability state and lifecycle; keep rendering and authored game content outside the kit."
                ),
                "constraints": ["render-agnostic", "idempotent", "snapshot-and-reset", "source-evidence-required"],
                "seed_domains": [capability],
                "source_context": context,
            }
        )
    return requests


async def _wait_for_control(workspace: Workspace) -> str:
    while True:
        action = str(workspace.control().get("action") or "run")
        if action != "pause":
            return action
        workspace.update_state(status="paused")
        await asyncio.sleep(0.5)


def _promote_requests(workspace: Workspace, config: Dict[str, Any], requests: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not config.get("auto_promote") or not requests:
        return {"ok": True, "skipped": True, "reason": "auto-promotion-disabled-or-empty"}
    count = min(int(config.get("max_promotions") or 50), len(requests))
    command = [
        sys.executable,
        "-m",
        "kituniverse_harness.cli",
        "batch",
        "--source-file",
        str(workspace.promotion_requests_path),
        "--count",
        str(count),
        "--base-url",
        str(config["base_url"]),
        "--model",
        str(config["model"]),
        "--context-length",
        str(config["context_length"]),
        "--max-context-tokens",
        str(config["max_context_tokens"]),
        "--max-predictions",
        str(config["parallel"]),
        "--simulator-cli",
        str(config["simulator_cli"]),
        "--universe-root",
        str(config["universe_root"]),
    ]
    result = subprocess.run(command, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    try:
        summary = json.loads(result.stdout)
    except json.JSONDecodeError:
        summary = {}
    report = {
        "ok": result.returncode == 0,
        "command": command,
        "returncode": result.returncode,
        "stdout_tail": result.stdout[-5000:],
        "stderr_tail": result.stderr[-5000:],
        "summary": summary,
    }
    write_json_atomic(workspace.root / "promotion-report.json", report)
    return report


async def run_pipeline(workspace_root: Path, config_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    workspace = Workspace(workspace_root)
    config = merge_config({**workspace.config(), **(config_override or {})})
    write_json_atomic(workspace.config_path, config)
    source_root = Path(config["source_root"]).expanduser().resolve()
    if not discover_chunks(source_root):
        raise FileNotFoundError(f"No rawg-*.jsonl chunks found in {source_root}")
    epoch = code_epoch(REPO_ROOT)
    epoch_id = epoch["code_hash"][:16]
    previous_state = workspace.state()
    prior_epochs = list(previous_state.get("prior_epochs") or [])
    if previous_state.get("pipeline_epoch") != epoch_id:
        previous_epoch = previous_state.get("pipeline_epoch")
        if previous_epoch and previous_epoch not in prior_epochs:
            prior_epochs.append(previous_epoch)
        append_jsonl(workspace.epochs_path, [{**epoch, "pipeline_epoch": epoch_id, "started_at": now()}])
        workspace.event("pipeline-epoch-started", {"pipeline_epoch": epoch_id, **epoch})
    workspace.set_control("run") if not workspace.control_path.exists() else None
    workspace.update_state(status="starting", pipeline_epoch=epoch_id, prior_epochs=prior_epochs, config=config)
    processed, evidence_by_fingerprint, ledger_statuses = _existing_sets(workspace)
    clusters = read_json(workspace.clusters_path, {}) or {}
    inventory = build_capability_inventory(Path(config["engine_root"]), Path(config["protokits_root"]))
    write_json_atomic(workspace.inventory_path, inventory)

    router: Optional[SmartRoutingService] = None
    if config.get("use_model"):
        provider = LMStudioProvider(config["base_url"], config["model"], timeout_seconds=90)
        try:
            load = provider.ensure_loaded(context_length=config["context_length"], parallel=config["parallel"])
            health = provider.health()
        except Exception as error:  # noqa: BLE001 - provider failures become resumable holds.
            workspace.event("provider-preflight-failed", {"error": str(error)})
            workspace.update_state(status="hold", hold_reason="provider-preflight-failed", provider_error=str(error))
            return workspace.state()
        workspace.event("provider-preflight", {"load": load, "health": health})
        if not load.get("ok") or not health.get("ok"):
            workspace.update_state(status="hold", hold_reason="provider-preflight-failed")
            return workspace.state()
        if load.get("status") == "already-loaded" and not load.get("config_matches", True):
            workspace.update_state(status="hold", hold_reason="provider-load-config-mismatch", provider_load=load)
            return workspace.state()
        router = SmartRoutingService(
            config["base_url"], config["model"], timeout_seconds=90,
            max_predictions=config["parallel"], max_context_tokens=config["max_context_tokens"],
        )

    state = workspace.state()
    start_file = state.get("current_file")
    start_line = int(state.get("current_line") or 0)
    processed_count = int(state.get("processed_sources") or len(processed))
    # Reconcile counters from the append-only ledger on every start. This keeps
    # dashboard totals truthful after a crash or a status-accounting code fix.
    failed_count = ledger_statuses["failed"]
    insufficient_count = ledger_statuses["insufficient-evidence"]
    evidence_count = int(state.get("evidence_records") or len(evidence_by_fingerprint))
    starting_processed_count = processed_count
    started = time.time()
    batch: List[tuple[Dict[str, Any], str, int]] = []

    async def flush(items: List[tuple[Dict[str, Any], str, int]]) -> bool:
        nonlocal processed_count, failed_count, insufficient_count, evidence_count, clusters
        if not items:
            return True
        action = await _wait_for_control(workspace)
        if action in {"drain", "stop"}:
            workspace.update_state(status="drained" if action == "drain" else "stopped")
            return False
        if workspace.free_gib() < config["min_free_gib"]:
            workspace.update_state(status="hold", hold_reason="low-disk-space", free_gib=workspace.free_gib())
            return False
        novel: Dict[str, Dict[str, Any]] = {}
        prepared = []
        for source, file_name, line_number in items:
            identity = source_identity(source)
            if identity in processed:
                continue
            if source.get("error") or not source.get("has_mechanic_evidence"):
                final_status = "failed" if source.get("error") else "insufficient-evidence"
                ledger = {
                    **source,
                    "source_identity": identity,
                    "status": final_status,
                    "status_history": ["normalized", final_status],
                    "capability_ids": [],
                }
                workspace.append_sharded("source", [ledger], processed_count)
                processed.add(identity)
                processed_count += 1
                if source.get("error"):
                    failed_count += 1
                else:
                    insufficient_count += 1
                continue
            fingerprint = source["evidence_fingerprint"]
            is_novel = fingerprint not in evidence_by_fingerprint and fingerprint not in novel
            if is_novel:
                novel[fingerprint] = source
            prepared.append((source, identity, file_name, line_number, is_novel))
        tasks = {fingerprint: asyncio.create_task(extract_mechanics(source, router, config["max_tokens"])) for fingerprint, source in novel.items()}
        for fingerprint, task in tasks.items():
            evidence = await task
            evidence_by_fingerprint[fingerprint] = evidence
            workspace.append_sharded("evidence", [evidence], evidence_count)
            evidence_count += 1
        for source, identity, _, _, is_novel in prepared:
            evidence = evidence_by_fingerprint[source["evidence_fingerprint"]]
            if evidence.get("source_id") != source.get("source_id"):
                evidence = {
                    **evidence,
                    "source_id": source.get("source_id"),
                    "source_hash": source.get("source_hash"),
                    "source_file": source.get("source_file"),
                    "source_line": source.get("source_line"),
                    "source_url": source.get("source_url"),
                    "pipeline_epoch": source.get("pipeline_epoch"),
                }
                evidence["evidence_hash"] = stable_hash(evidence)
            _cluster_mechanics(clusters, source, evidence)
            capability_ids = [item["capability_id"] for item in evidence.get("mechanics") or []]
            if capability_ids:
                final_status = "clustered" if is_novel else "grouped"
                status_history = ["normalized", "extracted", final_status] if is_novel else ["normalized", "grouped", "clustered"]
            else:
                final_status = "insufficient-evidence"
                status_history = ["normalized", "extracted", final_status]
            ledger = {
                **source,
                "source_identity": identity,
                "status": final_status,
                "status_history": status_history,
                "capability_ids": capability_ids,
                "evidence_hash": evidence.get("evidence_hash"),
            }
            workspace.append_sharded("source", [ledger], processed_count)
            processed.add(identity)
            processed_count += 1
            if not capability_ids:
                insufficient_count += 1
        last_source, last_file, last_line = items[-1]
        gaps = _rank_gaps(clusters, inventory)
        requests = _build_requests(gaps, clusters, config["min_cluster_support"])
        write_json_atomic(workspace.clusters_path, clusters)
        write_json_atomic(workspace.gaps_path, gaps)
        write_json_atomic(workspace.requests_path, requests)
        elapsed = max(0.001, time.time() - started)
        throughput = (processed_count - starting_processed_count) / elapsed
        remaining = max(0, 881069 - processed_count)
        workspace.update_state(
            status="running",
            current_file=last_file,
            current_line=last_line,
            processed_sources=processed_count,
            failed_sources=failed_count,
            insufficient_sources=insufficient_count,
            evidence_records=evidence_count,
            evidence_clusters=len(clusters),
            engine_gaps=sum(item["status"] == "missing" for item in gaps),
            candidate_kits=len(requests),
            throughput_per_second=round(throughput, 3),
            estimated_seconds_remaining=round(remaining / throughput, 1) if throughput else None,
            free_gib=round(workspace.free_gib(), 2),
            router=router.stats() if router else {"mode": "deterministic-only"},
        )
        workspace.event("source-batch-committed", {"processed": processed_count, "file": last_file, "line": last_line, "candidates": len(requests)})
        return True

    try:
        limit = int(config.get("max_records") or 0)
        run_processed = 0
        for item in stream_rawg_records(source_root, epoch_id, start_file, start_line):
            batch.append(item)
            if len(batch) >= config["batch_size"] or (limit and run_processed + len(batch) >= limit):
                if not await flush(batch):
                    batch = []
                    break
                run_processed += len(batch)
                batch = []
                if limit and run_processed >= limit:
                    break
        if batch:
            await flush(batch)
        state = workspace.state()
        if state.get("status") not in {"hold", "paused", "drained", "stopped"}:
            gaps = read_json(workspace.gaps_path, []) or []
            requests = read_json(workspace.requests_path, []) or []
            promotion_requests = requests
            cluster_review = {"ok": True, "skipped": True, "reason": "auto-promotion-disabled"}
            if config.get("auto_promote") and config.get("review_clusters") and requests:
                candidates = [
                    {
                        "cluster_id": item["source_context"]["capability_cluster_id"],
                        "name": item["name"],
                        "description": item["description"],
                        "source_context": item["source_context"],
                    }
                    for item in requests
                ]
                candidates_path = workspace.root / "cluster-review-candidates.json"
                write_json_atomic(candidates_path, candidates)
                cluster_review = run_cluster_review(
                    REPO_ROOT,
                    workspace.root,
                    candidates_path,
                    workspace.inventory_path,
                    [item["cluster_id"] for item in candidates],
                )
                write_json_atomic(workspace.root / "cluster-review.json", cluster_review)
                if not cluster_review.get("ok"):
                    workspace.update_state(status="hold", hold_reason="codex-cluster-review-failed", cluster_review=cluster_review)
                    return workspace.state()
                accepted = {item["cluster_id"] for item in cluster_review.get("decisions", []) if item.get("accepted") is True}
                promotion_requests = [item for item in requests if item["source_context"]["capability_cluster_id"] in accepted]
            write_json_atomic(workspace.promotion_requests_path, promotion_requests)
            promotion = _promote_requests(workspace, config, promotion_requests)
            workspace.update_state(
                status="complete" if not limit or run_processed < limit else "limit-complete",
                promotion=promotion,
                promoted_kits=int((promotion.get("summary") or {}).get("promoted") or 0),
                cluster_review=cluster_review,
                completed_at=now(),
            )
            workspace.event("pipeline-complete", {"processed": workspace.state().get("processed_sources"), "promotion": promotion})
    finally:
        if router:
            router.shutdown()
    return workspace.state()
