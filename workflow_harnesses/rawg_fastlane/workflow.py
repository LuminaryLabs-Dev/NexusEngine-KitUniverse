from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
import math
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from kituniverse_harness.smart_router import SmartRoutingService
from kituniverse_harness.providers import LMStudioProvider
from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash
from workflow_harnesses.rawg_capability_pipeline.source_adapter import stream_rawg_records
from workflow_harnesses.rawg_capability_pipeline.inventory import build_capability_inventory, capability_status
from workflow_harnesses.rawg_matrix_optimizer.workflow_rawg_matrix_optimizer import (
    DEFAULT_BASE_URL,
    MODEL_12B,
    MODEL_350M,
    MATRIX_ACTION_MARKERS,
    PROFILES,
    ShardedJsonlWriter,
    _filter_nodes,
    _evidence_anchor_terms,
    _matrix_evidence_units,
    _parse_output,
    _run_game,
    _seed_prompt,
    _seed_text,
    _stem_token,
    _walk_prompt,
)

from .ast import load_ast, ordered_nodes
from .codex_loop import run_bounded_review, run_kit_proposal_review
from .fingerprint import OBJECT_TERMS, fingerprint_record
from .grouping import GroupAccumulator
from .normalize import normalize_for_fastlane


TOTAL_RAWG_RECORDS = 881069
SWARM_STRONG_ACTIONS = MATRIX_ACTION_MARKERS - {"move", "play", "use", "win"}
ACTION_OBJECT_COMPATIBILITY = {
    "attack": {"combat", "damage", "enemy", "health", "weapon"},
    "battle": {"combat", "enemy", "party", "weapon"},
    "block": {"attack", "damage", "enemy", "weapon"},
    "build": {"base", "card", "deck", "economy", "item", "relationship", "resource", "weapon"},
    "buy": {"card", "currency", "item", "resource", "weapon"},
    "cast": {"ability", "damage", "enemy", "skill"},
    "choose": {"card", "choice", "dialogue", "ending", "item", "relationship", "weapon"},
    "climb": {"level", "path", "stamina", "surface", "world"},
    "collect": {"card", "currency", "item", "loot", "resource", "score", "skill", "weapon"},
    "craft": {"card", "deck", "item", "resource", "weapon"},
    "customize": {"ability", "character", "gear", "item", "skill", "weapon"},
    "dash": {"enemy", "level", "path", "stamina"},
    "destroy": {"base", "enemy", "item", "world"},
    "dodge": {"attack", "damage", "enemy"},
    "equip": {"ability", "gear", "item", "weapon"},
    "explore": {"level", "path", "puzzle", "quest", "resource", "world"},
    "fight": {"combat", "enemy", "party", "weapon"},
    "grow": {"ability", "economy", "relationship", "resource", "skill"},
    "hack": {"item", "system", "technology"},
    "interact": {"dialogue", "item", "puzzle", "relationship", "world"},
    "jump": {"level", "path", "stamina", "surface"},
    "manage": {"economy", "health", "inventory", "party", "resource"},
    "progress": {"experience", "level", "quest", "skill"},
    "repair": {"base", "gear", "item", "weapon"},
    "solve": {"puzzle", "quest"},
    "spend": {"currency", "resource"},
    "stun": {"enemy"},
    "trade": {"card", "currency", "economy", "item", "resource"},
    "unlock": {"ability", "card", "item", "level", "skill", "weapon"},
    "upgrade": {"ability", "card", "gear", "item", "skill", "weapon"},
}


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--ast", type=Path, required=True)
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--skip-model", action="store_true")
    parser.add_argument("--skip-codex", action="store_true")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="workflow-rawg-fastlane")
    configure_parser(parser)
    report = asyncio.run(run_workflow(parser.parse_args(argv)))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


async def run_workflow(args: argparse.Namespace) -> Dict[str, Any]:
    ast = load_ast(args.ast)
    controls = ast["controls"]
    workspace = (args.workspace or Path(controls["workspace"])).resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
    manifest = {
        "schema_version": "rawg.fastlane-manifest.v1",
        "run_id": run_id,
        "ast_path": str(args.ast.resolve()),
        "ast_hash": stable_hash(ast),
        "workflow_id": ast["workflow_id"],
        "controls": controls,
        "actual_workspace": str(workspace),
        "reused_artifacts": _artifact_lineage(workspace / "shards"),
        "started_at": datetime.now().astimezone().isoformat(),
    }
    _write_json(workspace / "manifest.json", manifest)
    context: Dict[str, Any] = {"manifest": manifest, "stage_reports": {}}
    history_writer = ShardedJsonlWriter(workspace / "shards", "stage-history", int(controls["shard_max_bytes"]))
    for node in ordered_nodes(ast):
        if node.get("enabled", True) is False:
            context["stage_reports"][node["id"]] = {"ok": True, "status": "disabled"}
            continue
        if node["op"] == "rawg.scan-group":
            context["stage_reports"][node["id"]] = _scan_group(node, controls, workspace, args.max_records)
        elif node["op"] == "group.expand-representatives":
            context["stage_reports"][node["id"]] = _expand_representative_strata(node, controls, workspace)
        elif node["op"] == "lfm.swarm-ideas":
            if args.skip_model:
                context["stage_reports"][node["id"]] = {"ok": True, "status": "skipped-by-cli"}
            else:
                context["stage_reports"][node["id"]] = await _swarm(node, controls, workspace)
        elif node["op"] == "lfm.refine-swarm":
            if args.skip_model:
                context["stage_reports"][node["id"]] = {"ok": True, "status": "skipped-by-cli"}
            else:
                context["stage_reports"][node["id"]] = await _refine_swarm(node, controls, workspace)
        elif node["op"] == "swarm.merge-clusters":
            context["stage_reports"][node["id"]] = _merge_swarm(node, controls, workspace)
        elif node["op"] == "lfm.refine-clusters":
            if args.skip_model:
                context["stage_reports"][node["id"]] = {"ok": True, "status": "skipped-by-cli"}
            else:
                context["stage_reports"][node["id"]] = await _refine_clusters(node, controls, workspace)
        elif node["op"] == "workflow.support-first-pipeline":
            if args.skip_model:
                context["stage_reports"][node["id"]] = {"ok": True, "status": "skipped-by-cli"}
            else:
                context["stage_reports"][node["id"]] = await _support_first_pipeline(node, controls, workspace)
        elif node["op"] == "filter.revalidate-atomic":
            context["stage_reports"][node["id"]] = _revalidate_atomic_candidates(node, controls, workspace)
        elif node["op"] == "lfm.expand-representatives":
            if args.skip_model:
                context["stage_reports"][node["id"]] = {"ok": True, "status": "skipped-by-cli"}
            else:
                context["stage_reports"][node["id"]] = await _expand(node, controls, workspace)
        elif node["op"] == "codex.review-recall":
            if args.skip_codex:
                context["stage_reports"][node["id"]] = {"ok": True, "status": "skipped-by-cli"}
            else:
                context["stage_reports"][node["id"]] = run_bounded_review(node, workspace, _review_packet(workspace, context))
        elif node["op"] == "codex.propose-kits":
            if args.skip_codex:
                context["stage_reports"][node["id"]] = {"ok": True, "status": "skipped-by-cli"}
            else:
                context["stage_reports"][node["id"]] = _propose_kits(node, controls, workspace)
        elif node["op"] == "workflow.report":
            context["stage_reports"][node["id"]] = {"ok": True, "status": "assembled"}
        history_writer.append({
            "schema_version": "rawg.fastlane-stage-history.v1",
            "run_id": run_id, "node_id": node["id"], "op": node["op"],
            "report": context["stage_reports"][node["id"]],
        })
    report = _final_report(ast, workspace, context)
    _write_json(workspace / "outputs.json", context)
    _write_json(workspace / "report.json", report)
    return report


def _scan_group(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path, max_records_override: Optional[int]) -> Dict[str, Any]:
    config = node.get("config") or {}
    source_root = Path(controls["source_root"])
    profile = config.get("fingerprint_profile", "balanced")
    max_representatives_per_group = int(config.get("max_representatives_per_group") or 1)
    max_records = max_records_override if max_records_override is not None else int(config.get("max_records") or 0)
    shard_bytes = int(controls["shard_max_bytes"])
    shards = workspace / "shards"
    event_writer = ShardedJsonlWriter(shards, "group-events", shard_bytes)
    ledger_writer = ShardedJsonlWriter(shards, "source-ledger", shard_bytes)
    representative_writer = ShardedJsonlWriter(shards, "representatives", shard_bytes)
    processed = _load_values(shards.glob("source-ledger-*.jsonl"), "identity")
    groups = GroupAccumulator()
    for event in _read_jsonl(shards.glob("group-events-*.jsonl")):
        groups.apply_event(event)
    started = time.monotonic()
    new_records = skipped = malformed = 0
    held = False
    for source, _, _ in stream_rawg_records(source_root, f"rawg-fastlane:{profile}"):
        identity = stable_hash([source.get("source_hash"), profile])
        if identity in processed:
            skipped += 1
            continue
        if new_records % 1000 == 0 and _free_gib(workspace) < float(controls.get("min_free_gib", 10.0)):
            held = True
            break
        normalized = normalize_for_fastlane(source)
        fingerprint = fingerprint_record(normalized, profile)
        first = groups.add(
            fingerprint["fingerprint"], fingerprint["status"] == "expandable",
            fingerprint["secondary_fingerprint"], max_representatives_per_group,
        )
        event = {
            "schema_version": "rawg.fast-group-event.v1",
            "identity": identity,
            "source_id": source.get("source_id"),
            "source_hash": source.get("source_hash"),
            "source_file": source.get("source_file"),
            "source_line": source.get("source_line"),
            **fingerprint,
            "representative": first,
        }
        event_writer.append(event)
        ledger_writer.append({
            "schema_version": "rawg.fast-source-ledger.v1", "identity": identity,
            "source_id": source.get("source_id"), "source_hash": source.get("source_hash"),
            "source_file": source.get("source_file"), "source_line": source.get("source_line"),
            "fingerprint": fingerprint["fingerprint"], "status": fingerprint["status"],
        })
        if first and fingerprint["status"] == "expandable":
            representative_writer.append({
                "schema_version": "rawg.fast-representative.v1",
                "representative_identity": stable_hash([fingerprint["fingerprint"], fingerprint["secondary_fingerprint"], source.get("source_hash")]),
                "fingerprint": fingerprint["fingerprint"], "signature": fingerprint["signature"],
                "secondary_fingerprint": fingerprint["secondary_fingerprint"],
                "secondary_signature": fingerprint["secondary_signature"],
                "source_id": source.get("source_id"), "source_hash": source.get("source_hash"),
                "source_file": source.get("source_file"), "source_line": source.get("source_line"),
            })
        processed.add(identity)
        new_records += 1
        malformed += bool(source.get("error"))
        if max_records and new_records >= max_records:
            break
    elapsed = max(0.001, time.monotonic() - started)
    total_seen = len(processed)
    rate = new_records / elapsed * 60
    expandable = groups.expandable_group_count
    group_sizes = sorted(groups.counts.values())
    p95_index = min(len(group_sizes) - 1, int(len(group_sizes) * 0.95)) if group_sizes else 0
    return {
        "ok": malformed == 0 and not held,
        "status": "hold" if held else ("limit-complete" if max_records else "complete"),
        "reason": "low-disk-space" if held else None,
        "fingerprint_profile": profile,
        "new_records": new_records,
        "skipped_existing": skipped,
        "total_records": total_seen,
        "groups": groups.group_count,
        "expandable_groups": expandable,
        "representatives": groups.representative_count,
        "max_representatives_per_group": max_representatives_per_group,
        "representative_ratio": round(groups.representative_count / max(total_seen, 1), 6),
        "max_group_size": max(group_sizes, default=0),
        "p95_group_size": group_sizes[p95_index] if group_sizes else 0,
        "largest_group_sizes": sorted(group_sizes, reverse=True)[:10],
        "dataset_records": TOTAL_RAWG_RECORDS,
        "records_per_minute": round(rate, 3),
        "projected_full_scan_hours": round(TOTAL_RAWG_RECORDS / max(rate, 0.001) / 60, 3),
        "malformed": malformed,
        "elapsed_seconds": round(elapsed, 3),
    }


def _expand_representative_strata(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    config = node.get("config") or {}
    thresholds = sorted(
        [(int(item["min_group_size"]), int(item["cap"])) for item in config.get("caps") or []],
        reverse=True,
    )
    default_cap = int(config.get("default_cap") or 12)
    shards = workspace / "shards"
    event_paths = list(shards.glob("group-events-*.jsonl"))
    counts = Counter(event["fingerprint"] for event in _read_jsonl(event_paths))
    selected: Dict[str, set[str]] = {}
    for rep in _read_jsonl(shards.glob("representatives-*.jsonl")):
        selected.setdefault(rep["fingerprint"], set()).add(rep.get("secondary_fingerprint") or rep["fingerprint"])
    writer = ShardedJsonlWriter(shards, "representatives", int(controls["shard_max_bytes"]))
    added = 0
    for event in _read_jsonl(event_paths):
        if event.get("status") != "expandable":
            continue
        fingerprint = event["fingerprint"]
        secondary = event["secondary_fingerprint"]
        current = selected.setdefault(fingerprint, set())
        cap = next((value for minimum, value in thresholds if counts[fingerprint] >= minimum), default_cap)
        if secondary in current or len(current) >= cap:
            continue
        writer.append({
            "schema_version": "rawg.fast-representative.v1",
            "representative_identity": stable_hash([fingerprint, secondary, event.get("source_hash")]),
            "fingerprint": fingerprint, "signature": event.get("signature") or [],
            "secondary_fingerprint": secondary, "secondary_signature": event.get("secondary_signature") or [],
            "source_id": event.get("source_id"), "source_hash": event.get("source_hash"),
            "source_file": event.get("source_file"), "source_line": event.get("source_line"),
            "selection_origin": "adaptive-secondary-stratum",
        })
        current.add(secondary)
        added += 1
    return {
        "ok": True, "status": "complete", "groups": len(counts),
        "previous_representatives": sum(len(values) for values in selected.values()) - added,
        "new_representatives": added,
        "total_representatives": sum(len(values) for values in selected.values()),
        "caps": [{"min_group_size": minimum, "cap": cap} for minimum, cap in thresholds],
        "default_cap": default_cap,
    }


async def _expand(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    config = node.get("config") or {}
    profile_ids = config.get("profile_ids") or [config.get("profile_id", "12b-seed-12b-walk-lean-beam2-d2")]
    profile_map = {item["profile_id"]: item for item in PROFILES if item["profile_id"] in set(profile_ids)}
    if len(profile_map) != len(set(profile_ids)):
        raise ValueError(f"unknown model expansion profile in: {profile_ids}")
    max_representatives = int(config.get("max_representatives") or 0)
    shards = workspace / "shards"
    completed = _load_values(shards.glob("model-ledger-*.jsonl"), "identity")
    representatives = list(_read_jsonl(shards.glob("representatives-*.jsonl")))
    assignments = [
        (item, profile_map[profile_ids[int(_representative_identity(item)[:8], 16) % len(profile_ids)]])
        for item in representatives
    ]
    pending = [(item, profile) for item, profile in assignments if stable_hash([_representative_identity(item), profile["profile_id"]]) not in completed]
    if max_representatives:
        pending = pending[:max_representatives]
    model_limits = controls.get("model_prediction_limits") or {}
    models = {profile[key] for profile in profile_map.values() for key in ("seed_model", "walk_model")}
    routers = {
        model: SmartRoutingService(
            controls.get("base_url", DEFAULT_BASE_URL), model, int(controls.get("timeout_seconds", 45)),
            max_predictions=int(model_limits.get(model, 8)), max_context_tokens=min(int(controls["max_context_tokens"]), 700),
        ) for model in models
    }
    writer = ShardedJsonlWriter(shards, "model-results", int(controls["shard_max_bytes"]))
    ledger = ShardedJsonlWriter(shards, "model-ledger", int(controls["shard_max_bytes"]))
    semaphore = asyncio.Semaphore(int(controls["task_concurrency"]))
    started = time.monotonic()

    async def one(rep: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            source = _hydrate_source(rep)
            worker_args = argparse.Namespace(beam_width=profile.get("beam_width", 2), max_depth=profile.get("max_depth", 2), max_tokens=int(config.get("max_tokens", 96)))
            result = await _run_game(source, profile, routers, worker_args)
            result["representative_fingerprint"] = rep["fingerprint"]
            result["expansion_profile_id"] = profile["profile_id"]
            return result

    try:
        results = await asyncio.gather(*(one(rep, profile) for rep, profile in pending))
    finally:
        for router in routers.values():
            router.shutdown()
    for (rep, profile), result in zip(pending, results):
        identity = stable_hash([_representative_identity(rep), profile["profile_id"]])
        writer.append(result)
        ledger.append({"identity": identity, "fingerprint": rep["fingerprint"], "profile_id": profile["profile_id"], "status": "complete"})
    elapsed = max(0.001, time.monotonic() - started)
    return {
        "ok": True, "status": "limit-complete" if max_representatives else "complete",
        "profile_ids": profile_ids, "new_representatives": len(results),
        "successful_hierarchies": sum(bool(item.get("ok")) for item in results),
        "profile_counts": dict(Counter(item.get("expansion_profile_id") for item in results)),
        "profile_successes": dict(Counter(item.get("expansion_profile_id") for item in results if item.get("ok"))),
        "representatives_per_minute": round(len(results) / elapsed * 60, 3),
        "elapsed_seconds": round(elapsed, 3),
        "router_stats": {model: router.stats() for model, router in routers.items()},
    }


async def _swarm(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    config = node.get("config") or {}
    if _free_gib(workspace) < float(controls.get("min_free_gib", 10.0)):
        return {"ok": False, "status": "hold", "reason": "low-disk-space"}
    model = config.get("model", MODEL_350M)
    if model != MODEL_350M:
        raise ValueError("the high-concurrency swarm lane is restricted to lfm2.5-350m")
    max_representatives = int(config.get("max_representatives") or 0)
    beam_width = int(config.get("beam_width") or 4)
    temperature = float(config.get("temperature") or 1.2)
    context_tokens = min(int(config.get("context_tokens") or 512), int(controls["max_context_tokens"]))
    prediction_limit = int((controls.get("model_prediction_limits") or {}).get(model, 64))
    provider = LMStudioProvider(controls.get("base_url", DEFAULT_BASE_URL), model, int(controls.get("timeout_seconds", 45)))
    health = provider.health()
    load = provider.ensure_loaded(context_tokens * prediction_limit, prediction_limit)
    reconfigure = None
    if load.get("ok") and not load.get("config_matches", True) and config.get("allow_reconfigure"):
        reconfigure = provider.unload(str(load.get("instance_id") or model))
        load = provider.ensure_loaded(context_tokens * prediction_limit, prediction_limit) if reconfigure.get("ok") else load
    if not health.get("ok") or not load.get("ok") or not load.get("config_matches", True):
        return {"ok": False, "status": "hold", "reason": "swarm-provider-preflight", "health": health, "load": load, "reconfigure": reconfigure}
    shards = workspace / "shards"
    swarm_config_id = stable_hash([model, temperature, beam_width, context_tokens, "deterministic-pair-plus-swarm-v4"])
    completed = _load_values(shards.glob("swarm-ledger-*.jsonl"), "identity")
    representatives = list(_read_jsonl(shards.glob("representatives-*.jsonl")))
    pending = [item for item in representatives if stable_hash([_representative_identity(item), swarm_config_id]) not in completed]
    if max_representatives:
        pending = pending[:max_representatives]
    router = SmartRoutingService(
        controls.get("base_url", DEFAULT_BASE_URL), model, int(controls.get("timeout_seconds", 45)),
        max_predictions=prediction_limit,
        max_context_tokens=context_tokens,
    )
    writer = ShardedJsonlWriter(shards, "swarm-results", int(controls["shard_max_bytes"]))
    ledger = ShardedJsonlWriter(shards, "swarm-ledger", int(controls["shard_max_bytes"]))
    semaphore = asyncio.Semaphore(int(controls["task_concurrency"]))
    started = time.monotonic()

    async def one(rep: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            source = _hydrate_source(rep)
            units = _matrix_evidence_units(source)[:8]
            active = [{
                "node_id": "root", "label": _seed_text(source, units), "level": 0,
                "lineage_evidence_source_ids": [item["source_id"] for item in units],
            }]
            controlled_groups = _controlled_evidence_groups(units)
            baseline_nodes = _deterministic_seed_nodes(source, units, active, controlled_groups)
            if not controlled_groups:
                return {
                    "schema_version": "rawg.fast-swarm-result.v1",
                    "representative_fingerprint": rep["fingerprint"],
                    "representative_identity": _representative_identity(rep),
                    "source_id": source.get("source_id"), "source_hash": source.get("source_hash"),
                    "source_file": source.get("source_file"), "source_line": source.get("source_line"),
                    "model": model, "temperature": temperature, "beam_width": beam_width,
                    "swarm_config_id": swarm_config_id, "prompt": None, "response": None, "attempts": 0,
                    "proposals": [], "baseline_nodes": baseline_nodes, "model_nodes": [], "accepted_nodes": baseline_nodes,
                    "rejections": [{"reason": "insufficient-controlled-mechanics"}], "ok": False,
                }
            prompt = _swarm_prompt(controlled_groups, beam_width)
            response, attempts = await router.chat(
                [{"role": "system", "content": "Return short diverse list items only; no reasoning."}, {"role": "user", "content": prompt}],
                temperature=temperature, max_tokens=int(config.get("max_tokens") or 64), retries=0,
            )
            proposals = _parse_output(response.content, active, beam_width)
            accepted, rejected = _filter_nodes(proposals, set(), source, units, 1, active)
            behavior_nodes = []
            for candidate in accepted:
                tokens = {_stem_token(value) for value in slug(candidate["label"]).split("-") if len(value) >= 4}
                raw_tokens = [value for value in slug(candidate["label"]).split("-") if value]
                if not 2 <= len(raw_tokens) <= 3 or not _atomic_relation_supported(candidate["label"], units):
                    rejected.append({**candidate, "reason": "not-behavior-shaped"})
                else:
                    behavior_nodes.append(candidate)
            accepted = behavior_nodes
            for candidate in accepted:
                candidate["origin"] = "lfm-350m-swarm"
            combined = {item["semantic_key"]: item for item in baseline_nodes}
            for candidate in accepted:
                combined.setdefault(candidate["semantic_key"], candidate)
            accepted = list(combined.values())
            return {
                "schema_version": "rawg.fast-swarm-result.v1",
                "representative_fingerprint": rep["fingerprint"],
                "representative_identity": _representative_identity(rep),
                "source_id": source.get("source_id"), "source_hash": source.get("source_hash"),
                "source_file": source.get("source_file"), "source_line": source.get("source_line"),
                "model": model, "temperature": temperature, "beam_width": beam_width,
                "swarm_config_id": swarm_config_id,
                "prompt": prompt, "response": response.to_dict(), "attempts": attempts,
                "proposals": proposals, "baseline_nodes": baseline_nodes, "model_nodes": behavior_nodes,
                "accepted_nodes": accepted, "rejections": rejected,
                "ok": bool(accepted),
            }

    try:
        results = await asyncio.gather(*(one(rep) for rep in pending))
        stats = router.stats()
    finally:
        router.shutdown()
    for rep, result in zip(pending, results):
        identity = stable_hash([_representative_identity(rep), swarm_config_id])
        writer.append(result)
        ledger.append({"identity": identity, "fingerprint": rep["fingerprint"], "swarm_config_id": swarm_config_id, "status": "complete"})
    elapsed = max(0.001, time.monotonic() - started)
    accepted_nodes = sum(len(item["accepted_nodes"]) for item in results)
    baseline_nodes = sum(len(item.get("baseline_nodes") or []) for item in results)
    model_nodes = sum(len(item.get("model_nodes") or []) for item in results)
    return {
        "ok": True, "status": "limit-complete" if max_representatives else "complete",
        "model": model, "swarm_config_id": swarm_config_id,
        "provider_load": load,
        "provider_reconfigure": reconfigure,
        "new_representatives": len(results), "representatives_with_ideas": sum(bool(item["ok"]) for item in results),
        "accepted_ideas": accepted_nodes, "deterministic_pair_ideas": baseline_nodes, "model_added_ideas": model_nodes,
        "accepted_ideas_per_call": round(accepted_nodes / max(stats.get("calls_completed", 0), 1), 3),
        "representatives_per_minute": round(len(results) / elapsed * 60, 3),
        "elapsed_seconds": round(elapsed, 3), "router_stats": stats,
    }


def _merge_swarm(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    config = node.get("config") or {}
    min_support = int(config.get("min_support") or 2)
    singleton_recall = int(config.get("singleton_recall") or 0)
    shards = workspace / "shards"
    clusters: Dict[str, Dict[str, Any]] = {}
    for result in _read_jsonl(shards.glob("swarm-results-*.jsonl")):
        for candidate in result.get("accepted_nodes") or []:
            tokens = {_stem_token(value) for value in slug(candidate.get("semantic_key") or candidate.get("label")).split("-") if value}
            actions = sorted(tokens & SWARM_STRONG_ACTIONS)
            objects = sorted(tokens - MATRIX_ACTION_MARKERS)
            if not actions or not objects or len(tokens) > 3 or set(objects) & {"ability", "action", "anything", "experience", "game", "play"}:
                continue
            key = "-".join([actions[0], objects[0], *[value for value in objects[1:] if value != objects[0]]])
            cluster = clusters.setdefault(key, {
                "cluster_key": key, "aliases": set(), "source_ids": set(), "references": [], "origins": Counter(),
            })
            cluster["aliases"].add(candidate.get("label"))
            cluster["source_ids"].add(result.get("source_id"))
            cluster["origins"][candidate.get("origin") or "legacy-swarm"] += 1
            if len(cluster["references"]) < 100:
                cluster["references"].append({
                    "source_id": result.get("source_id"), "source_hash": result.get("source_hash"),
                    "source_file": result.get("source_file"), "source_line": result.get("source_line"),
                    "evidence_source_ids": candidate.get("direct_evidence_source_ids") or [],
                    "origin": candidate.get("origin") or "legacy-swarm",
                })
    ledger_values = _load_values(shards.glob("swarm-cluster-ledger-*.jsonl"), "identity")
    writer = ShardedJsonlWriter(shards, "swarm-clusters", int(controls["shard_max_bytes"]))
    ledger = ShardedJsonlWriter(shards, "swarm-cluster-ledger", int(controls["shard_max_bytes"]))
    emitted = 0
    support_distribution = Counter()
    singleton_keys = [key for key, value in clusters.items() if len(value["source_ids"]) == 1]
    singleton_keys = sorted(singleton_keys, key=lambda key: stable_hash([key, "singleton-recall-order"]))[:singleton_recall]
    singleton_key_set = set(singleton_keys)
    for key, value in sorted(clusters.items()):
        support = len(value["source_ids"])
        support_distribution[_support_bucket(support)] += 1
        recall_lane = support == 1 and key in singleton_key_set
        if support < min_support and not recall_lane:
            continue
        bucket = _support_bucket(support)
        identity = stable_hash([key, bucket, "rawg.fast-swarm-cluster.v1"])
        if identity in ledger_values:
            continue
        record = {
            "schema_version": "rawg.fast-swarm-cluster.v1", "identity": identity,
            "cluster_id": f"swarm:{key}", "cluster_key": key,
            "label": sorted(value["aliases"], key=lambda item: (len(str(item)), str(item)))[0],
            "aliases": sorted(value["aliases"]), "source_ids": sorted(value["source_ids"]),
            "support_count": support, "support_bucket": bucket,
            "review_lane": "singleton-recall" if recall_lane else "supported",
            "origins": dict(value["origins"]), "evidence_references": value["references"],
            "evidence_hash": stable_hash([key, sorted(value["source_ids"]), value["references"]]),
        }
        writer.append(record)
        ledger.append({"identity": identity, "cluster_key": key, "support_bucket": bucket, "support_count": support})
        emitted += 1
    return {
        "ok": True, "status": "complete", "cluster_count": len(clusters),
        "eligible_clusters": sum(len(value["source_ids"]) >= min_support for value in clusters.values()),
        "singleton_recall_clusters": len(singleton_keys),
        "new_cluster_milestones": emitted, "min_support": min_support,
        "support_distribution": {str(key): value for key, value in sorted(support_distribution.items())},
    }


async def _refine_clusters(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    config = node.get("config") or {}
    if _free_gib(workspace) < float(controls.get("min_free_gib", 10.0)):
        return {"ok": False, "status": "hold", "reason": "low-disk-space"}
    model = config.get("model", MODEL_12B)
    if model != MODEL_12B:
        raise ValueError("cluster refinement is restricted to the quality 1.2B model")
    temperature = float(config.get("temperature") or 0.7)
    max_clusters = int(config.get("max_clusters") or 0)
    min_support = int(config.get("min_support") or 2)
    shards = workspace / "shards"
    singleton_quota = int(config.get("singleton_quota") or 0)
    refine_config_id = stable_hash([model, temperature, config.get("max_tokens", 96), singleton_quota, "support-cluster-atomic-v3"])
    latest: Dict[str, Dict[str, Any]] = {}
    for cluster in _read_jsonl(shards.glob("swarm-clusters-*.jsonl")):
        if cluster.get("support_count", 0) >= min_support or (config.get("include_singleton_recall") and cluster.get("review_lane") == "singleton-recall"):
            previous = latest.get(cluster["cluster_key"])
            if previous is None or cluster["support_count"] > previous["support_count"]:
                latest[cluster["cluster_key"]] = cluster
    completed = _load_values(shards.glob("cluster-refine-ledger-*.jsonl"), "identity")
    supported_pending = []
    singleton_pending = []
    for cluster in sorted(latest.values(), key=lambda value: (-value["support_count"], value["cluster_key"])):
        identity = stable_hash([cluster["identity"], refine_config_id])
        if identity not in completed:
            target = singleton_pending if cluster.get("review_lane") == "singleton-recall" else supported_pending
            target.append((identity, cluster))
    if max_clusters:
        recall_count = min(singleton_quota, len(singleton_pending), max_clusters)
        pending = supported_pending[: max_clusters - recall_count] + singleton_pending[:recall_count]
    else:
        pending = supported_pending + singleton_pending
    prediction_limit = int((controls.get("model_prediction_limits") or {}).get(model, 8))
    router = SmartRoutingService(
        controls.get("base_url", DEFAULT_BASE_URL), model, int(controls.get("timeout_seconds", 45)),
        max_predictions=prediction_limit, max_context_tokens=min(int(controls["max_context_tokens"]), 700),
    )
    writer = ShardedJsonlWriter(shards, "cluster-refine-results", int(controls["shard_max_bytes"]))
    ledger = ShardedJsonlWriter(shards, "cluster-refine-ledger", int(controls["shard_max_bytes"]))
    semaphore = asyncio.Semaphore(min(int(controls["task_concurrency"]), prediction_limit))
    started = time.monotonic()

    async def one(identity: str, cluster: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            units = []
            for reference in cluster.get("evidence_references", [])[:4]:
                source = _hydrate_source(reference)
                for unit in _matrix_evidence_units(source)[:6]:
                    units.append({**unit, "source_id": f"{source['source_id']}:{unit['source_id']}"})
            parent = {
                "node_id": "cluster-parent", "label": cluster["label"], "semantic_key": cluster["cluster_key"],
                "level": 1, "direct_evidence_source_ids": [unit["source_id"] for unit in units],
                "lineage_evidence_source_ids": [unit["source_id"] for unit in units],
            }
            prompt = _cluster_refine_prompt(cluster, units)
            response, attempts = await router.chat(
                [{"role": "system", "content": "Return short grounded subdomain lines only; no reasoning."}, {"role": "user", "content": prompt}],
                temperature=temperature, max_tokens=int(config.get("max_tokens") or 96), retries=0,
            )
            proposals = _parse_output(response.content, [parent], 3)
            synthetic_source = {
                "source_id": cluster["cluster_id"], "source_hash": cluster["evidence_hash"], "name": "",
            }
            accepted, rejected = _filter_nodes(proposals, {cluster["cluster_key"]}, synthetic_source, units, 2, [parent])
            atomic_nodes = []
            for candidate in accepted:
                label = candidate["label"]
                tokens = {_stem_token(value) for value in slug(label).split("-") if len(value) >= 4}
                raw_tokens = [value for value in slug(label).split("-") if value]
                actions = tokens & SWARM_STRONG_ACTIONS
                all_actions = tokens & MATRIX_ACTION_MARKERS
                objects = tokens - MATRIX_ACTION_MARKERS
                if "|" in label or len(raw_tokens) != 2 or len(set(raw_tokens)) != 2 or len(tokens) != 2 or len(actions) != 1 or len(all_actions) != 1 or len(objects) != 1 or not _atomic_relation_supported(label, units):
                    rejected.append({**candidate, "reason": "not-atomic-behavior"})
                else:
                    candidate["origin"] = "lfm-1.2b-cluster-refine"
                    atomic_nodes.append(candidate)
            return {
                "schema_version": "rawg.fast-cluster-refine-result.v1", "identity": identity,
                "cluster": cluster, "model": model, "temperature": temperature,
                "prompt": prompt, "response": response.to_dict(), "attempts": attempts,
                "proposals": proposals, "accepted_nodes": atomic_nodes, "rejections": rejected,
                "ok": bool(atomic_nodes),
            }

    try:
        results = await asyncio.gather(*(one(identity, cluster) for identity, cluster in pending))
        stats = router.stats()
    finally:
        router.shutdown()
    for (identity, cluster), result in zip(pending, results):
        writer.append(result)
        ledger.append({
            "identity": identity, "cluster_id": cluster["cluster_id"], "cluster_key": cluster["cluster_key"],
            "support_count": cluster["support_count"], "refine_config_id": refine_config_id, "status": "complete",
        })
    elapsed = max(0.001, time.monotonic() - started)
    accepted_nodes = sum(len(item["accepted_nodes"]) for item in results)
    novelty = _novelty_metrics(results) if results and config.get("novelty_metrics", True) else {"skipped": True}
    singleton_results = [item for item in results if (item.get("cluster") or {}).get("review_lane") == "singleton-recall"]
    return {
        "ok": True, "status": "limit-complete" if max_clusters else "complete",
        "model": model, "refine_config_id": refine_config_id,
        "new_clusters": len(results), "clusters_with_subdomains": sum(bool(item["ok"]) for item in results),
        "accepted_subdomains": accepted_nodes, "accepted_subdomains_per_call": round(accepted_nodes / max(len(results), 1), 3),
        "novelty": novelty,
        "singleton_recall": {
            "refined": len(singleton_results),
            "accepted": sum(bool(item.get("ok")) for item in singleton_results),
            "accepted_subdomains": sum(len(item.get("accepted_nodes") or []) for item in singleton_results),
        },
        "evidence_excerpt_coverage": {
            "results_with_accepted_nodes": sum(bool(item.get("accepted_nodes")) for item in results),
            "results_with_evidence_prompt": sum(bool(item.get("accepted_nodes")) and bool(item.get("prompt")) for item in results),
        },
        "full_corpus_refine_cap": int(config.get("full_corpus_cap") or 10000),
        "clusters_per_minute": round(len(results) / elapsed * 60, 3),
        "elapsed_seconds": round(elapsed, 3), "router_stats": stats,
    }


async def _refine_swarm(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    config = node.get("config") or {}
    model = config.get("model", MODEL_12B)
    if model != MODEL_12B:
        raise ValueError("swarm refinement is restricted to the quality 1.2B model")
    temperature = float(config.get("temperature") or 0.7)
    max_results = int(config.get("max_results") or 0)
    shards = workspace / "shards"
    refine_config_id = stable_hash([model, temperature, config.get("max_tokens", 96), "controlled-unused-v2"])
    completed = _load_values(shards.glob("refine-ledger-*.jsonl"), "identity")
    swarm_results = [item for item in _read_jsonl(shards.glob("swarm-results-*.jsonl")) if item.get("accepted_nodes")]
    pending = []
    for result in swarm_results:
        parent_keys = sorted(item["semantic_key"] for item in result["accepted_nodes"])
        identity = stable_hash([result.get("representative_identity") or result["representative_fingerprint"], parent_keys, refine_config_id])
        if identity not in completed:
            pending.append((identity, result))
    if max_results:
        pending = pending[:max_results]
    prediction_limit = int((controls.get("model_prediction_limits") or {}).get(model, 8))
    router = SmartRoutingService(
        controls.get("base_url", DEFAULT_BASE_URL), model, int(controls.get("timeout_seconds", 45)),
        max_predictions=prediction_limit, max_context_tokens=min(int(controls["max_context_tokens"]), 700),
    )
    writer = ShardedJsonlWriter(shards, "refine-results", int(controls["shard_max_bytes"]))
    ledger = ShardedJsonlWriter(shards, "refine-ledger", int(controls["shard_max_bytes"]))
    semaphore = asyncio.Semaphore(min(int(controls["task_concurrency"]), prediction_limit))
    started = time.monotonic()

    async def one(identity: str, swarm: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            source = _hydrate_source(swarm)
            units = _matrix_evidence_units(source)[:8]
            parents = swarm["accepted_nodes"][:4]
            prompt = _refine_prompt(parents, units)
            response, attempts = await router.chat(
                [{"role": "system", "content": "Return one grounded child per input parent; no reasoning."}, {"role": "user", "content": prompt}],
                temperature=temperature, max_tokens=int(config.get("max_tokens") or 96), retries=0,
            )
            proposals = _parse_output(response.content, parents, 1)
            accepted, rejected = _filter_nodes(proposals, {item["semantic_key"] for item in parents}, source, units, 2, parents)
            atomic_nodes = []
            for candidate in accepted:
                label = candidate["label"]
                tokens = {_stem_token(value) for value in slug(label).split("-") if len(value) >= 4}
                banned = {"against", "also", "download", "experience", "full", "narrative", "personal", "version", "will"}
                if "|" in label or tokens & banned or not (tokens & MATRIX_ACTION_MARKERS) or not (tokens - MATRIX_ACTION_MARKERS):
                    rejected.append({**candidate, "reason": "not-atomic-behavior"})
                else:
                    atomic_nodes.append(candidate)
            accepted = atomic_nodes
            expanded = {item["parent_id"] for item in accepted}
            return {
                "schema_version": "rawg.fast-refine-result.v1",
                "identity": identity, "representative_fingerprint": swarm["representative_fingerprint"],
                "source_id": source.get("source_id"), "source_hash": source.get("source_hash"),
                "source_file": source.get("source_file"), "source_line": source.get("source_line"),
                "model": model, "temperature": temperature, "parents": parents,
                "prompt": prompt, "response": response.to_dict(), "attempts": attempts,
                "proposals": proposals, "accepted_nodes": accepted, "rejections": rejected,
                "parent_coverage": round(len(expanded) / max(len(parents), 1), 4),
                "ok": bool(accepted), "complete": len(expanded) == len(parents),
            }

    try:
        results = await asyncio.gather(*(one(identity, result) for identity, result in pending))
        stats = router.stats()
    finally:
        router.shutdown()
    for (identity, swarm), result in zip(pending, results):
        writer.append(result)
        ledger.append({
            "identity": identity, "fingerprint": swarm["representative_fingerprint"],
            "refine_config_id": refine_config_id, "status": "complete",
        })
    elapsed = max(0.001, time.monotonic() - started)
    accepted_nodes = sum(len(item["accepted_nodes"]) for item in results)
    return {
        "ok": True, "status": "limit-complete" if max_results else "complete",
        "model": model, "refine_config_id": refine_config_id,
        "new_results": len(results), "results_with_subdomains": sum(bool(item["ok"]) for item in results),
        "complete_hierarchies": sum(bool(item["complete"]) for item in results),
        "accepted_subdomains": accepted_nodes, "accepted_subdomains_per_call": round(accepted_nodes / max(len(results), 1), 3),
        "results_per_minute": round(len(results) / elapsed * 60, 3),
        "elapsed_seconds": round(elapsed, 3), "router_stats": stats,
    }


def _revalidate_atomic_candidates(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    config = node.get("config") or {}
    max_candidates = int(config.get("max_candidates") or 100)
    shards = workspace / "shards"
    aggregates: Dict[str, Dict[str, Any]] = {}
    invalid = []
    input_nodes = 0
    for result in _read_jsonl(shards.glob("cluster-refine-results-*.jsonl")):
        cluster = result.get("cluster") or {}
        units = []
        for reference in cluster.get("evidence_references", [])[:8]:
            source = _hydrate_source(reference)
            for unit in _matrix_evidence_units(source)[:8]:
                units.append({**unit, "source_id": f"{source['source_id']}:{unit['source_id']}"})
        for candidate in result.get("accepted_nodes") or []:
            input_nodes += 1
            label = candidate.get("label") or ""
            key = _canonical_atomic_key(label)
            if not key or not _atomic_relation_supported(label, units):
                invalid.append({
                    "schema_version": "rawg.atomic-rejection.v1",
                    "identity": stable_hash([cluster.get("cluster_id"), candidate.get("semantic_key"), "relation-revalidation-v1"]),
                    "cluster_id": cluster.get("cluster_id"), "label": label,
                    "semantic_key": candidate.get("semantic_key"), "reason": "atomic-relation-not-entailed",
                    "source_ids": cluster.get("source_ids") or [], "evidence_hash": cluster.get("evidence_hash"),
                })
                continue
            value = aggregates.setdefault(key, {
                "aliases": set(), "source_ids": set(), "cluster_ids": set(), "evidence_hashes": set(), "evidence_excerpts": [],
            })
            value["aliases"].add(label)
            value["source_ids"].update(cluster.get("source_ids") or [])
            value["cluster_ids"].add(cluster.get("cluster_id"))
            value["evidence_hashes"].add(cluster.get("evidence_hash"))
            for unit in units:
                if _atomic_relation_supported(label, [unit]) and len(value["evidence_excerpts"]) < 12:
                    value["evidence_excerpts"].append({"source_id": unit["source_id"], "field": unit.get("field"), "text": unit.get("text")})
    inventory = build_capability_inventory(
        Path("/Users/crimsonwheeler/Documents/GitHub/NexusEngine"),
        Path("/Users/crimsonwheeler/Documents/GitHub/NexusEngine-ProtoKits"),
    )
    validated = []
    already_supported = 0
    for key, value in sorted(aggregates.items(), key=lambda item: (-len(item[1]["source_ids"]), item[0])):
        comparison = capability_status(key, inventory)
        if comparison.get("status") != "missing":
            already_supported += 1
            continue
        candidate_id = stable_hash([key, sorted(value["source_ids"]), sorted(value["evidence_hashes"]), "rawg.atomic-candidate.v1"])
        validated.append({
            "schema_version": "rawg.atomic-candidate.v1", "candidate_id": candidate_id,
            "semantic_key": key, "label": key.replace("-", " ").title(),
            "aliases": sorted(value["aliases"]), "support_count": len(value["source_ids"]),
            "source_ids": sorted(value["source_ids"]), "cluster_ids": sorted(value["cluster_ids"]),
            "evidence_hash": stable_hash(sorted(value["evidence_hashes"])),
            "evidence_excerpts": value["evidence_excerpts"],
            "inventory_status": "missing", "promotion_level": "proposal-only",
        })
    validated = validated[:max_candidates]
    existing_candidates = _load_values(shards.glob("validated-atomic-candidates-*.jsonl"), "candidate_id")
    existing_rejections = _load_values(shards.glob("atomic-rejections-*.jsonl"), "identity")
    candidate_writer = ShardedJsonlWriter(shards, "validated-atomic-candidates", int(controls["shard_max_bytes"]))
    rejection_writer = ShardedJsonlWriter(shards, "atomic-rejections", int(controls["shard_max_bytes"]))
    new_candidates = 0
    for candidate in validated:
        if candidate["candidate_id"] not in existing_candidates:
            candidate_writer.append(candidate)
            new_candidates += 1
    new_rejections = 0
    for rejection in invalid:
        if rejection["identity"] not in existing_rejections:
            rejection_writer.append(rejection)
            new_rejections += 1
    return {
        "ok": True, "status": "complete", "input_accepted_nodes": input_nodes,
        "relation_valid_nodes": sum(len(value["aliases"]) for value in aggregates.values()),
        "canonical_candidates": len(validated), "already_supported": already_supported,
        "new_candidates": new_candidates, "atomic_rejections": len(invalid), "new_atomic_rejections": new_rejections,
    }


def _propose_kits(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    config = node.get("config") or {}
    shards = workspace / "shards"
    candidates = list(_read_jsonl(shards.glob("validated-atomic-candidates-*.jsonl")))
    candidates.sort(key=lambda value: (-value.get("support_count", 0), value.get("semantic_key", "")))
    candidates = candidates[: int(config.get("max_candidates") or 50)]
    codex = run_kit_proposal_review(node, workspace, {"schema_version": "rawg.atomic-candidate-packet.v1", "candidates": candidates})
    writer = ShardedJsonlWriter(shards, "kit-build-requests", int(controls["shard_max_bytes"]))
    existing = _load_values(shards.glob("kit-build-requests-*.jsonl"), "proposal_id")
    candidate_map = {item["candidate_id"]: item for item in candidates}
    new_proposals = 0
    if codex.get("ok"):
        for kit in codex.get("kits") or []:
            candidate = candidate_map.get(kit.get("candidate_id"))
            kit_id = slug(kit.get("kit_id") or kit.get("name"))
            if not candidate or not kit_id:
                continue
            proposal_id = stable_hash([candidate["candidate_id"], kit_id, "proposal-only"])
            if proposal_id in existing:
                continue
            writer.append({
                "schema_version": "kit.build-request.v1", "proposal_id": proposal_id,
                "source_id": f"rawg-fastlane-{kit_id}", "name": kit.get("name") or kit_id.replace("-", " ").title(),
                "description": kit.get("owned_behavior") or "", "seed_domains": [slug(kit.get("domain")) or candidate["semantic_key"]],
                "constraints": ["render-agnostic", "idempotent", "snapshot-and-reset", "proposal-only", "codex-reviewed"],
                "source_context": {
                    "candidate_id": candidate["candidate_id"], "semantic_key": candidate["semantic_key"],
                    "rawg_source_ids": candidate["source_ids"][:100], "support_count": candidate["support_count"],
                    "evidence_hash": candidate["evidence_hash"], "promotion_level": "proposal-only",
                    "inputs": kit.get("inputs") or [], "outputs": kit.get("outputs") or [],
                    "novelty_reason": kit.get("novelty_reason") or "",
                },
            })
            existing.add(proposal_id)
            new_proposals += 1
    return {"ok": bool(codex.get("ok")), "status": "complete" if codex.get("ok") else "hold", "candidate_count": len(candidates), "new_proposals": new_proposals, "codex": codex}


async def _support_first_pipeline(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    config = node.get("config") or {}
    swarm_config = dict(config.get("swarm") or {})
    merge_config = dict(config.get("merge") or {})
    refine_config = dict(config.get("refine") or {})
    swarm_config["max_representatives"] = int(config.get("swarm_batch") or 2048)
    refine_config["max_clusters"] = int(config.get("refine_batch") or 96)
    max_iterations = int(config.get("max_iterations") or 1000)
    started = time.monotonic()
    iterations = []
    total_swarm_records = total_refine_clusters = total_cluster_milestones = 0
    summed_stage_seconds = 0.0
    for iteration in range(1, max_iterations + 1):
        swarm_task = asyncio.create_task(_swarm({"config": swarm_config}, controls, workspace))
        refine_task = asyncio.create_task(_refine_clusters({"config": refine_config}, controls, workspace))
        swarm_report, refine_report = await asyncio.gather(swarm_task, refine_task)
        merge_report = _merge_swarm({"config": merge_config}, controls, workspace)
        summed_stage_seconds += float(swarm_report.get("elapsed_seconds") or 0) + float(refine_report.get("elapsed_seconds") or 0)
        total_swarm_records += int(swarm_report.get("new_representatives") or 0)
        total_refine_clusters += int(refine_report.get("new_clusters") or 0)
        total_cluster_milestones += int(merge_report.get("new_cluster_milestones") or 0)
        iterations.append({"iteration": iteration, "swarm": swarm_report, "merge": merge_report, "refine": refine_report})
        if not swarm_report.get("ok") or not refine_report.get("ok") or not merge_report.get("ok"):
            break
        if (
            int(swarm_report.get("new_representatives") or 0) == 0
            and int(refine_report.get("new_clusters") or 0) == 0
            and int(merge_report.get("new_cluster_milestones") or 0) == 0
        ):
            break
    elapsed = max(0.001, time.monotonic() - started)
    complete = bool(iterations) and all(
        int(iterations[-1][stage].get(metric) or 0) == 0
        for stage, metric in [
            ("swarm", "new_representatives"), ("refine", "new_clusters"), ("merge", "new_cluster_milestones"),
        ]
    )
    all_refined = list(_read_jsonl((workspace / "shards").glob("cluster-refine-results-*.jsonl")))
    return {
        "ok": bool(iterations) and all(item[stage].get("ok") for item in iterations for stage in ["swarm", "merge", "refine"]),
        "status": "complete" if complete else "iteration-limit",
        "iterations": len(iterations),
        "swarm_batch": swarm_config["max_representatives"], "refine_batch": refine_config["max_clusters"],
        "total_swarm_representatives": total_swarm_records,
        "total_cluster_milestones": total_cluster_milestones,
        "total_refined_clusters": total_refine_clusters,
        "elapsed_seconds": round(elapsed, 3),
        "summed_stage_seconds": round(summed_stage_seconds, 3),
        "overlap_seconds": round(max(0.0, summed_stage_seconds - elapsed), 3),
        "novelty": _novelty_metrics(all_refined) if all_refined else {"skipped": True},
        "latest": iterations[-1] if iterations else None,
    }


def _review_packet(workspace: Path, context: Dict[str, Any]) -> Dict[str, Any]:
    shards = workspace / "shards"
    samples = {}
    for prefix in ["swarm-results", "refine-results", "cluster-refine-results", "model-results"]:
        values = list(_read_jsonl(shards.glob(f"{prefix}-*.jsonl")))
        samples[prefix] = [
            {
                "source_id": item.get("source_id"),
                "accepted_nodes": (item.get("accepted_nodes") or [])[:8],
                "rejections": (item.get("rejections") or [])[:5],
                "cluster": {
                    key: (item.get("cluster") or {}).get(key)
                    for key in ["cluster_id", "cluster_key", "support_count", "review_lane", "source_ids"]
                } if item.get("cluster") else None,
                "evidence_prompt_excerpt": str(item.get("prompt") or "")[:1200],
                "ok": item.get("ok"),
                "complete": item.get("complete"),
            }
            for item in values[-20:]
        ]
    return {
        "schema_version": "rawg.fastlane-review-packet.v1",
        "workspace": str(workspace),
        "manifest": context.get("manifest"),
        "stage_reports": context.get("stage_reports"),
        "stage_history": list(_read_jsonl(shards.glob("stage-history-*.jsonl")))[-40:],
        "artifact_paths": [str(path) for path in sorted(shards.glob("*.jsonl"))],
        "samples": samples,
    }


def _swarm_prompt(controlled_groups: List[Dict[str, Any]], beam_width: int) -> str:
    lines = "\n".join(f"{item['source_id']}: {', '.join(item['terms'])}" for item in controlled_groups)
    return (
        f"EVIDENCE-LOCAL MECHANIC WORDS:\n{lines}\n"
        f"Generate {beam_width} different broad reusable behavior seeds. Each seed is exactly 2 or 3 words and contains one strong action plus one acted-on object/state selected from the SAME evidence line. Do not mix lines. Leave other words unused for a later subdomain.\n"
        f"RETURN EXACTLY {beam_width} LINES. No numbering, names, genres, platforms, containers, or prose."
    )


def _refine_prompt(parents: List[Dict[str, Any]], units: List[Dict[str, Any]]) -> str:
    anchors = _controlled_evidence_terms(units)
    lines = []
    for parent in parents:
        parent_tokens = set(slug(parent["label"]).split("-"))
        unused = [value for value in anchors if _stem_token(value) not in {_stem_token(token) for token in parent_tokens}]
        lines.append(f"{parent['node_id']} | {parent['label']} | unused evidence: {', '.join(unused[:12])}")
    return (
        "Create exactly one narrower reusable subdomain for each parent. Preserve one parent behavior word and add one exact unused evidence word from that same line. Do not invent rules, thresholds, containers, names, or technology.\n"
        f"INPUTS:\n{chr(10).join(lines)}\n"
        f"RETURN EXACTLY {len(parents)} LINES: parent_id | 2 to 5 word subdomain. No prose."
    )


def _controlled_evidence_terms(units: List[Dict[str, Any]]) -> List[str]:
    allowed = MATRIX_ACTION_MARKERS | OBJECT_TERMS
    output = []
    for value in _evidence_anchor_terms(units):
        stem = _stem_token(value)
        if stem in allowed and stem not in output:
            output.append(stem)
    return output[:24]


def _controlled_evidence_groups(units: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    allowed = MATRIX_ACTION_MARKERS | OBJECT_TERMS
    output = []
    for unit in units:
        terms = []
        for value in slug(unit.get("text")).split("-"):
            stem = _stem_token(value)
            if stem in allowed and stem not in terms:
                terms.append(stem)
        if terms and set(terms) & SWARM_STRONG_ACTIONS and set(terms) - MATRIX_ACTION_MARKERS:
            output.append({"source_id": unit["source_id"], "terms": terms[:12]})
    return output[:8]


def _cluster_refine_prompt(cluster: Dict[str, Any], units: List[Dict[str, Any]]) -> str:
    evidence_lines = []
    for unit in units:
        controlled = _controlled_evidence_terms([unit])
        if controlled:
            evidence_lines.append(f"{unit['source_id']}: {', '.join(controlled)}")
    return (
        f"PARENT CAPABILITY: {cluster['label']}\n"
        f"SUPPORTING GAMES: {cluster['support_count']}\n"
        f"EVIDENCE-LOCAL WORDS:\n{chr(10).join(evidence_lines[:24])}\n"
        "Return up to three narrower atomic subdomains. Each retains one parent action/object word and adds one new word from a single evidence line. It must describe an action on an object/state, not a genre, name, container, or invented rule.\n"
        "RETURN: one 2 to 5 word subdomain per line. No ids, numbering, separators, or prose."
    )


def _deterministic_seed_nodes(
    source: Dict[str, Any], units: List[Dict[str, Any]], active: List[Dict[str, Any]], groups: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    proposals = []
    seen_labels = set()
    for group in groups:
        actions = [term for term in group["terms"] if term in SWARM_STRONG_ACTIONS]
        objects = [term for term in group["terms"] if term not in MATRIX_ACTION_MARKERS]
        for action in actions:
            for obj in objects:
                label = f"{action} {obj}"
                if label not in seen_labels and _atomic_relation_supported(label, units):
                    proposals.append({"parent_id": "root", "label": label})
                    seen_labels.add(label)
                if len(proposals) >= 4:
                    break
            if len(proposals) >= 4:
                break
        if len(proposals) >= 4:
            break
    kept, _ = _filter_nodes(proposals, set(), source, units, 1, active)
    for candidate in kept:
        candidate["origin"] = "deterministic-evidence-pair"
    return kept


def _support_bucket(support: int) -> int:
    return 1 if support <= 1 else 2 ** int(math.floor(math.log2(support)))


def _atomic_relation_supported(label: str, units: List[Dict[str, Any]]) -> bool:
    tokens = {_stem_token(value) for value in slug(label).split("-") if len(value) >= 4}
    actions = tokens & SWARM_STRONG_ACTIONS
    objects = tokens - MATRIX_ACTION_MARKERS
    if len(actions) != 1 or len(objects) != 1:
        return False
    action = next(iter(actions))
    obj = next(iter(objects))
    if obj not in ACTION_OBJECT_COMPATIBILITY.get(action, set()):
        return False
    for unit in units:
        words = [_stem_token(value) for value in slug(unit.get("text")).split("-") if value]
        action_positions = [index for index, value in enumerate(words) if value == action]
        object_positions = [index for index, value in enumerate(words) if value == obj]
        if action_positions and object_positions and min(abs(a - b) for a in action_positions for b in object_positions) <= 6:
            return True
    return False


def _canonical_atomic_key(label: str) -> str:
    tokens = {_stem_token(value) for value in slug(label).split("-") if len(value) >= 4}
    actions = tokens & SWARM_STRONG_ACTIONS
    objects = tokens - MATRIX_ACTION_MARKERS
    if len(actions) != 1 or len(objects) != 1:
        return ""
    return f"{next(iter(actions))}-{next(iter(objects))}"


def _novelty_metrics(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    keys = {item["semantic_key"] for result in results for item in result.get("accepted_nodes") or []}
    reference_keys = set()
    reference_root = Path("runs/workflow-harnesses/rawg-matrix-optimizer/20260712-023830-383/shards")
    for record in _read_jsonl(reference_root.glob("matrix-records-*.jsonl")):
        reference_keys.update(item.get("semantic_key") for item in record.get("accepted_nodes") or [] if item.get("semantic_key"))
    inventory = build_capability_inventory(
        Path("/Users/crimsonwheeler/Documents/GitHub/NexusEngine"),
        Path("/Users/crimsonwheeler/Documents/GitHub/NexusEngine-ProtoKits"),
    )
    inventory_missing = []
    inventory_supported = []
    for key in sorted(keys):
        status = capability_status(key, inventory)
        (inventory_missing if status.get("status") == "missing" else inventory_supported).append(key)
    return {
        "canonical_unique_subdomains": len(keys),
        "matrix_reference_overlap": len(keys & reference_keys),
        "matrix_reference_new": len(keys - reference_keys),
        "inventory_missing": len(inventory_missing),
        "inventory_supported": len(inventory_supported),
        "missing_keys_sample": inventory_missing[:25],
    }


def _hydrate_source(rep: Dict[str, Any]) -> Dict[str, Any]:
    path = Path(rep["source_file"])
    target = int(rep["source_line"])
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if line_number == target:
                raw = json.loads(line)
                from workflow_harnesses.rawg_capability_pipeline.source_adapter import normalize_rawg_record
                return normalize_rawg_record(raw, path, target, "rawg-fastlane-expand")
    raise RuntimeError(f"source pointer not found: {path}:{target}")


def _final_report(ast: Dict[str, Any], workspace: Path, context: Dict[str, Any]) -> Dict[str, Any]:
    stages = context["stage_reports"]
    files = list((workspace / "shards").glob("*.jsonl"))
    history = list(_read_jsonl((workspace / "shards").glob("stage-history-*.jsonl")))
    return {
        "ok": all(value.get("ok", False) for value in stages.values()),
        "schema_version": "rawg.fastlane-report.v1",
        "workflow_id": ast["workflow_id"],
        "workspace": str(workspace),
        "stages": stages,
        "shards": len(files),
        "max_observed_shard_bytes": max((path.stat().st_size for path in files), default=0),
        "malformed_jsonl": _count_malformed(files),
        "full_corpus_projection": _full_corpus_projection(stages, history),
    }


def _read_jsonl(paths: Iterable[Path]) -> Iterable[Dict[str, Any]]:
    for path in sorted(paths):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def _load_values(paths: Iterable[Path], key: str) -> set[str]:
    return {str(value[key]) for value in _read_jsonl(paths) if value.get(key)}


def _representative_identity(value: Dict[str, Any]) -> str:
    return str(value.get("representative_identity") or stable_hash([
        value.get("fingerprint"), value.get("secondary_fingerprint"), value.get("source_hash"), value.get("source_id"),
    ]))


def _count_malformed(paths: Iterable[Path]) -> int:
    malformed = 0
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
    return malformed


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _free_gib(path: Path) -> float:
    return shutil.disk_usage(path).free / (1024 ** 3)


def _artifact_lineage(shards: Path) -> List[Dict[str, Any]]:
    output = []
    for path in sorted(shards.glob("*.jsonl")):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        output.append({"path": str(path), "bytes": path.stat().st_size, "sha256": digest})
    return output


def _full_corpus_projection(stages: Dict[str, Any], history: List[Dict[str, Any]]) -> Dict[str, Any]:
    latest_by_op = {item.get("op"): item.get("report") or {} for item in history}
    scan = next((value for value in stages.values() if value.get("records_per_minute")), None) or latest_by_op.get("rawg.scan-group", {})
    swarm = next((value for value in stages.values() if value.get("representatives_per_minute")), None) or latest_by_op.get("lfm.swarm-ideas", {})
    merge = next((value for value in stages.values() if value.get("eligible_clusters") is not None), None) or latest_by_op.get("swarm.merge-clusters", {})
    refine = next((value for value in stages.values() if value.get("clusters_per_minute")), None) or latest_by_op.get("lfm.refine-clusters", {})
    if not scan or not swarm or not merge or not refine:
        return {"ok": False, "reason": "missing-stage-rate"}
    sampled_records = max(int(scan.get("total_records") or 0), 1)
    representatives = TOTAL_RAWG_RECORDS * float(scan.get("representative_ratio") or 0)
    swarm_hours = representatives / max(float(swarm.get("representatives_per_minute") or 0), 0.001) / 60
    projected_clusters = TOTAL_RAWG_RECORDS * int(merge.get("eligible_clusters") or 0) / sampled_records
    refine_calls = min(projected_clusters, int(refine.get("full_corpus_refine_cap") or 10000))
    refine_hours = refine_calls / max(float(refine.get("clusters_per_minute") or 0), 0.001) / 60
    scan_hours = TOTAL_RAWG_RECORDS / max(float(scan.get("records_per_minute") or 0), 0.001) / 60
    return {
        "ok": True,
        "projected_representatives": round(representatives),
        "projected_supported_clusters_uncapped": round(projected_clusters),
        "projected_refine_calls_capped": round(refine_calls),
        "scan_hours": round(scan_hours, 3),
        "swarm_hours": round(swarm_hours, 3),
        "refine_hours": round(refine_hours, 3),
        "sequential_hours_before_codex": round(scan_hours + swarm_hours + refine_hours, 3),
        "pipelined_hours_before_codex": round(scan_hours + max(swarm_hours, refine_hours), 3),
    }
