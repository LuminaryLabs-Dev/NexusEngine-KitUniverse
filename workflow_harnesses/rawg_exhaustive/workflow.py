from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from kituniverse_harness.smart_router import SmartRoutingService
from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash
from workflow_harnesses.rawg_capability_pipeline.inventory import build_capability_inventory, capability_status
from workflow_harnesses.rawg_capability_pipeline.source_adapter import stream_rawg_records
from workflow_harnesses.rawg_matrix_optimizer.workflow_rawg_matrix_optimizer import ShardedJsonlWriter
from workflow_harnesses.kit_universe_batch.simulator_adapter import resolve_simulator_cli, run_runtime_proof

from .ast import load_ast, ordered_nodes
from .authoring import author_runtime_kits
from .codex_runner import cleanup_scratch, peak_overlap
from .contracts import (
    BUILD_REQUEST_SCHEMA,
    MASTER_KIT_SCHEMA,
    REFINED_KIT_SCHEMA,
    validate_game_map,
    validate_interaction,
    validate_kit_observation,
)
from .decomposition import aggregate_pointer_masters, build_pointer_map, compact_pointer_map, facet_registry
from .domain_architecture import audit_domain_architecture, is_architectural_domain, place_domain_architecture
from .domain_review import apply_domain_mapping, load_active_domain_mappings, organize_accepted_domains
from .evidence import (
    ACTION_WORDS,
    TARGET_ALIASES,
    canonical_action,
    build_game_evidence_map,
    deterministic_interactions,
    game_domain_kit_map,
    kit_observation,
    model_interaction,
)
from .implementation import build_runtime_package, descriptor_architecture, runtime_kit_shape
from .master_review import run_master_review


TOTAL_RAWG_RECORDS = 881_069


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--ast", type=Path, required=True)
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--max-model-pages", type=int)
    parser.add_argument("--max-master-kits", type=int)
    parser.add_argument("--max-builds", type=int)
    parser.add_argument("--max-authored-kits", type=int)
    parser.add_argument("--max-codex-batches", type=int)
    parser.add_argument("--max-domain-batches", type=int)
    parser.add_argument("--start-at", help="Resume at this workflow node id without replaying earlier stages")
    parser.add_argument("--stop-after", help="Stop cleanly after this workflow node id")
    parser.add_argument("--skip-scan", action="store_true")
    parser.add_argument("--skip-model", action="store_true")
    parser.add_argument("--skip-codex", action="store_true")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="workflow-rawg-exhaustive")
    configure_parser(parser)
    report = asyncio.run(run_workflow(parser.parse_args(argv)))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


async def run_workflow(args: argparse.Namespace) -> Dict[str, Any]:
    ast = load_ast(args.ast)
    nodes = ordered_nodes(ast)
    node_ids = [str(node["id"]) for node in nodes]
    if args.start_at and args.start_at not in node_ids:
        raise ValueError(f"unknown --start-at node: {args.start_at}")
    if args.stop_after and args.stop_after not in node_ids:
        raise ValueError(f"unknown --stop-after node: {args.stop_after}")
    start_index = node_ids.index(args.start_at) if args.start_at else 0
    stop_index = node_ids.index(args.stop_after) if args.stop_after else len(nodes) - 1
    if start_index > stop_index:
        raise ValueError("--start-at must not come after --stop-after")
    controls = dict(ast["controls"])
    workspace = (args.workspace or Path(controls["workspace"])).resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    prior_manifest: Dict[str, Any] = {}
    manifest_path = workspace / "manifest.json"
    if manifest_path.exists():
        try:
            prior_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            prior_manifest = {"unreadable": True}
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
    ast_hash = stable_hash(ast)
    git_state = _git_state(Path.cwd())
    epoch_id = stable_hash([run_id, ast_hash, git_state])
    controls["pipeline_epoch"] = epoch_id
    manifest = {
        "schema_version": "rawg.exhaustive-manifest.v2",
        "run_id": run_id,
        "pipeline_epoch": epoch_id,
        "workflow_id": ast["workflow_id"],
        "ast_path": str(args.ast.resolve()),
        "ast_hash": ast_hash,
        "git_commit": git_state["git_commit"],
        "dirty_tree": git_state["dirty_tree"],
        "dirty_tree_hash": git_state["dirty_tree_hash"],
        "prior_run_id": prior_manifest.get("run_id"),
        "prior_pipeline_epoch": prior_manifest.get("pipeline_epoch"),
        "controls": controls,
        "started_at": datetime.now().astimezone().isoformat(),
        "workspace": str(workspace),
        "start_at": args.start_at,
        "stop_after": args.stop_after,
    }
    _write_json(manifest_path, manifest)
    reports: Dict[str, Any] = {}
    history = ShardedJsonlWriter(workspace / "shards", "stage-history", int(controls["shard_max_bytes"]))
    epochs = ShardedJsonlWriter(workspace / "shards", "processing-epoch-events", int(controls["shard_max_bytes"]))
    epochs.append({
        "schema_version": "rawg.processing-epoch-event.v1",
        "event": "started",
        **manifest,
    })
    for node_index, node in enumerate(nodes):
        op = node["op"]
        if node_index < start_index:
            report = {"ok": True, "status": "skipped-before-start", "resume_node": args.start_at}
        elif node_index > stop_index:
            break
        elif op == "rawg.map-evidence":
            report = {"ok": True, "status": "skipped-by-cli"} if args.skip_scan else _map_evidence(node, controls, workspace, args.max_records)
        elif op == "lfm.extract-interactions":
            report = {"ok": True, "status": "skipped-by-cli"} if args.skip_model else await _extract_interactions(
                node, controls, workspace, args.max_model_pages
            )
        elif op == "master.merge-observations":
            report = _merge_master(node, controls, workspace)
        elif op == "kit.expand-pointer-decomposition":
            report = _expand_pointer_decomposition(node, controls, workspace)
        elif op == "lfm.refine-master-kits":
            report = {"ok": True, "status": "skipped-by-cli"} if args.skip_model else await _refine_master(
                node, controls, workspace, args.max_master_kits
            )
        elif op == "codex.review-master-kits":
            report = {"ok": True, "status": "skipped-by-cli"} if args.skip_codex else _review_master_kits(
                node, controls, workspace, args.max_codex_batches
            )
        elif op == "codex.organize-domains":
            report = {"ok": True, "status": "skipped-by-cli"} if args.skip_codex else organize_accepted_domains(
                node, controls, workspace, args.max_domain_batches
            )
        elif op == "kit.enqueue-builds":
            report = _enqueue_builds(node, controls, workspace)
        elif op == "codex.author-runtime-kits":
            report = {"ok": True, "status": "skipped-by-cli"} if args.skip_codex else author_runtime_kits(
                node, controls, workspace, args.max_authored_kits
            )
        elif op == "kit.build-runtime-prove":
            report = _build_runtime_prove(node, controls, workspace, args.max_builds)
        elif op == "kit.place-domain-architecture":
            report = place_domain_architecture(workspace, controls)
        elif op == "kit.audit-domain-architecture":
            report = audit_domain_architecture(workspace, controls)
        else:
            report = {"ok": True, "status": "assembled"}
        reports[node["id"]] = report
        history.append({
            "schema_version": "rawg.exhaustive-stage-history.v1",
            "run_id": manifest["run_id"],
            "pipeline_epoch": epoch_id,
            "node_id": node["id"],
            "op": op,
            "report": report,
        })
        if not report.get("ok") and report.get("status") == "hold":
            break
        if node_index == stop_index:
            break
    reconciliation = _reconcile_run(workspace, controls, reports)
    reports["run-reconciliation"] = reconciliation
    history.append({
        "schema_version": "rawg.exhaustive-stage-history.v1",
        "run_id": manifest["run_id"],
        "pipeline_epoch": epoch_id,
        "node_id": "run-reconciliation",
        "op": "workflow.reconcile-run",
        "report": reconciliation,
    })
    output = {"manifest": manifest, "stage_reports": reports}
    report = _report(ast, workspace, reports)
    _write_json(workspace / "outputs.json", output)
    _write_json(workspace / "report.json", report)
    epochs.append({
        "schema_version": "rawg.processing-epoch-event.v1",
        "event": "finished" if report.get("ok") else "held-or-failed",
        "run_id": run_id,
        "pipeline_epoch": epoch_id,
        "finished_at": datetime.now().astimezone().isoformat(),
        "ok": report.get("ok"),
    })
    return report


def _map_evidence(
    node: Dict[str, Any], controls: Dict[str, Any], workspace: Path, max_records_override: Optional[int]
) -> Dict[str, Any]:
    config = node.get("config") or {}
    limit = max_records_override if max_records_override is not None else int(config.get("max_records") or 0)
    shards = workspace / "shards"
    shard_bytes = int(controls["shard_max_bytes"])
    map_writer = ShardedJsonlWriter(shards, "game-evidence-maps", shard_bytes)
    ledger_writer = ShardedJsonlWriter(shards, "game-evidence-ledger", shard_bytes)
    interaction_writer = ShardedJsonlWriter(shards, "mechanic-interactions", shard_bytes)
    observation_writer = ShardedJsonlWriter(shards, "kit-observations", shard_bytes)
    game_map_writer = ShardedJsonlWriter(shards, "game-domain-kit-maps", shard_bytes)
    completed = _load_values(shards.glob("game-evidence-ledger-*.jsonl"), "identity")
    started = time.monotonic()
    new = skipped = malformed = interactions = observations = insufficient = 0
    domain_counts: Counter[str] = Counter()
    held = False
    source_root = Path(controls["source_root"])
    for source, _, _ in stream_rawg_records(source_root, "rawg-exhaustive-v1"):
        identity = stable_hash([source.get("source_hash"), "rawg-exhaustive-v1"])
        if identity in completed:
            skipped += 1
            continue
        if new % 1000 == 0 and _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
            break
        evidence_map = build_game_evidence_map(source)
        evidence_map["pipeline_epoch"] = controls.get("pipeline_epoch")
        map_writer.append(evidence_map)
        local_interactions = deterministic_interactions(evidence_map)
        local_observations = []
        for interaction in local_interactions:
            if validate_interaction(interaction):
                continue
            observation = kit_observation(interaction)
            if validate_kit_observation(observation):
                continue
            interaction["pipeline_epoch"] = controls.get("pipeline_epoch")
            observation["pipeline_epoch"] = controls.get("pipeline_epoch")
            interaction_writer.append(interaction)
            observation_writer.append(observation)
            local_observations.append(observation)
            interactions += 1
            observations += 1
        game_map = game_domain_kit_map(evidence_map, local_observations)
        game_map["pipeline_epoch"] = controls.get("pipeline_epoch")
        if validate_game_map(game_map):
            raise RuntimeError(f"invalid game map for {source.get('source_id')}")
        game_map_writer.append(game_map)
        ledger_writer.append({
            "schema_version": "rawg.game-evidence-ledger.v1",
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "identity": identity,
            "source_id": source.get("source_id"),
            "source_hash": source.get("source_hash"),
            "map_id": evidence_map["map_id"],
            "coverage_status": evidence_map["coverage_status"],
            "evidence_units": len(evidence_map["evidence_units"]),
            "mechanical_units": evidence_map["mechanical_unit_count"],
            "deterministic_interactions": len(local_interactions),
        })
        completed.add(identity)
        new += 1
        malformed += bool(source.get("error"))
        insufficient += evidence_map["coverage_status"] == "insufficient-evidence"
        domain_counts.update(evidence_map["domain_coverage"])
        if limit and new >= limit:
            break
    elapsed = max(0.001, time.monotonic() - started)
    return {
        "ok": malformed == 0 and not held,
        "status": "hold" if held else ("limit-complete" if limit else "complete"),
        "reason": "low-disk-space" if held else None,
        "new_records": new,
        "skipped_existing": skipped,
        "total_records": len(completed),
        "dataset_records": TOTAL_RAWG_RECORDS,
        "malformed": malformed,
        "insufficient_evidence": insufficient,
        "deterministic_interactions": interactions,
        "kit_observations": observations,
        "domain_coverage": dict(domain_counts.most_common()),
        "elapsed_seconds": round(elapsed, 3),
        "records_per_minute": round(new / elapsed * 60, 3),
    }


async def _extract_interactions(
    node: Dict[str, Any], controls: Dict[str, Any], workspace: Path, max_pages_override: Optional[int]
) -> Dict[str, Any]:
    config = node.get("config") or {}
    model = str(config.get("model") or "lfm2.5-350m")
    page_size = max(1, min(12, int(config.get("evidence_units_per_page") or 8)))
    limit = max_pages_override if max_pages_override is not None else int(config.get("max_pages") or 0)
    limits = controls.get("model_prediction_limits") or {}
    router = SmartRoutingService(
        controls["base_url"], model, int(controls.get("timeout_seconds", 45)),
        max_predictions=int(limits[model]),
        max_context_tokens=min(int(config.get("context_tokens") or 512), int(controls["max_context_tokens"])),
    )
    shards = workspace / "shards"
    shard_bytes = int(controls["shard_max_bytes"])
    result_writer = ShardedJsonlWriter(shards, "interaction-page-results", shard_bytes)
    ledger_writer = ShardedJsonlWriter(shards, "interaction-page-ledger", shard_bytes)
    interaction_writer = ShardedJsonlWriter(shards, "mechanic-interactions", shard_bytes)
    observation_writer = ShardedJsonlWriter(shards, "kit-observations", shard_bytes)
    completed = {
        item["identity"]
        for item in _read_jsonl(shards.glob("interaction-page-ledger-*.jsonl"))
        if item.get("identity") and item.get("status") == "extracted"
    }
    existing_interactions = _load_values(shards.glob("mechanic-interactions-*.jsonl"), "interaction_id")
    existing_observations = _load_values(shards.glob("kit-observations-*.jsonl"), "observation_id")
    task_concurrency = int(controls["task_concurrency"])
    batch_size = max(task_concurrency, int(config.get("dispatch_batch") or 1024))
    pending: List[Tuple[List[Tuple[Dict[str, Any], Dict[str, Any]]], str]] = []
    pages_seen = pages_done = accepted = rejected = duplicates = calls = 0
    held = False
    started = time.monotonic()

    async def flush() -> None:
        nonlocal pages_done, accepted, rejected, duplicates, calls, held
        if not pending:
            return
        if _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            pending.clear()
            held = True
            return
        semaphore = asyncio.Semaphore(task_concurrency)

        async def one(item: Tuple[List[Tuple[Dict[str, Any], Dict[str, Any]]], str]) -> Dict[str, Any]:
            entries, page_id = item
            units = [unit for _, unit in entries]
            async with semaphore:
                prompt = _interaction_prompt(units)
                response, attempts = await router.chat(
                    [{"role": "system", "content": "Return short evidence-local list items only. No reasoning."}, {"role": "user", "content": prompt}],
                    temperature=float(config.get("temperature", 1.2)),
                    max_tokens=int(config.get("max_tokens") or 160),
                    retries=int(config.get("retries") or 1),
                )
                parsed = _parse_swarm_lines(response.content)
                return {"entries": entries, "page_id": page_id, "response": response, "attempts": attempts, "parsed": parsed}

        tasks = [asyncio.create_task(one(item)) for item in list(pending)]
        for task in asyncio.as_completed(tasks):
            result = await task
            page_accepted = 0
            entries = result["entries"]
            for relation in result["parsed"]:
                try:
                    index = int(relation.get("i", -1))
                except (TypeError, ValueError):
                    rejected += 1
                    continue
                if index < 0 or index >= len(entries):
                    rejected += 1
                    continue
                game_map, unit = entries[index]
                normalized = {
                    "subject": relation.get("s") or "player-or-system",
                    "trigger": relation.get("g") or "",
                    "condition": relation.get("c") or "",
                    "action": relation.get("a") or "",
                    "target": relation.get("t") or "",
                    "effect": relation.get("e") or "",
                    "duration": relation.get("d") or "",
                    "stacking": relation.get("k") or "",
                    "cancellation": relation.get("x") or "",
                    "resulting_state": relation.get("r") or "",
                }
                interaction = model_interaction(game_map, unit, normalized)
                if interaction is None:
                    rejected += 1
                    continue
                errors = validate_interaction(interaction)
                if errors:
                    rejected += 1
                    continue
                if interaction["interaction_id"] in existing_interactions:
                    duplicates += 1
                    continue
                observation = kit_observation(interaction)
                errors.extend(validate_kit_observation(observation))
                if errors:
                    rejected += 1
                    continue
                interaction["pipeline_epoch"] = controls.get("pipeline_epoch")
                observation["pipeline_epoch"] = controls.get("pipeline_epoch")
                interaction_writer.append(interaction)
                observation_writer.append(observation)
                existing_interactions.add(interaction["interaction_id"])
                existing_observations.add(observation["observation_id"])
                page_accepted += 1
                accepted += 1
            response = result["response"]
            result_writer.append({
                "schema_version": "rawg.interaction-page-result.v1",
                "prompt_version": "interaction-page-v5-all-action-tokens",
                "pipeline_epoch": controls.get("pipeline_epoch"),
                "identity": result["page_id"],
                "source_ids": [game_map["source_id"] for game_map, _ in entries],
                "source_hashes": [game_map["source_hash"] for game_map, _ in entries],
                "evidence_ids": [unit["evidence_id"] for _, unit in entries],
                "model": model,
                "attempts": result["attempts"],
                "ok": response.ok,
                "response": response.content,
                "usage": response.usage,
                "accepted_interactions": page_accepted,
            })
            ledger_writer.append({
                "schema_version": "rawg.interaction-page-ledger.v1",
                "prompt_version": "interaction-page-v5-all-action-tokens",
                "pipeline_epoch": controls.get("pipeline_epoch"),
                "identity": result["page_id"],
                "source_ids": [game_map["source_id"] for game_map, _ in entries],
                "status": "extracted" if response.ok else "failed",
            })
            if response.ok:
                completed.add(result["page_id"])
            pages_done += 1
            calls += result["attempts"]
        pending.clear()

    try:
        stop = False
        page_entries: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []

        async def queue_page(entries: List[Tuple[Dict[str, Any], Dict[str, Any]]]) -> None:
            nonlocal pages_seen, stop, held
            if pages_seen % 128 == 0 and _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
                held = True
                stop = True
                return
            page_id = stable_hash([[(game_map["map_id"], unit["evidence_id"]) for game_map, unit in entries], model, "interaction-page-v5-all-action-tokens"])
            pages_seen += 1
            if page_id not in completed:
                pending.append((list(entries), page_id))
                if len(pending) >= batch_size:
                    await flush()
            if limit and pages_done + len(pending) >= limit:
                stop = True

        for game_map in _read_jsonl(shards.glob("game-evidence-maps-*.jsonl")):
            mechanical = [unit for unit in game_map.get("evidence_units") or [] if unit.get("mechanical") and _unit_has_relation_terms(unit)]
            for unit in mechanical:
                page_entries.append((game_map, unit))
                if len(page_entries) >= page_size:
                    await queue_page(page_entries)
                    page_entries = []
                if stop:
                    break
            if stop:
                break
        if page_entries and not stop:
            await queue_page(page_entries)
        await flush()
    finally:
        stats = router.stats()
        router.shutdown()
    elapsed = max(0.001, time.monotonic() - started)
    return {
        "ok": not held,
        "status": "hold" if held else ("limit-complete" if limit else "complete"),
        "reason": "low-disk-space" if held else None,
        "model": model,
        "pages_seen": pages_seen,
        "new_pages": pages_done,
        "completed_pages": len(completed),
        "accepted_interactions": accepted,
        "rejected_relations": rejected,
        "duplicate_relations_skipped": duplicates,
        "model_calls": calls,
        "elapsed_seconds": round(elapsed, 3),
        "pages_per_minute": round(pages_done / elapsed * 60, 3),
        "router_stats": stats,
    }


def _interaction_prompt(units: Sequence[Dict[str, Any]]) -> str:
    lines = []
    for index, unit in enumerate(units):
        words = [slug(value) for value in str(unit["text"]).split()]
        actions: List[str] = []
        targets: List[str] = []
        for word in words:
            action = canonical_action(word)
            if action and action not in actions:
                actions.append(action)
            target = TARGET_ALIASES.get(word)
            if target and target not in targets:
                targets.append(target)
        lines.append(f"{index}: {', '.join([*actions, *targets])}")
    return (
        "EVIDENCE-LOCAL WORDS:\n" + "\n".join(lines) + "\n"
        "For each input, return every distinct grounded action/object mechanic present; the same index may appear more than once. "
        "RETURN ONE LINE PER PAIR: index | action | object. Use only words from that input. No prose."
    )


def _unit_has_relation_terms(unit: Dict[str, Any]) -> bool:
    words = set(slug(unit.get("text") or "").split("-"))
    return any(canonical_action(word) for word in words) and any(word in TARGET_ALIASES for word in words)


def _parse_array(text: str) -> List[Dict[str, Any]]:
    raw = str(text or "").strip()
    if "```" in raw:
        raw = raw.replace("```json", "").replace("```", "").strip()
    start, end = raw.find("["), raw.rfind("]")
    if start < 0 or end < start:
        return []
    try:
        value = json.loads(raw[start : end + 1])
    except json.JSONDecodeError:
        return []
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _parse_swarm_lines(text: str) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for line in str(text or "").replace("```", "").splitlines():
        parts = [slug(part) for part in line.split("|")]
        if len(parts) < 3 or not parts[0].isdigit():
            continue
        for index, token in enumerate(parts[1:], start=1):
            if not canonical_action(token):
                continue
            target = parts[index + 1] if index + 1 < len(parts) else ""
            output.append({"i": int(parts[0]), "a": token, "t": target})
    return output


def _expand_pointer_decomposition(
    node: Dict[str, Any], controls: Dict[str, Any], workspace: Path
) -> Dict[str, Any]:
    shards = workspace / "shards"
    shard_bytes = int(controls["shard_max_bytes"])
    pipeline_epoch = str(controls.get("pipeline_epoch") or "unknown-epoch")
    _write_json(workspace / "kit-facet-registry.json", facet_registry())
    pointer_writer = ShardedJsonlWriter(shards, "game-kit-pointer-maps", shard_bytes)
    pointer_ledger = ShardedJsonlWriter(shards, "game-kit-pointer-ledger", shard_bytes)
    completed = {
        item["identity"]
        for item in _read_jsonl(shards.glob("game-kit-pointer-ledger-*.jsonl"))
        if item.get("identity") and item.get("status") == "decomposed"
    }
    interactions_by_source: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for interaction in _read_jsonl(shards.glob("mechanic-interactions-*.jsonl")):
        source_hash = str(interaction.get("source_hash") or "")
        if source_hash:
            interactions_by_source[source_hash].append({
                "interaction_id": interaction.get("interaction_id"),
                "semantic_key": interaction.get("semantic_key"),
                "source_id": interaction.get("source_id"),
                "source_hash": source_hash,
                "relation": interaction.get("relation") or {},
                "domains": interaction.get("domains") or [],
                "evidence": interaction.get("evidence") or {},
                "origin": interaction.get("origin") or "unknown",
            })

    new_maps = new_seeds = new_expanded = hundreds = evidence_limited = insufficient = 0
    held = False
    for game_map in _read_jsonl(shards.glob("game-evidence-maps-*.jsonl")):
        source_hash = game_map["source_hash"]
        if source_hash in completed:
            continue
        if new_maps % 128 == 0 and _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
            break
        pointer = build_pointer_map(game_map, interactions_by_source.get(source_hash, []), pipeline_epoch)
        pointer_writer.append(compact_pointer_map(pointer))
        pointer_ledger.append({
            "schema_version": "rawg.game-kit-pointer-ledger.v1",
            "pipeline_epoch": pipeline_epoch,
            "identity": source_hash,
            "pointer_map_id": pointer["pointer_map_id"],
            "status": "decomposed",
            "seed_count": pointer["seed_count"],
            "expanded_node_count": pointer["expanded_node_count"],
            "coverage_status": pointer["coverage_status"],
        })
        completed.add(source_hash)
        new_maps += 1
        new_seeds += pointer["seed_count"]
        new_expanded += pointer["expanded_node_count"]
        hundreds += pointer["coverage_status"] == "hundreds-decomposed"
        evidence_limited += pointer["coverage_status"] == "evidence-limited"
        insufficient += pointer["coverage_status"] == "insufficient-evidence"
    if held:
        return {
            "ok": False, "status": "hold", "reason": "low-disk-space",
            "new_pointer_maps": new_maps, "total_pointer_maps": len(completed),
            "new_seeds": new_seeds, "new_expanded_nodes": new_expanded,
        }

    inventory = build_capability_inventory(Path(controls["engine_root"]), Path(controls["protokits_root"]))
    master_writer = ShardedJsonlWriter(shards, "pointer-master-kits", shard_bytes)
    existing_master_keys = _load_values(shards.glob("pointer-master-kits-*.jsonl"), "semantic_key")
    new_masters = pointer_masters_seen = 0
    regenerated_pointers = (
        build_pointer_map(game_map, interactions_by_source.get(game_map["source_hash"], []), pipeline_epoch)
        for game_map in _read_jsonl(shards.glob("game-evidence-maps-*.jsonl"))
    )
    for master in aggregate_pointer_masters(
        regenerated_pointers,
        lambda key: capability_status(key, inventory),
        pipeline_epoch,
    ):
        pointer_masters_seen += 1
        if master["semantic_key"] in existing_master_keys:
            continue
        if new_masters % 1000 == 0 and _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
            break
        master_writer.append(master)
        existing_master_keys.add(master["semantic_key"])
        new_masters += 1

    totals = Counter()
    total_seeds = total_expanded = 0
    for item in _read_jsonl(shards.glob("game-kit-pointer-ledger-*.jsonl")):
        if item.get("status") != "decomposed":
            continue
        totals[str(item.get("coverage_status"))] += 1
        total_seeds += int(item.get("seed_count") or 0)
        total_expanded += int(item.get("expanded_node_count") or 0)
    return {
        "ok": not held,
        "status": "hold" if held else "complete",
        "reason": "low-disk-space" if held else None,
        "new_pointer_maps": new_maps,
        "total_pointer_maps": len(completed),
        "new_seeds": new_seeds,
        "total_seeds": total_seeds,
        "new_expanded_nodes": new_expanded,
        "total_expanded_nodes": total_expanded,
        "hundreds_decomposed": totals["hundreds-decomposed"],
        "evidence_limited": totals["evidence-limited"],
        "insufficient_evidence": totals["insufficient-evidence"],
        "pointer_master_keys_seen": pointer_masters_seen,
        "new_pointer_master_kits": new_masters,
        "total_pointer_master_kits": len(existing_master_keys),
        "facet_registry_hash": stable_hash(facet_registry()),
    }


def _merge_master(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    shards = workspace / "shards"
    shard_bytes = int(controls["shard_max_bytes"])
    master_writer = ShardedJsonlWriter(shards, "master-kits", shard_bytes)
    evidence_writer = ShardedJsonlWriter(shards, "master-kit-evidence", shard_bytes)
    evidence_done = _load_values(shards.glob("master-kit-evidence-*.jsonl"), "observation_id")
    masters_done = _load_values(shards.glob("master-kits-*.jsonl"), "semantic_key")
    grouped: Dict[str, Dict[str, Any]] = {}
    new_evidence = 0
    observations_seen = 0
    held = False
    for observation in _read_jsonl(shards.glob("kit-observations-*.jsonl")):
        observations_seen += 1
        if observations_seen % 1000 == 0 and _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
            break
        key = observation.get("merge_key") or observation["semantic_key"]
        item = grouped.setdefault(key, {"sample": observation, "sources": set(), "domains": set(), "relations": set(), "count": 0})
        item["count"] += 1
        item["sources"].add(observation["source_context"]["source_id"])
        item["domains"].add(observation["domain"])
        item["relations"].add(observation["semantic_key"])
        if observation["observation_id"] not in evidence_done:
            evidence_writer.append({
                "schema_version": "kituniverse.master-kit-evidence.v1",
                "pipeline_epoch": controls.get("pipeline_epoch"),
                "observation_id": observation["observation_id"],
                "semantic_key": observation["semantic_key"],
                "master_key": key,
                "source_context": observation["source_context"],
            })
            evidence_done.add(observation["observation_id"])
            new_evidence += 1
    if held:
        return {
            "ok": False,
            "status": "hold",
            "reason": "low-disk-space",
            "observation_count": observations_seen,
            "new_evidence_links": new_evidence,
        }
    engine_root = Path(controls["engine_root"])
    protokits_root = Path(controls["protokits_root"])
    inventory = build_capability_inventory(engine_root, protokits_root)
    _write_json(workspace / "capability-inventory.json", inventory)
    new_masters = supported = missing = 0
    for key, item in sorted(grouped.items()):
        if key in masters_done:
            continue
        status = capability_status(key, inventory)
        master_writer.append({
            "schema_version": MASTER_KIT_SCHEMA,
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "master_kit_id": stable_hash([key, MASTER_KIT_SCHEMA]),
            "semantic_key": key,
            "canonical_observation": item["sample"],
            "domains": sorted(item["domains"]),
            "support_count": item["count"],
            "source_count": len(item["sources"]),
            "source_ids_sample": sorted(item["sources"])[:64],
            "interaction_keys_sample": sorted(item["relations"])[:128],
            "inventory_status": status,
            "lifecycle_status": "already-supported" if status["status"] == "already-supported" else "observed",
        })
        masters_done.add(key)
        new_masters += 1
        supported += status["status"] == "already-supported"
        missing += status["status"] == "missing"
    return {
        "ok": True,
        "status": "complete",
        "observation_count": sum(item["count"] for item in grouped.values()),
        "canonical_keys_seen": len(grouped),
        "new_master_kits": new_masters,
        "total_master_kits": len(masters_done),
        "new_evidence_links": new_evidence,
        "already_supported": supported,
        "missing": missing,
        "inventory_hash": inventory["inventory_hash"],
    }


async def _refine_master(
    node: Dict[str, Any], controls: Dict[str, Any], workspace: Path, max_kits_override: Optional[int]
) -> Dict[str, Any]:
    config = node.get("config") or {}
    model = str(config.get("model") or "lfm2.5-1.2b-instruct")
    evidence_strength_filter = str(config.get("evidence_strength") or "")
    semantic_contains = str(config.get("semantic_contains") or "")
    limit = max_kits_override if max_kits_override is not None else int(config.get("max_master_kits") or 0)
    limits = controls.get("model_prediction_limits") or {}
    router = SmartRoutingService(
        controls["base_url"], model, int(controls.get("timeout_seconds", 45)),
        max_predictions=int(limits[model]),
        max_context_tokens=min(int(config.get("context_tokens") or 2000), int(controls["max_context_tokens"])),
    )
    shards = workspace / "shards"
    completed = _load_values(shards.glob("master-refine-ledger-*.jsonl"), "identity")
    master_paths = [*shards.glob("master-kits-*.jsonl"), *shards.glob("pointer-master-kits-*.jsonl")]
    pending = [
        item for item in _read_jsonl(master_paths)
        if item["master_kit_id"] not in completed and item["inventory_status"]["status"] == "missing"
        and (not evidence_strength_filter or str(item.get("canonical_observation", {}).get("evidence_strength") or "direct") == evidence_strength_filter)
        and (not semantic_contains or semantic_contains in str(item.get("semantic_key") or ""))
    ]
    facet_priority = {
        name: index for index, name in enumerate((
            "target-resolution", "effect-application", "state-transition", "event-emission", "eligibility",
            "condition-evaluation", "request-intake", "schema-validation", "idempotency", "snapshot",
            "restore", "reset", "replay", "diagnostics", "audit", "test-fixture",
        ))
    }
    pending.sort(key=lambda item: (
        0 if item.get("canonical_observation", {}).get("evidence_strength") == "direct" else 1,
        facet_priority.get(str(item.get("canonical_observation", {}).get("source_context", {}).get("proposed_facet") or ""), 999),
        -int(item.get("source_count") or 0),
        item.get("semantic_key") or "",
    ))
    if limit:
        pending = pending[:limit]
    writer = ShardedJsonlWriter(shards, "refined-master-kits", int(controls["shard_max_bytes"]))
    ledger = ShardedJsonlWriter(shards, "master-refine-ledger", int(controls["shard_max_bytes"]))
    semaphore = asyncio.Semaphore(int(controls["task_concurrency"]))
    started = time.monotonic()

    async def one(master: Dict[str, Any]) -> Tuple[Dict[str, Any], Any, int]:
        async with semaphore:
            observation = master["canonical_observation"]
            strength = observation.get("evidence_strength") or "direct"
            source_context = observation.get("source_context") or {}
            pointer_proposal = observation.get("kind") == "atomic-pointer-materialization"
            evidence_rule = (
                "Decide whether the raw evidence directly entails both the proposed capability and this exact facet"
                if pointer_proposal
                else "Decide whether the exact action-to-target mechanic is directly entailed by the raw evidence"
            )
            evidence_packet = ({
                "evidence_strength": strength,
                "raw_evidence_field": source_context.get("evidence_field"),
                "raw_evidence_text": source_context.get("evidence_text"),
                "raw_relation": source_context.get("relation") or {},
                "proposed_capability": source_context.get("proposed_capability"),
                "proposed_facet": source_context.get("proposed_facet"),
                "facet_basis": source_context.get("facet_basis"),
                "required_facets": source_context.get("required_facets") or [],
                "proposed_domain": observation.get("domain"),
                "proposed_subdomain": observation.get("subdomain"),
                "support": master["source_count"],
            } if pointer_proposal else {
                "evidence_strength": "direct",
                "raw_evidence_field": source_context.get("evidence_field"),
                "raw_evidence_text": source_context.get("evidence_text"),
                "raw_relation": source_context.get("relation") or {},
                "proposed_capability": observation.get("subdomain"),
                "proposed_facet": "atomic-mechanic",
                "facet_basis": "mechanic-entailed",
                "proposed_domain": observation.get("domain"),
                "proposed_subdomain": observation.get("subdomain"),
                "support": master["source_count"],
            })
            prompt = (
                f"First {evidence_rule}. Default to false. Only raw_evidence_text and raw_relation are evidence; proposed names, support counts, generated contracts, and facet labels are not evidence. "
                "Facet basis is a rule: capability-root requires one valid direct atomic action/target relation and treats required_facets as internal contract obligations, not separate gameplay claims; adapter-root requires an explicit platform; domain-root may own only a generic boundary for an evidenced domain. Mechanic-entailed requires a valid direct relation; kit-quality-required may derive a reusable-kit invariant; explicit-evidence-required needs literal facet evidence. "
                "A genre, tag, platform, or broad domain word proves only that category, not authorization, networking, persistence, stacking, timing, UI, AI, or another facet unless the raw evidence explicitly supports it. "
                "Reject nearby words, noun phrases, passive descriptions, titles, idioms, narrative outcomes, and merely plausible engine design. "
                "Calibration: raw 'collect stars' plus relation collect/star and target-resolution is TRUE because the mechanic must resolve a star target. The same evidence plus network-command is FALSE. "
                "A proven collect/star mechanic plus snapshot with kit-quality-required is TRUE as a kit invariant, but does not claim the game visibly exposes snapshots. A bare Action genre plus authorization is FALSE. "
                "If false return only entailed=false and a specific reason of at most twelve words; never use the literal reason 'short'. If true, return keys entailed=true, reason, name, owns, does_not_own, inputs, outputs, idempotency, reset_snapshot, proof, domain, subdomain. "
                "Do not broaden beyond evidence.\n" + json.dumps(evidence_packet, ensure_ascii=False, separators=(",", ":"))
            )
            response, attempts = await router.chat(
                [{"role": "system", "content": "Return only one valid compact JSON object."}, {"role": "user", "content": prompt}],
                temperature=float(config.get("temperature", 0.2)),
                max_tokens=int(config.get("max_tokens") or 320),
                retries=int(config.get("retries") or 1),
            )
            return master, response, attempts

    accepted = failed = rejected_not_entailed = review_required = 0
    processed = 0
    held = False

    def persist(master: Dict[str, Any], response: Any, attempts: int) -> None:
        nonlocal accepted, failed, rejected_not_entailed, review_required, processed
        value = _parse_object(response.content)
        observation = master["canonical_observation"]
        entailed = value.get("entailed") is True
        explicitly_rejected = value.get("entailed") is False
        contract = _normalize_refined_contract(value, observation)
        ok = response.ok and entailed and _refined_contract_aligned(contract, observation)
        facet_basis = str((observation.get("source_context") or {}).get("facet_basis") or "")
        protected_basis = facet_basis in {
            "capability-root", "adapter-root", "domain-root", "mechanic-entailed", "kit-quality-required",
            "adapter-required", "domain-architecture-required"
        }
        status = (
            "refined" if ok else
            "review-required" if protected_basis else
            "rejected-not-entailed" if explicitly_rejected else
            "needs-repair"
        )
        source_context = {
            **observation["source_context"],
            "source_ids_sample": list(master.get("source_ids_sample") or [])[:64],
            "source_count": int(master.get("source_count") or 0),
            "provenance_query": master.get("provenance_query") or observation["source_context"].get("provenance_query"),
        }
        record = {
            "schema_version": REFINED_KIT_SCHEMA,
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "refined_kit_id": stable_hash([master["master_kit_id"], REFINED_KIT_SCHEMA]),
            "master_kit_id": master["master_kit_id"],
            "semantic_key": master["semantic_key"],
            "status": status,
            "entailment_reason": value.get("reason"),
            "contract": contract,
            "source_context": source_context,
            "support_count": master["source_count"],
            "model": model,
            "model_response": response.content,
            "attempts": attempts,
        }
        writer.append(record)
        ledger.append({"schema_version": "kituniverse.master-refine-ledger.v1", "pipeline_epoch": controls.get("pipeline_epoch"), "identity": master["master_kit_id"], "status": record["status"]})
        completed.add(master["master_kit_id"])
        accepted += ok
        failed += status == "needs-repair"
        rejected_not_entailed += status == "rejected-not-entailed"
        review_required += status == "review-required"
        processed += 1

    tasks: List[asyncio.Task[Any]] = []
    try:
        if _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
        else:
            tasks = [asyncio.create_task(one(master)) for master in pending]
            for task in asyncio.as_completed(tasks):
                master, response, attempts = await task
                persist(master, response, attempts)
                if processed % 8 == 0 and _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
                    held = True
                    for queued in tasks:
                        if not queued.done():
                            queued.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    break
    finally:
        stats = router.stats()
        router.shutdown()
    elapsed = max(0.001, time.monotonic() - started)
    return {
        "ok": not held,
        "status": "hold" if held else ("limit-complete" if limit else "complete"),
        "reason": "low-disk-space" if held else None,
        "new_refined": processed,
        "accepted": accepted,
        "needs_repair": failed,
        "rejected_not_entailed": rejected_not_entailed,
        "review_required": review_required,
        "total_completed": len(completed),
        "elapsed_seconds": round(elapsed, 3),
        "kits_per_minute": round(processed / elapsed * 60, 3),
        "router_stats": stats,
    }


def _parse_object(text: str) -> Dict[str, Any]:
    raw = str(text or "").replace("```json", "").replace("```", "").strip()
    start, end = raw.find("{"), raw.rfind("}")
    if start < 0 or end < start:
        return {}
    try:
        value = json.loads(raw[start : end + 1])
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _refined_contract_aligned(value: Dict[str, Any], observation: Dict[str, Any]) -> bool:
    action_target = {part for part in slug(observation["subdomain"]).split("-") if part}
    name_terms = set(slug(value.get("name")).replace("-kit", "").split("-"))
    owns_terms = set(slug(value.get("owns")).split("-"))
    inputs = value.get("inputs")
    outputs = value.get("outputs")
    return (
        action_target <= name_terms
        and action_target <= owns_terms
        and slug(value.get("domain")) == slug(observation["domain"])
        and isinstance(inputs, list) and all(str(item).strip() for item in inputs)
        and isinstance(outputs, list) and all(str(item).strip() for item in outputs)
    )


def _normalize_refined_contract(value: Dict[str, Any], observation: Dict[str, Any]) -> Dict[str, Any]:
    def text(key: str, fallback: str) -> str:
        candidate = value.get(key)
        return str(candidate).strip() if isinstance(candidate, str) and candidate.strip() else fallback

    def items(key: str, fallback: List[str]) -> List[str]:
        candidate = value.get(key)
        return [str(item).strip() for item in candidate if str(item).strip()] if isinstance(candidate, list) and candidate else list(fallback)

    return {
        "name": text("name", observation["kit_name"]),
        "owns": text("owns", observation["owns"]),
        "does_not_own": text("does_not_own", observation["does_not_own"]),
        "inputs": items("inputs", observation["inputs"]),
        "outputs": items("outputs", observation["outputs"]),
        "idempotency": text("idempotency", text("idempotency_rule", observation["idempotency_rule"])),
        "reset_snapshot": text("reset_snapshot", observation["reset_or_snapshot"]),
        "proof": text("proof", observation["first_proof"]),
        "domain": observation["domain"],
        "subdomain": observation["subdomain"],
    }


def _review_contract_valid(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    text_fields = ("name", "owns", "does_not_own", "idempotency", "reset_snapshot", "proof", "domain", "subdomain")
    if any(not isinstance(value.get(key), str) or not value[key].strip() for key in text_fields):
        return False
    if str(value.get("proof") or "").strip().lower() == "false":
        return False
    return is_architectural_domain(value.get("domain")) and is_architectural_domain(value.get("subdomain")) and all(
        isinstance(value.get(key), list) and value[key] and all(isinstance(item, str) and item.strip() for item in value[key])
        for key in ("inputs", "outputs")
    )


def _enqueue_builds(node: Dict[str, Any], controls: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    shards = workspace / "shards"
    completed = _load_values(shards.glob("exhaustive-build-requests-*.jsonl"), "source_id")
    accepted_decisions = {
        item["master_kit_id"]: item for item in _read_jsonl(shards.glob("master-codex-decisions-*.jsonl"))
        if item.get("accepted") is True
    }
    domain_mappings = load_active_domain_mappings(shards)
    master_index = _master_provenance_index(shards)
    wanted_source_ids = {
        source_id
        for master_id in accepted_decisions
        for source_id in list(master_index.get(str(master_id), {}).get("source_ids_sample") or [])[:8]
    }
    source_provenance = _source_provenance_index(shards, wanted_source_ids)
    writer = ShardedJsonlWriter(shards, "exhaustive-build-requests", int(controls["shard_max_bytes"]))
    added = repair = deferred_domain_review = 0
    for refined in _read_jsonl(shards.glob("refined-master-kits-*.jsonl")):
        if refined["status"] in {"rejected-not-entailed", "needs-repair"}:
            continue
        if refined["master_kit_id"] not in accepted_decisions:
            continue
        source_id = f"rawg-exhaustive:{refined['master_kit_id']}"
        if source_id in completed:
            continue
        reviewed_contract = accepted_decisions[refined["master_kit_id"]].get("contract")
        contract = reviewed_contract if _review_contract_valid(reviewed_contract) else refined["contract"]
        source_domain = slug(contract.get("domain"))
        domain_mapping = domain_mappings.get(source_domain)
        if not domain_mapping:
            deferred_domain_review += 1
            continue
        contract = apply_domain_mapping(contract, str(domain_mapping["target_domain"]), str(domain_mapping["reason"]))
        architecture_ready = is_architectural_domain(contract.get("domain")) and is_architectural_domain(
            contract.get("subdomain")
        )
        source_context = _enrich_source_context(
            refined.get("source_context") or {},
            master_index.get(str(refined["master_kit_id"]), {}),
            source_provenance,
        )
        provenance_ready = (
            source_context.get("source_id") not in {None, "", "pointer-query"}
            and bool(source_context.get("source_provenance_sample"))
            and bool(source_context.get("evidence_text"))
        )
        writer.append({
            "schema_version": BUILD_REQUEST_SCHEMA,
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "source_id": source_id,
            "title": contract["name"],
            "description": contract["owns"],
            "contract": contract,
            "domain_organization": contract.get("domain_organization"),
            "promotion_level": "build-required",
            "kit_role": contract.get("kit_role", "atomic"),
            "promotion_tier": contract.get("promotion_tier", "protokit-candidate"),
            "visibility": contract.get("visibility", "public"),
            "parent_kit_id": contract.get("parent_kit_id"),
            "child_kit_ids": contract.get("child_kit_ids", []),
            "child_idempotency_proof": contract.get("child_idempotency_proof"),
            "core_kits_reused": contract.get("core_kits_reused", []),
            "requires": contract.get("requires", []),
            "provides": contract.get("provides", []),
            "build_status": (
                "queued"
                if refined["status"] in {"refined", "review-required"} and architecture_ready and provenance_ready
                else "repair-required"
            ),
            "source_context": {
                **source_context,
                "master_kit_id": refined["master_kit_id"],
                "semantic_key": refined["semantic_key"],
                "support_count": refined["support_count"],
            },
        })
        completed.add(source_id)
        added += 1
        repair += refined["status"] not in {"refined", "review-required"} or not architecture_ready or not provenance_ready
    return {
        "ok": True,
        "status": "complete-with-domain-review-pending" if deferred_domain_review else "complete",
        "new_build_requests": added,
        "repair_required": repair,
        "deferred_domain_review": deferred_domain_review,
        "total_build_requests": len(completed),
    }


def _review_master_kits(
    node: Dict[str, Any], controls: Dict[str, Any], workspace: Path, max_batches_override: Optional[int]
) -> Dict[str, Any]:
    config = node.get("config") or {}
    batch_size = max(1, min(100, int(config.get("batch_size") or 25)))
    max_concurrency = max(1, min(16, int(config.get("max_concurrency") or 1)))
    model = str(config.get("model") or "gpt-5.6-luna")
    reasoning_effort = str(config.get("reasoning_effort") or "low")
    retries = max(0, min(2, int(config.get("retries") or 1)))
    batch_limit = max_batches_override if max_batches_override is not None else int(config.get("max_batches") or 0)
    shards = workspace / "shards"
    decided = _load_values(shards.glob("master-codex-decisions-*.jsonl"), "master_kit_id")
    pending = [
        item for item in _read_jsonl(shards.glob("refined-master-kits-*.jsonl"))
        if item["status"] in {"refined", "review-required"} and item["master_kit_id"] not in decided
    ]
    pending, queue_profile = _domain_diverse_review_order(pending, shards, workspace)
    batches = list(_chunks(pending, batch_size))
    if batch_limit:
        batches = batches[:batch_limit]
    master_index = _master_provenance_index(shards)
    selected = [item for batch in batches for item in batch]
    wanted_source_ids = {
        source_id
        for item in selected
        for source_id in list(master_index.get(str(item["master_kit_id"]), {}).get("source_ids_sample") or [])[:8]
    }
    source_provenance = _source_provenance_index(shards, wanted_source_ids)
    review_contexts = {
        str(item["master_kit_id"]): _enrich_source_context(
            item.get("source_context") or {},
            master_index.get(str(item["master_kit_id"]), {}),
            source_provenance,
        )
        for item in selected
    }
    writer = ShardedJsonlWriter(shards, "master-codex-decisions", int(controls["shard_max_bytes"]))
    accepted = rejected = 0
    reports: List[Dict[str, Any]] = []
    failed_reports: List[Dict[str, Any]] = []
    held = False
    review_root = workspace / "codex-master-review"
    stage_started = time.monotonic()
    jobs = [
        (
            f"batch-{str(controls.get('pipeline_epoch') or 'epoch')[:12]}-{stable_hash([item['master_kit_id'] for item in batch])[:20]}",
            batch,
        )
        for batch in batches
    ]
    for wave_index, wave in enumerate(_chunks(jobs, max_concurrency)):
        if _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
            break
        wave_started = time.monotonic()
        with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            future_jobs = {}
            for batch_id, batch in wave:
                packet = [{
                    "master_kit_id": item["master_kit_id"],
                    "semantic_key": item["semantic_key"],
                    "refinement_status": item["status"],
                    "entailment_reason": item.get("entailment_reason"),
                    "contract": item["contract"],
                    "source_context": review_contexts[str(item["master_kit_id"])],
                    "support_count": item["support_count"],
                } for item in batch]
                future = executor.submit(
                    run_master_review,
                    Path.cwd(),
                    review_root,
                    batch_id,
                    packet,
                    int(config.get("timeout_seconds") or 900),
                    model=model,
                    reasoning_effort=reasoning_effort,
                    retries=retries,
                )
                future_jobs[future] = (batch_id, len(batch))
            wave_reports: List[Dict[str, Any]] = []
            for future in as_completed(future_jobs):
                batch_id, candidate_count = future_jobs[future]
                try:
                    report = future.result()
                except Exception as exc:  # preserve the wave and retry this batch next run
                    report = {"ok": False, "batch_id": batch_id, "error": str(exc), "model": model, "reasoning_effort": reasoning_effort}
                report["candidate_count"] = candidate_count
                wave_reports.append(report)
        for report in sorted(wave_reports, key=lambda item: str(item.get("batch_id"))):
            reports.append(report)
            if not report.get("ok"):
                failed_reports.append(report)
                continue
            batch_id = str(report["batch_id"])
            for decision in report.get("decisions") or []:
                source_context = review_contexts.get(str(decision["master_kit_id"]), {})
                writer.append({
                    "schema_version": "kituniverse.master-codex-decision.v1",
                    "pipeline_epoch": controls.get("pipeline_epoch"),
                    "master_kit_id": decision["master_kit_id"],
                    "accepted": decision["accepted"],
                    "reasons": decision.get("reasons") or [],
                    "contract": decision.get("contract") if isinstance(decision.get("contract"), dict) else None,
                    "batch_id": batch_id,
                    "model": report.get("model"),
                    "reasoning_effort": report.get("reasoning_effort"),
                    "evidence_text": source_context.get("evidence_text"),
                    "source_ids_sample": source_context.get("source_ids_sample") or [],
                    "source_provenance_sample": source_context.get("source_provenance_sample") or [],
                    "provenance_query": source_context.get("provenance_query"),
                    "deterministic_warnings": source_context.get("deterministic_warnings") or [],
                })
                decided.add(decision["master_kit_id"])
                accepted += decision["accepted"] is True
                rejected += decision["accepted"] is not True
        wave_elapsed = max(0.001, time.monotonic() - wave_started)
        _write_json(review_root / "waves" / f"{controls.get('pipeline_epoch')}-{wave_index:06d}.json", {
            "schema_version": "kituniverse.codex-review-wave.v1",
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "wave_index": wave_index,
            "model": model,
            "reasoning_effort": reasoning_effort,
            "configured_concurrency": max_concurrency,
            "peak_active_lanes": peak_overlap(wave_reports),
            "batches": len(wave_reports),
            "valid_batches": sum(item.get("ok") is True for item in wave_reports),
            "failed_batches": sum(item.get("ok") is not True for item in wave_reports),
            "candidate_count": sum(int(item.get("candidate_count") or 0) for item in wave_reports),
            "elapsed_seconds": round(wave_elapsed, 3),
            "candidates_per_minute": round(sum(int(item.get("candidate_count") or 0) for item in wave_reports) / wave_elapsed * 60, 3),
            "scratch_cleaned": all(item.get("scratch_cleaned") is True for item in wave_reports),
            "reports": [
                {key: item.get(key) for key in ("batch_id", "ok", "candidate_count", "attempts", "elapsed_seconds", "error", "packet_path", "raw_output", "execution_report")}
                for item in wave_reports
            ],
        })
        if _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
            break
    stage_elapsed = max(0.001, time.monotonic() - stage_started)
    return {
        "ok": not held,
        "status": "hold" if held else ("complete-with-retries-needed" if failed_reports else ("limit-complete" if batch_limit else "complete")),
        "reason": "low-disk-space" if held else None,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "max_concurrency": max_concurrency,
        "peak_active_lanes": peak_overlap(reports),
        "attempted_batches": len(reports),
        "new_batches": sum(item.get("ok") is True for item in reports),
        "failed_batches": len(failed_reports),
        "new_decisions": accepted + rejected,
        "accepted": accepted,
        "rejected": rejected,
        "total_decided": len(decided),
        "elapsed_seconds": round(stage_elapsed, 3),
        "candidates_per_minute": round((accepted + rejected) / stage_elapsed * 60, 3),
        "queue_policy": "domain-diverse-round-robin-v1",
        "queue_profile": queue_profile,
        "reports": [
            {key: item.get(key) for key in ("batch_id", "elapsed_seconds", "ok", "attempts", "error", "packet_path", "raw_output", "execution_report")}
            for item in reports
        ],
    }


def _build_runtime_prove(
    node: Dict[str, Any], controls: Dict[str, Any], workspace: Path, max_builds_override: Optional[int]
) -> Dict[str, Any]:
    config = node.get("config") or {}
    authoring_required = bool(config.get("require_codex_authored", False))
    template_version = "codex-authored-runtime-v2-explicit-architecture" if authoring_required else "runtime-template-v4-explicit-architecture"
    limit = max_builds_override if max_builds_override is not None else int(config.get("max_builds") or 0)
    shards = workspace / "shards"
    prior_attempts = list(_read_jsonl(shards.glob("runtime-build-ledger-*.jsonl")))
    completed = {
        item.get("identity") for item in prior_attempts
        if item.get("identity") and item.get("status") == "runtime-proven"
    }
    completed_contracts = set()
    for item in prior_attempts:
        if item.get("status") != "runtime-proven" or not item.get("source_id"):
            continue
        descriptor_path = Path(str(item.get("descriptor_path") or ""))
        try:
            descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if descriptor.get("contract_hash"):
            completed_contracts.add((str(item["source_id"]), str(descriptor["contract_hash"])))
    identity_revisions: Dict[str, Dict[str, Any]] = {}
    for item in _read_jsonl(shards.glob("kit-identity-revisions-*.jsonl")):
        if item.get("source_id") and item.get("kit_identity_override"):
            identity_revisions[str(item["source_id"])] = item
    requests = []
    for stored_request in _read_jsonl(shards.glob("exhaustive-build-requests-*.jsonl")):
        item = dict(stored_request)
        revision = identity_revisions.get(str(item.get("source_id")))
        if revision:
            item["kit_identity_override"] = revision["kit_identity_override"]
        if (
            stable_hash([item["source_id"], template_version, item["contract"], descriptor_architecture(item)]) not in completed
            and (str(item["source_id"]), stable_hash(item["contract"])) not in completed_contracts
            and item["build_status"] == "queued"
        ):
            requests.append(item)
    if limit:
        requests = requests[:limit]
    simulator_cli = resolve_simulator_cli(controls.get("simulator_cli"))
    engine_root = Path(controls["engine_root"])
    ledger = ShardedJsonlWriter(shards, "runtime-build-ledger", int(controls["shard_max_bytes"]))
    events = ShardedJsonlWriter(shards, "master-kit-status-events", int(controls["shard_max_bytes"]))
    identity_writer = ShardedJsonlWriter(shards, "kit-identity-revisions", int(controls["shard_max_bytes"]))
    reserved_kit_ids = {path.name for path in (workspace / "kits").glob("*") if path.is_dir()}
    reserved_kit_ids.update(
        runtime_kit_shape({"contract": {"name": item["kit_identity_override"]}, "kit_identity_override": item["kit_identity_override"]})["kit_id"]
        for item in identity_revisions.values()
    )
    latest_author_attempts: Dict[str, Dict[str, Any]] = {}
    for item in _read_jsonl(shards.glob("codex-kit-author-ledger-*.jsonl")):
        if item.get("source_id"):
            latest_author_attempts[str(item["source_id"])] = item
    authored = {
        source_id: item
        for source_id, item in latest_author_attempts.items()
        if item.get("status") == "authored" and item.get("author_root")
    }
    passed = failed = missing_authored = admission_conflicts = 0
    held = False
    for request in requests:
        if _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
            break
        identity = stable_hash([request["source_id"], template_version, request["contract"], descriptor_architecture(request)])
        build_id = request["source_context"]["master_kit_id"]
        staging_root = workspace / ".kit-staging" / identity
        if authoring_required:
            author_record = authored.get(request["source_id"])
            if not author_record or not author_record.get("author_root"):
                missing_authored += 1
                continue
            author_root = Path(author_record["author_root"])
            try:
                authored_descriptor = json.loads((author_root / "kit.json").read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                missing_authored += 1
                continue
            expected_shape = runtime_kit_shape(request)
            expected_architecture = descriptor_architecture(request)
            if (
                authored_descriptor.get("kit_id") != expected_shape["kit_id"]
                or authored_descriptor.get("domain_path") != expected_architecture["domain_path"]
                or authored_descriptor.get("parent_domain_path") != expected_architecture["parent_domain_path"]
            ):
                missing_authored += 1
                continue
            if staging_root.exists():
                recovery_identity = stable_hash([
                    identity,
                    controls.get("pipeline_epoch"),
                    staging_root.stat().st_mtime_ns,
                ])[:16]
                recovery_root = workspace / "quarantine" / "staging-recovery" / f"{identity}-{recovery_identity}"
                recovery_root.parent.mkdir(parents=True, exist_ok=True)
                if recovery_root.exists():
                    return {
                        "ok": False,
                        "status": "hold",
                        "reason": "staging-kit-recovery-conflict",
                        "staging_root": str(staging_root),
                        "recovery_root": str(recovery_root),
                    }
                os.replace(staging_root, recovery_root)
            shutil.copytree(author_root, staging_root)
            descriptor = json.loads((staging_root / "kit.json").read_text(encoding="utf-8"))
            if descriptor.get("contract_hash") != stable_hash(request["contract"]):
                return {"ok": False, "status": "hold", "reason": "authored-contract-hash-mismatch"}
            built = {
                "kit_id": descriptor["kit_id"],
                "manifest_path": staging_root / "runtime-proof-manifest.json",
                "descriptor_path": staging_root / "kit.json",
            }
        else:
            built = build_runtime_package(request, staging_root, engine_root)
        proof_path = staging_root / "runtime-proof.json"
        proof = run_runtime_proof(
            simulator_cli, built["manifest_path"], proof_path, f"rawg-exhaustive-{build_id[:12]}",
            timeout_seconds=int(config.get("timeout_seconds") or 300),
        )
        status = "runtime-proven" if proof.get("ok") else "runtime-failed"
        proof_errors = list(proof.get("errors") or [])
        package_root = workspace / "kits" / built["kit_id"]
        if proof.get("ok") is True:
            package_root.parent.mkdir(parents=True, exist_ok=True)
            if package_root.exists():
                override = _derive_collision_identity(request, reserved_kit_ids)
                conflict_report_path = workspace / "admission-conflicts" / f"{identity}.json"
                try:
                    incumbent = json.loads((package_root / "kit.json").read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    incumbent = {"kit_id": built["kit_id"], "error": "unreadable-incumbent-descriptor"}
                _write_json(conflict_report_path, {
                    "schema_version": "kituniverse.kit-admission-conflict.v1",
                    "pipeline_epoch": controls.get("pipeline_epoch"),
                    "source_id": request["source_id"],
                    "master_kit_id": build_id,
                    "colliding_kit_id": built["kit_id"],
                    "kit_identity_override": override,
                    "incumbent": incumbent,
                    "challenger_contract": request["contract"],
                    "challenger_proof": str(proof_path),
                })
                identity_writer.append({
                    "schema_version": "kituniverse.kit-identity-revision.v1",
                    "pipeline_epoch": controls.get("pipeline_epoch"),
                    "source_id": request["source_id"],
                    "master_kit_id": build_id,
                    "colliding_kit_id": built["kit_id"],
                    "kit_identity_override": override,
                    "reason": "distinct-contract-package-id-collision",
                    "conflict_report": str(conflict_report_path),
                })
                reserved_kit_ids.add(runtime_kit_shape({**request, "kit_identity_override": override})["kit_id"])
                quarantine_root = workspace / "quarantine" / "admission-conflicts" / identity
                quarantine_root.parent.mkdir(parents=True, exist_ok=True)
                if quarantine_root.exists():
                    quarantine_root = quarantine_root.with_name(
                        f"{identity}-{stable_hash([controls.get('pipeline_epoch'), build_id])[:16]}"
                    )
                if quarantine_root.exists():
                    return {
                        "ok": False,
                        "status": "hold",
                        "reason": "admission-conflict-quarantine-conflict",
                        "quarantine_root": str(quarantine_root),
                    }
                os.replace(staging_root, quarantine_root)
                package_root = quarantine_root
                status = "admission-conflict"
                proof_errors.append("kit-package-conflict")
                admission_conflicts += 1
            else:
                os.replace(staging_root, package_root)
        else:
            quarantine_root = workspace / "quarantine" / "kits" / identity
            quarantine_root.parent.mkdir(parents=True, exist_ok=True)
            if quarantine_root.exists():
                author_identity = authored.get(request["source_id"], {}).get("author_identity")
                suffix = stable_hash([
                    identity,
                    controls.get("pipeline_epoch"),
                    author_identity,
                    len(prior_attempts) + passed + failed,
                ])[:16]
                quarantine_root = workspace / "quarantine" / "kits" / f"{identity}-{suffix}"
            if quarantine_root.exists():
                return {
                    "ok": False,
                    "status": "hold",
                    "reason": "quarantine-kit-conflict",
                    "quarantine_root": str(quarantine_root),
                }
            os.replace(staging_root, quarantine_root)
            package_root = quarantine_root
        manifest_path = package_root / "runtime-proof-manifest.json"
        final_proof_path = package_root / "runtime-proof.json"
        descriptor_path = package_root / "kit.json"
        ledger.append({
            "schema_version": "kituniverse.runtime-build-ledger.v1",
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "identity": identity,
            "source_id": request["source_id"],
            "master_kit_id": build_id,
            "kit_id": built["kit_id"],
            "status": status,
            "package_root": str(package_root),
            "manifest_path": str(manifest_path),
            "descriptor_path": str(descriptor_path),
            "proof_path": str(final_proof_path),
            "proof_errors": proof_errors,
            "template_version": template_version,
            "kit_identity_override": request.get("kit_identity_override"),
        })
        events.append({
            "schema_version": "kituniverse.master-kit-status-event.v1",
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "master_kit_id": build_id,
            "from": "refined",
            "to": status,
            "kit_id": built["kit_id"],
            "proof_path": str(final_proof_path),
        })
        if status == "runtime-proven":
            completed.add(identity)
        passed += status == "runtime-proven"
        failed += status == "runtime-failed"
    return {
        "ok": failed == 0 and missing_authored == 0 and admission_conflicts == 0 and not held,
        "status": "hold" if held else ("complete-with-unbuilt" if failed or missing_authored or admission_conflicts else ("limit-complete" if limit else "complete")),
        "reason": "low-disk-space" if held else None,
        "new_builds": passed + failed,
        "runtime_proven": passed,
        "runtime_failed": failed,
        "admission_conflicts": admission_conflicts,
        "missing_authored": missing_authored,
        "total_attempted_builds": len(prior_attempts) + len(requests),
    }


def _derive_collision_identity(request: Dict[str, Any], reserved_kit_ids: set[str]) -> str:
    shape = runtime_kit_shape(request)
    base = shape["runtime_domain"]
    base_terms = set(base.split("-"))
    generic_terms = {
        "accepted", "rejected", "state", "result", "complete", "completed", "applied", "updated", "event"
    }
    candidates: List[str] = []
    for output in request.get("contract", {}).get("outputs") or []:
        distinct = [
            term for term in slug(output).split("-")
            if term and term not in base_terms and term not in generic_terms
        ]
        if distinct:
            candidates.append(slug(f"{base}-{'-'.join(distinct)}"))
    subdomain = slug(request.get("contract", {}).get("subdomain"))
    if subdomain and subdomain != base:
        candidates.append(slug(f"{base}-{subdomain}"))
    master_id = str(request.get("source_context", {}).get("master_kit_id") or "")
    candidates.append(slug(f"{base}-{master_id[:8]}"))
    for candidate in candidates:
        if not candidate or candidate == base:
            continue
        kit_id = runtime_kit_shape({**request, "kit_identity_override": candidate})["kit_id"]
        if kit_id not in reserved_kit_ids:
            return candidate
    return slug(f"{base}-{stable_hash([request.get('source_id'), request.get('contract')])[:12]}")


def _reconcile_run(workspace: Path, controls: Dict[str, Any], reports: Dict[str, Any]) -> Dict[str, Any]:
    cleanup = cleanup_scratch(workspace)
    execution_paths = sorted((workspace / "codex-master-review" / "executions").glob("*.json"))
    execution_paths.extend(sorted((workspace / "codex-domain-review" / "executions").glob("*.json")))
    execution_paths.extend(sorted((workspace / "codex-kit-author" / "executions").glob("*.json")))
    executions = []
    unreadable = []
    for path in execution_paths:
        try:
            executions.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as exc:
            unreadable.append({"path": str(path), "error": str(exc)})
    stage_holds = [
        {"node_id": node_id, "reason": report.get("reason"), "status": report.get("status")}
        for node_id, report in reports.items()
        if report.get("status") == "hold" or report.get("ok") is False
    ]
    report = {
        "schema_version": "rawg.exhaustive-run-reconciliation.v1",
        "pipeline_epoch": controls.get("pipeline_epoch"),
        "ok": cleanup.get("ok") is True and not unreadable,
        "status": "complete" if cleanup.get("ok") is True and not unreadable else "hold",
        "reason": None if cleanup.get("ok") is True and not unreadable else "run-reconciliation-failed",
        "stage_holds": stage_holds,
        "codex_execution_reports": len(executions),
        "codex_successful_executions": sum(item.get("ok") is True for item in executions),
        "codex_failed_executions": sum(item.get("ok") is not True for item in executions),
        "unreadable_execution_reports": unreadable,
        "scratch_cleanup": cleanup,
        "permanent_artifacts_preserved": [
            "codex-master-review/packets",
            "codex-master-review/responses",
            "codex-master-review/executions",
            "codex-master-review/waves",
            "codex-domain-review/packets",
            "codex-domain-review/responses",
            "codex-domain-review/executions",
            "codex-domain-review/reports",
            "codex-kit-author/packets",
            "codex-kit-author/responses",
            "codex-kit-author/executions",
            "codex-kit-author/waves",
        ],
    }
    epoch = str(controls.get("pipeline_epoch") or "unknown")
    _write_json(workspace / "run-reconciliation" / f"{epoch}.json", report)
    return report


def _report(ast: Dict[str, Any], workspace: Path, reports: Dict[str, Any]) -> Dict[str, Any]:
    shards = list((workspace / "shards").glob("*.jsonl"))
    cache_path = workspace / "shard-validation-cache.json"
    try:
        cache = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        cache = {"schema_version": "rawg.shard-validation-cache.v1", "files": {}}
    cached_files = cache.get("files") if isinstance(cache.get("files"), dict) else {}
    trusted_cutoff = 0
    prior_report_path = workspace / "report.json"
    try:
        prior_report = json.loads(prior_report_path.read_text(encoding="utf-8"))
        if prior_report.get("malformed_jsonl") == 0 and int(prior_report.get("shards") or 0) == len(shards):
            trusted_cutoff = prior_report_path.stat().st_mtime_ns
    except (OSError, ValueError, json.JSONDecodeError):
        pass
    malformed = 0
    scanned = reused = trusted_from_prior_report = 0
    next_cache: Dict[str, Dict[str, int]] = {}
    for path in sorted(shards):
        stat = path.stat()
        key = path.name
        cached = cached_files.get(key) if isinstance(cached_files, dict) else None
        if (
            isinstance(cached, dict)
            and int(cached.get("size") or -1) == stat.st_size
            and int(cached.get("mtime_ns") or -1) == stat.st_mtime_ns
        ):
            file_malformed = int(cached.get("malformed") or 0)
            reused += 1
        elif trusted_cutoff and stat.st_mtime_ns <= trusted_cutoff:
            file_malformed = 0
            trusted_from_prior_report += 1
        else:
            file_malformed = 0
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    try:
                        json.loads(line)
                    except json.JSONDecodeError:
                        file_malformed += 1
            scanned += 1
        malformed += file_malformed
        next_cache[key] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "malformed": file_malformed}
    _write_json(cache_path, {"schema_version": "rawg.shard-validation-cache.v1", "files": next_cache})
    return {
        "schema_version": "rawg.exhaustive-report.v1",
        "ok": malformed == 0 and all(report.get("ok") for report in reports.values()),
        "workflow_id": ast["workflow_id"],
        "stages": reports,
        "shards": len(shards),
        "max_shard_bytes": max((path.stat().st_size for path in shards), default=0),
        "malformed_jsonl": malformed,
        "shard_validation": {
            "scanned": scanned,
            "reused": reused,
            "trusted_from_prior_full_report": trusted_from_prior_report,
        },
    }


def _read_jsonl(paths: Iterable[Path]) -> Iterator[Dict[str, Any]]:
    for path in sorted(paths):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def _load_values(paths: Iterable[Path], key: str) -> set[str]:
    return {str(item[key]) for item in _read_jsonl(paths) if item.get(key) is not None}


def _master_provenance_index(shards: Path) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    paths = [*shards.glob("master-kits-*.jsonl"), *shards.glob("pointer-master-kits-*.jsonl")]
    for item in _read_jsonl(paths):
        master_id = str(item.get("master_kit_id") or "")
        if not master_id:
            continue
        context = (item.get("canonical_observation") or {}).get("source_context") or {}
        source_ids = [str(value) for value in item.get("source_ids_sample") or [] if str(value)]
        if not source_ids and context.get("source_id") not in {None, "", "pointer-query"}:
            source_ids = [str(context["source_id"])]
        index[master_id] = {
            "source_ids_sample": source_ids[:64],
            "source_count": int(item.get("source_count") or len(source_ids)),
            "provenance_query": item.get("provenance_query") or context.get("provenance_query"),
        }
    return index


def _source_provenance_index(shards: Path, wanted_source_ids: set[str]) -> Dict[str, Dict[str, Any]]:
    if not wanted_source_ids:
        return {}
    remaining = set(wanted_source_ids)
    result: Dict[str, Dict[str, Any]] = {}
    for item in _read_jsonl(shards.glob("game-evidence-maps-*.jsonl")):
        source_id = str(item.get("source_id") or "")
        if source_id not in remaining:
            continue
        result[source_id] = {
            "dataset": item.get("dataset"),
            "source_file": item.get("source_file"),
            "source_line": item.get("source_line"),
            "source_id": source_id,
            "source_hash": item.get("source_hash"),
            "source_url": item.get("source_url"),
            "name": item.get("name"),
        }
        remaining.remove(source_id)
        if not remaining:
            break
    return result


def _enrich_source_context(
    context: Dict[str, Any], master: Dict[str, Any], source_provenance: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    source_ids = [str(value) for value in master.get("source_ids_sample") or context.get("source_ids_sample") or [] if str(value)][:8]
    current_source_id = str(context.get("source_id") or "")
    canonical_source_id = current_source_id if current_source_id in source_provenance else (source_ids[0] if source_ids else current_source_id)
    canonical = source_provenance.get(canonical_source_id, {})
    sample = [source_provenance[source_id] for source_id in source_ids if source_id in source_provenance]
    enriched = {
        **context,
        "dataset": canonical.get("dataset") or context.get("dataset"),
        "source_file": canonical.get("source_file") or context.get("source_file"),
        "source_line": canonical.get("source_line") or context.get("source_line"),
        "source_id": canonical_source_id,
        "source_hash": canonical.get("source_hash") or context.get("source_hash"),
        "source_url": canonical.get("source_url") or context.get("source_url"),
        "source_ids_sample": source_ids,
        "source_count": int(master.get("source_count") or context.get("source_count") or len(source_ids)),
        "source_provenance_sample": sample,
        "provenance_query": master.get("provenance_query") or context.get("provenance_query"),
    }
    warnings = []
    evidence_text = str(enriched.get("evidence_text") or "")
    relation = enriched.get("relation") or {}
    narrative_goal = re.search(
        r"\b(must|tries? to|attempts? to|determined to|quest(?: is)? to|goal(?: is)? to|needs? to)\b",
        evidence_text,
        flags=re.IGNORECASE,
    )
    operational = re.search(
        r"\b(players? can|you can|allows? (?:the player|you) to|press|activate|use|equip|collect|build|craft)\b",
        evidence_text,
        flags=re.IGNORECASE,
    )
    if narrative_goal and not operational and relation.get("action"):
        warnings.append("narrative-goal-language")
    if not sample or canonical_source_id in {"", "pointer-query"}:
        warnings.append("missing-exact-source-provenance")
    enriched["deterministic_warnings"] = warnings
    return enriched


def _chunks(items: Sequence[Dict[str, Any]], size: int) -> Iterator[List[Dict[str, Any]]]:
    for index in range(0, len(items), size):
        yield list(items[index : index + size])


def _domain_diverse_review_order(
    pending: List[Dict[str, Any]], shards: Path, workspace: Path
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Reorder the complete queue without sampling or dropping candidates."""
    mappings = load_active_domain_mappings(shards)
    validated: set[str] = set()
    architecture_path = workspace / "domain-architecture-report.json"
    try:
        architecture = json.loads(architecture_path.read_text(encoding="utf-8"))
        if architecture.get("ok") is True:
            validated = {slug(value) for value in (architecture.get("domain_counts") or {}) if slug(value)}
    except (OSError, json.JSONDecodeError):
        pass

    catch_all = {"general", "generic", "mechanics", "misc", "miscellaneous", "other", "unknown", "unclassified"}
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    group_priority: Dict[str, int] = {}
    for item in pending:
        contract = item.get("contract") or {}
        source_domain = slug(contract.get("domain")) or "unclassified"
        mapping = mappings.get(source_domain) or {}
        target_domain = slug(mapping.get("target_domain")) or source_domain
        if source_domain in catch_all or target_domain in catch_all:
            group_key = f"catch-all:{source_domain}"
            priority = 1
        else:
            group_key = target_domain
            priority = 2 if target_domain in validated else 0
        groups[group_key].append(item)
        group_priority[group_key] = priority

    keys = sorted(groups, key=lambda key: (group_priority[key], len(groups[key]), key))
    offsets = {key: 0 for key in keys}
    ordered: List[Dict[str, Any]] = []
    while len(ordered) < len(pending):
        progressed = False
        for key in keys:
            offset = offsets[key]
            if offset >= len(groups[key]):
                continue
            ordered.append(groups[key][offset])
            offsets[key] = offset + 1
            progressed = True
        if not progressed:
            break
    if len(ordered) != len(pending):
        raise ValueError("domain-diverse-review-order-lost-candidates")
    profile_counts = Counter(
        "unseen" if group_priority[key] == 0 else "catch-all" if group_priority[key] == 1 else "validated"
        for key in keys
    )
    return ordered, {
        "pending_candidates": len(pending),
        "domain_groups": len(groups),
        "validated_domains": len(validated),
        "unseen_groups": profile_counts["unseen"],
        "catch_all_groups": profile_counts["catch-all"],
        "validated_groups": profile_counts["validated"],
    }


def _free_gib(path: Path) -> float:
    return shutil.disk_usage(path).free / (1024 ** 3)


def _git_state(root: Path) -> Dict[str, Any]:
    def run(*args: str) -> bytes:
        process = subprocess.run(
            ["git", *args], cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
        )
        return process.stdout if process.returncode == 0 else b""

    commit = run("rev-parse", "HEAD").decode("utf-8", errors="replace").strip() or None
    status = run("status", "--porcelain=v1", "--untracked-files=all")
    difference = run("diff", "--binary", "HEAD", "--", ".")
    digest = hashlib.sha256()
    digest.update(commit.encode("utf-8") if commit else b"no-commit")
    digest.update(b"\0status\0" + status)
    digest.update(b"\0diff\0" + difference)
    for line in status.decode("utf-8", errors="surrogateescape").splitlines():
        if not line.startswith("?? "):
            continue
        path = root / line[3:]
        if path.is_file():
            digest.update(b"\0untracked\0" + line[3:].encode("utf-8", errors="surrogateescape"))
            try:
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
            except OSError:
                digest.update(b"unreadable")
    return {
        "git_commit": commit,
        "dirty_tree": bool(status),
        "dirty_tree_hash": digest.hexdigest(),
    }


def _write_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)
