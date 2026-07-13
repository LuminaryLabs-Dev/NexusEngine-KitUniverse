from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import time
from collections import Counter, defaultdict
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
from .contracts import (
    BUILD_REQUEST_SCHEMA,
    MASTER_KIT_SCHEMA,
    REFINED_KIT_SCHEMA,
    validate_game_map,
    validate_interaction,
    validate_kit_observation,
)
from .decomposition import aggregate_pointer_masters, build_pointer_map, compact_pointer_map, facet_registry
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
from .implementation import build_runtime_package
from .master_review import run_master_review


TOTAL_RAWG_RECORDS = 881_069


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--ast", type=Path, required=True)
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--max-model-pages", type=int)
    parser.add_argument("--max-master-kits", type=int)
    parser.add_argument("--max-builds", type=int)
    parser.add_argument("--max-codex-batches", type=int)
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
    for node in ordered_nodes(ast):
        op = node["op"]
        if op == "rawg.map-evidence":
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
        elif op == "kit.enqueue-builds":
            report = _enqueue_builds(node, controls, workspace)
        elif op == "kit.build-runtime-prove":
            report = _build_runtime_prove(node, controls, workspace, args.max_builds)
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
    words = [slug(value) for value in str(unit.get("text") or "").split()]
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
        record = {
            "schema_version": REFINED_KIT_SCHEMA,
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "refined_kit_id": stable_hash([master["master_kit_id"], REFINED_KIT_SCHEMA]),
            "master_kit_id": master["master_kit_id"],
            "semantic_key": master["semantic_key"],
            "status": status,
            "entailment_reason": value.get("reason"),
            "contract": contract,
            "source_context": observation["source_context"],
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
    return all(
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
    writer = ShardedJsonlWriter(shards, "exhaustive-build-requests", int(controls["shard_max_bytes"]))
    added = repair = 0
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
        writer.append({
            "schema_version": BUILD_REQUEST_SCHEMA,
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "source_id": source_id,
            "title": contract["name"],
            "description": contract["owns"],
            "contract": contract,
            "promotion_level": "build-required",
            "build_status": "queued" if refined["status"] in {"refined", "review-required"} else "repair-required",
            "source_context": {
                **refined["source_context"],
                "master_kit_id": refined["master_kit_id"],
                "semantic_key": refined["semantic_key"],
                "support_count": refined["support_count"],
            },
        })
        completed.add(source_id)
        added += 1
        repair += refined["status"] not in {"refined", "review-required"}
    return {"ok": True, "status": "complete", "new_build_requests": added, "repair_required": repair, "total_build_requests": len(completed)}


def _review_master_kits(
    node: Dict[str, Any], controls: Dict[str, Any], workspace: Path, max_batches_override: Optional[int]
) -> Dict[str, Any]:
    config = node.get("config") or {}
    batch_size = max(1, min(100, int(config.get("batch_size") or 25)))
    batch_limit = max_batches_override if max_batches_override is not None else int(config.get("max_batches") or 0)
    shards = workspace / "shards"
    decided = _load_values(shards.glob("master-codex-decisions-*.jsonl"), "master_kit_id")
    pending = [
        item for item in _read_jsonl(shards.glob("refined-master-kits-*.jsonl"))
        if item["status"] in {"refined", "review-required"} and item["master_kit_id"] not in decided
    ]
    batches = list(_chunks(pending, batch_size))
    if batch_limit:
        batches = batches[:batch_limit]
    writer = ShardedJsonlWriter(shards, "master-codex-decisions", int(controls["shard_max_bytes"]))
    accepted = rejected = 0
    reports = []
    held = False
    for index, batch in enumerate(batches):
        if _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
            break
        packet = [{
            "master_kit_id": item["master_kit_id"],
            "semantic_key": item["semantic_key"],
            "refinement_status": item["status"],
            "entailment_reason": item.get("entailment_reason"),
            "contract": item["contract"],
            "source_context": item["source_context"],
            "support_count": item["support_count"],
        } for item in batch]
        batch_id = f"batch-{len(decided) // batch_size + index:06d}"
        report = run_master_review(Path.cwd(), workspace / "codex-master-review", batch_id, packet, int(config.get("timeout_seconds") or 900))
        reports.append(report)
        if not report.get("ok"):
            return {"ok": False, "status": "hold", "reason": "codex-master-review-failed", "report": report}
        for decision in report.get("decisions") or []:
            writer.append({
                "schema_version": "kituniverse.master-codex-decision.v1",
                "pipeline_epoch": controls.get("pipeline_epoch"),
                "master_kit_id": decision["master_kit_id"],
                "accepted": decision["accepted"],
                "reasons": decision.get("reasons") or [],
                "contract": decision.get("contract") if isinstance(decision.get("contract"), dict) else None,
                "batch_id": batch_id,
                "model": report.get("model"),
            })
            decided.add(decision["master_kit_id"])
            accepted += decision["accepted"] is True
            rejected += decision["accepted"] is not True
    return {
        "ok": not held,
        "status": "hold" if held else ("limit-complete" if batch_limit else "complete"),
        "reason": "low-disk-space" if held else None,
        "new_batches": len(batches),
        "new_decisions": accepted + rejected,
        "accepted": accepted,
        "rejected": rejected,
        "total_decided": len(decided),
        "reports": [{"batch_id": item.get("batch_id"), "elapsed_seconds": item.get("elapsed_seconds"), "ok": item.get("ok")} for item in reports],
    }


def _build_runtime_prove(
    node: Dict[str, Any], controls: Dict[str, Any], workspace: Path, max_builds_override: Optional[int]
) -> Dict[str, Any]:
    config = node.get("config") or {}
    template_version = "runtime-template-v2"
    limit = max_builds_override if max_builds_override is not None else int(config.get("max_builds") or 0)
    shards = workspace / "shards"
    prior_attempts = list(_read_jsonl(shards.glob("runtime-build-ledger-*.jsonl")))
    completed = {
        item.get("identity") for item in prior_attempts
        if item.get("identity") and item.get("status") == "runtime-proven"
    }
    requests = [
        item for item in _read_jsonl(shards.glob("exhaustive-build-requests-*.jsonl"))
        if stable_hash([item["source_id"], template_version]) not in completed and item["build_status"] == "queued"
    ]
    if limit:
        requests = requests[:limit]
    simulator_cli = resolve_simulator_cli(controls.get("simulator_cli"))
    engine_root = Path(controls["engine_root"])
    ledger = ShardedJsonlWriter(shards, "runtime-build-ledger", int(controls["shard_max_bytes"]))
    events = ShardedJsonlWriter(shards, "master-kit-status-events", int(controls["shard_max_bytes"]))
    passed = failed = 0
    held = False
    for request in requests:
        if _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
            break
        identity = stable_hash([request["source_id"], template_version])
        build_id = request["source_context"]["master_kit_id"]
        package_root = workspace / "built-kits" / build_id
        built = build_runtime_package(request, package_root, engine_root)
        proof_path = package_root / "runtime-proof.json"
        proof = run_runtime_proof(
            simulator_cli, built["manifest_path"], proof_path, f"rawg-exhaustive-{build_id[:12]}",
            timeout_seconds=int(config.get("timeout_seconds") or 300),
        )
        status = "runtime-proven" if proof.get("ok") else "runtime-failed"
        ledger.append({
            "schema_version": "kituniverse.runtime-build-ledger.v1",
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "identity": identity,
            "source_id": request["source_id"],
            "master_kit_id": build_id,
            "kit_id": built["kit_id"],
            "status": status,
            "package_root": str(package_root),
            "manifest_path": str(built["manifest_path"]),
            "proof_path": str(proof_path),
            "proof_errors": proof.get("errors") or [],
            "template_version": template_version,
        })
        events.append({
            "schema_version": "kituniverse.master-kit-status-event.v1",
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "master_kit_id": build_id,
            "from": "refined",
            "to": status,
            "kit_id": built["kit_id"],
            "proof_path": str(proof_path),
        })
        if proof.get("ok") is True:
            completed.add(identity)
        passed += proof.get("ok") is True
        failed += proof.get("ok") is not True
    return {
        "ok": failed == 0 and not held,
        "status": "hold" if held else ("limit-complete" if limit else "complete"),
        "reason": "low-disk-space" if held else None,
        "new_builds": passed + failed,
        "runtime_proven": passed,
        "runtime_failed": failed,
        "total_attempted_builds": len(prior_attempts) + len(requests),
    }


def _report(ast: Dict[str, Any], workspace: Path, reports: Dict[str, Any]) -> Dict[str, Any]:
    shards = list((workspace / "shards").glob("*.jsonl"))
    malformed = 0
    for path in shards:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
    return {
        "schema_version": "rawg.exhaustive-report.v1",
        "ok": malformed == 0 and all(report.get("ok") for report in reports.values()),
        "workflow_id": ast["workflow_id"],
        "stages": reports,
        "shards": len(shards),
        "max_shard_bytes": max((path.stat().st_size for path in shards), default=0),
        "malformed_jsonl": malformed,
    }


def _read_jsonl(paths: Iterable[Path]) -> Iterator[Dict[str, Any]]:
    for path in sorted(paths):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def _load_values(paths: Iterable[Path], key: str) -> set[str]:
    return {str(item[key]) for item in _read_jsonl(paths) if item.get(key) is not None}


def _chunks(items: Sequence[Dict[str, Any]], size: int) -> Iterator[List[Dict[str, Any]]]:
    for index in range(0, len(items), size):
        yield list(items[index : index + size])


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
