from __future__ import annotations

import argparse
import asyncio
import json
import re
import subprocess
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from kituniverse_harness.smart_router import SmartRoutingService
from workflow_harnesses.guided_kit_builder.codex_cli_review import CODEX_BINARY, CODEX_MODEL
from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash
from workflow_harnesses.rawg_capability_pipeline.source_adapter import stream_rawg_records
from workflow_harnesses.rawg_chainstorm.workflow_rawg_chainstorm import (
    DEFAULT_SOURCE_ROOT,
    EVIDENCE_STOPWORDS,
    GENERIC_LABELS,
    GENERIC_TOKENS,
    _mechanical_seed_unit,
)
from workflow_harnesses.rawg_domain_loop.evidence_chain import select_evidence_units


DEFAULT_BASE_URL = "http://10.0.0.137:1234/v1"
MODEL_12B = "lfm2.5-1.2b-instruct"
MODEL_350M = "lfm2.5-350m"
DEFAULT_RUN_ROOT = Path("runs/workflow-harnesses/rawg-matrix-optimizer")
DEFAULT_FEEDBACK_WORKSPACE = Path("runs/rawg-881k/matrix-production")
FOCUS_CELLS = [
    "state resources limits",
    "choice relationships consequences",
    "movement world interaction",
    "combat abilities status",
    "progression rewards lifecycle",
    "multiplayer party session",
]
MATRIX_BANNED_TOKENS = {
    "audio", "core", "hub", "input", "mechanical", "mode", "module", "music", "prompt", "scene",
    "realm", "sound", "story", "subdomain", "technology", "world", "zone",
}
MATRIX_IGNORED_TAGS = {
    "controller", "controller-support", "full-controller-support", "partial-controller-support",
    "singleplayer", "steam-achievements", "steam-cloud", "steam-trading-cards",
}
MATRIX_MECHANIC_TAG_MARKERS = {
    "automation", "base-building", "character-customization", "choices-matter", "city-builder",
    "co-op", "combat", "crafting", "deckbuilding", "destruction", "dialogue", "diplomacy",
    "economy", "exploration", "fishing", "hacking", "inventory-management", "local-multiplayer",
    "morality", "multiple-endings", "online-multiplayer", "parkour", "permadeath", "physics",
    "procedural-generation", "puzzle", "pvp", "resource-management", "split-screen", "stealth",
    "survival", "time-manipulation", "tower-defense", "turn-based",
}
MATRIX_ACTION_MARKERS = {
    "attack", "battle", "block", "build", "buy", "cast", "choose", "climb", "collect", "craft",
    "customize", "destroy", "dodge", "equip", "explore", "fight", "fish", "grow", "hack", "jump",
    "manage", "move", "play", "progress", "solve", "spend", "trade", "upgrade", "unlock", "use", "win",
}
PROFILES = [
    {"profile_id": "12b-seed-12b-walk", "seed_model": MODEL_12B, "walk_model": MODEL_12B, "seed_temperature": 1.2, "walk_temperature": 1.0},
    {"profile_id": "12b-seed-350m-walk", "seed_model": MODEL_12B, "walk_model": MODEL_350M, "seed_temperature": 1.2, "walk_temperature": 0.9},
    {"profile_id": "350m-seed-12b-walk", "seed_model": MODEL_350M, "walk_model": MODEL_12B, "seed_temperature": 1.2, "walk_temperature": 0.9},
    {"profile_id": "350m-seed-350m-walk", "seed_model": MODEL_350M, "walk_model": MODEL_350M, "seed_temperature": 1.2, "walk_temperature": 0.9},
    {"profile_id": "12b-seed-12b-walk-grounded", "seed_model": MODEL_12B, "walk_model": MODEL_12B, "seed_temperature": 1.2, "walk_temperature": 0.8},
    {"profile_id": "12b-seed-12b-walk-lean", "seed_model": MODEL_12B, "walk_model": MODEL_12B, "seed_temperature": 1.2, "walk_temperature": 0.7},
    {"profile_id": "12b-seed-12b-walk-balanced-075", "seed_model": MODEL_12B, "walk_model": MODEL_12B, "seed_temperature": 1.2, "walk_temperature": 0.75, "beam_width": 3},
    {"profile_id": "12b-seed-12b-walk-lean-beam2", "seed_model": MODEL_12B, "walk_model": MODEL_12B, "seed_temperature": 1.2, "walk_temperature": 0.7, "beam_width": 2},
    {"profile_id": "12b-seed-12b-walk-lean-beam2-d2", "seed_model": MODEL_12B, "walk_model": MODEL_12B, "seed_temperature": 1.2, "walk_temperature": 0.7, "beam_width": 2, "max_depth": 2},
    {"profile_id": "12b-seed-12b-walk-balanced-075-beam2-d3", "seed_model": MODEL_12B, "walk_model": MODEL_12B, "seed_temperature": 1.2, "walk_temperature": 0.75, "beam_width": 2, "max_depth": 3},
    {"profile_id": "12b-seed-12b-walk-lean-beam3-d2", "seed_model": MODEL_12B, "walk_model": MODEL_12B, "seed_temperature": 1.2, "walk_temperature": 0.7, "beam_width": 3, "max_depth": 2},
    {"profile_id": "350m-seed-12b-walk-lean-beam2-d2", "seed_model": MODEL_350M, "walk_model": MODEL_12B, "seed_temperature": 1.2, "walk_temperature": 0.7, "beam_width": 2, "max_depth": 2},
    {"profile_id": "350m-seed-12b-walk-balanced-beam3-d2", "seed_model": MODEL_350M, "walk_model": MODEL_12B, "seed_temperature": 1.2, "walk_temperature": 0.8, "beam_width": 3, "max_depth": 2},
    {"profile_id": "12b-seed-12b-walk-grounded-beam3-d2", "seed_model": MODEL_12B, "walk_model": MODEL_12B, "seed_temperature": 1.1, "walk_temperature": 0.7, "beam_width": 3, "max_depth": 2},
    {"profile_id": "350m-seed-12b-walk-grounded-beam3-d2", "seed_model": MODEL_350M, "walk_model": MODEL_12B, "seed_temperature": 1.1, "walk_temperature": 0.7, "beam_width": 3, "max_depth": 2},
]


class ShardedJsonlWriter:
    def __init__(self, root: Path, prefix: str, max_bytes: int) -> None:
        self.root = root
        self.prefix = prefix
        self.max_bytes = max_bytes
        self.root.mkdir(parents=True, exist_ok=True)
        existing = sorted(self.root.glob(f"{prefix}-*.jsonl"))
        self.index = int(existing[-1].stem.rsplit("-", 1)[-1]) if existing else 0
        self.path = self._path()
        self.size = self.path.stat().st_size if self.path.exists() else 0
        self.paths = [str(path) for path in existing]

    def _path(self) -> Path:
        return self.root / f"{self.prefix}-{self.index:06d}.jsonl"

    def append(self, value: Dict[str, Any]) -> Path:
        encoded = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        if len(encoded) > self.max_bytes:
            raise ValueError("one JSONL record exceeds shard byte limit")
        if self.size and self.size + len(encoded) > self.max_bytes:
            self.index += 1
            self.path = self._path()
            self.size = 0
        with self.path.open("ab") as handle:
            handle.write(encoded)
            handle.flush()
        self.size += len(encoded)
        if str(self.path) not in self.paths:
            self.paths.append(str(self.path))
        return self.path


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--feedback-workspace", type=Path, default=DEFAULT_FEEDBACK_WORKSPACE)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--sample-games", type=int, default=4)
    parser.add_argument("--max-depth", type=int, default=2)
    parser.add_argument("--beam-width", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--parallel-per-model", type=int, default=8)
    parser.add_argument("--context-per-slot", type=int, default=2000)
    parser.add_argument("--max-tokens", type=int, default=96)
    parser.add_argument("--timeout-seconds", type=int, default=45)
    parser.add_argument("--shard-max-mb", type=int, default=90)
    parser.add_argument("--profiles", help="comma-separated profile ids")
    parser.add_argument("--skip-codex", action="store_true")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-rawg-matrix-optimizer",
        description="Benchmark local-model seed/walk matrices and select a corpus profile.",
    )
    configure_parser(parser)
    report = asyncio.run(run_optimizer(parser.parse_args(argv)))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


async def run_optimizer(args: argparse.Namespace) -> Dict[str, Any]:
    if args.sample_games < 1:
        raise ValueError("--sample-games must be positive")
    if not 2 <= args.max_depth <= 4:
        raise ValueError("--max-depth must be between 2 and 4")
    if not 2 <= args.beam_width <= 6:
        raise ValueError("--beam-width must be between 2 and 6")
    if not 1 <= args.concurrency <= 16:
        raise ValueError("--concurrency must be between 1 and 16")
    if not 1 <= args.parallel_per_model <= 8:
        raise ValueError("--parallel-per-model must be between 1 and 8")
    if args.context_per_slot > 2000:
        raise ValueError("--context-per-slot may not exceed 2000")
    if args.shard_max_mb > 90:
        raise ValueError("--shard-max-mb may not exceed 90")

    selected_ids = set((args.profiles or "").split(",")) - {""}
    profiles = [value for value in PROFILES if not selected_ids or value["profile_id"] in selected_ids]
    if not profiles:
        raise ValueError("no profiles selected")
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
    run_dir = args.run_root / run_id
    shards_dir = run_dir / "shards"
    run_dir.mkdir(parents=True, exist_ok=True)
    max_shard_bytes = args.shard_max_mb * 1_000_000
    writer = ShardedJsonlWriter(shards_dir, "matrix-records", max_shard_bytes)
    ledger_writer = ShardedJsonlWriter(shards_dir, "completion-ledger", max_shard_bytes)
    matrix_feedback = _load_matrix_feedback(args.feedback_workspace)
    _write_json(run_dir / "matrix-feedback.json", matrix_feedback)

    records = []
    for source, _, _ in stream_rawg_records(args.source_root, "rawg-matrix-optimizer"):
        if source.get("has_mechanic_evidence") and not source.get("error"):
            records.append(source)
        if len(records) >= args.sample_games:
            break
    if len(records) != args.sample_games:
        raise RuntimeError(f"found only {len(records)} usable records")

    required_models = sorted({profile[key] for profile in profiles for key in ("seed_model", "walk_model")})
    routers = {
        model: SmartRoutingService(
            args.base_url, model, args.timeout_seconds,
            max_predictions=args.parallel_per_model,
            max_context_tokens=min(args.context_per_slot, 700),
        )
        for model in required_models
    }
    preflight = _provider_preflight(args, required_models)
    _write_json(run_dir / "provider-preflight.json", preflight)
    manifest = {
        "schema_version": "rawg.matrix-optimizer-manifest.v1",
        "run_id": run_id,
        "controls": {
            "sample_games": args.sample_games,
            "max_depth": args.max_depth,
            "beam_width": args.beam_width,
            "concurrency": args.concurrency,
            "parallel_per_model": args.parallel_per_model,
            "total_prediction_ceiling": args.parallel_per_model * len(required_models),
            "context_per_slot": args.context_per_slot,
            "loaded_context_expected": args.context_per_slot * args.parallel_per_model,
            "shard_max_bytes": max_shard_bytes,
        },
        "profiles": profiles,
        "source_ids": [record["source_id"] for record in records],
        "feedback_workspace": str(args.feedback_workspace),
        "stages": ["mechanical-seed", "high-temperature-ideation", "recursive-subdomain-walk", "filter", "profile-score", "codex-matrix-review"],
    }
    _write_json(run_dir / "manifest.json", manifest)
    if not preflight.get("ok"):
        report = {"ok": False, "status": "hold", "reason": "provider-preflight-failed", "run_dir": str(run_dir), "preflight": preflight}
        _write_json(run_dir / "report.json", report)
        for router in routers.values():
            router.shutdown()
        return report

    profile_reports = []
    profile_results: Dict[str, List[Dict[str, Any]]] = {}
    samples_for_codex: Dict[str, List[Dict[str, Any]]] = {}
    semaphore = asyncio.Semaphore(args.concurrency)
    try:
        for profile in profiles:
            started = time.monotonic()

            async def run_one(source: Dict[str, Any]) -> Dict[str, Any]:
                async with semaphore:
                    return await _run_game(source, profile, routers, args)

            results = await asyncio.gather(*(run_one(source) for source in records))
            wall = max(0.001, time.monotonic() - started)
            for result in results:
                writer.append(result)
                ledger_writer.append(
                    {
                        "identity": stable_hash([result["source_hash"], profile["profile_id"]]),
                        "source_id": result["source_id"],
                        "source_hash": result["source_hash"],
                        "profile_id": profile["profile_id"],
                        "status": "complete" if result["ok"] else "failed",
                    }
                )
            profile_report = _score_profile(profile, results, wall)
            profile_reports.append(profile_report)
            profile_results[profile["profile_id"]] = results
            samples_for_codex[profile["profile_id"]] = [
                {"source_id": result["source_id"], "nodes": result["accepted_nodes"][:8], "rejections": result["rejections"][:5]}
                for result in results[: min(3, len(results))]
            ]
    finally:
        for router in routers.values():
            router.shutdown()

    cross_frequency = Counter()
    profile_keys: Dict[str, set[str]] = {}
    for profile_id, results in profile_results.items():
        keys = {node["semantic_key"] for result in results for node in result["accepted_nodes"]}
        profile_keys[profile_id] = keys
        cross_frequency.update(keys)
    for value in profile_reports:
        keys = profile_keys[value["profile_id"]]
        exclusive = sum(cross_frequency[key] == 1 for key in keys)
        ratio = exclusive / max(len(keys), 1)
        value["cross_profile_unique_nodes"] = exclusive
        value["cross_profile_novelty_ratio"] = round(ratio, 4)
        value["score"] = round(value["score"] * (0.75 + 0.25 * ratio), 4)
    ranked = sorted(profile_reports, key=lambda value: (-value["score"], -value["games_per_minute"], value["profile_id"]))
    benchmark = {
        "schema_version": "rawg.matrix-benchmark.v1",
        "profiles": profile_reports,
        "ranking": [item["profile_id"] for item in ranked],
        "deterministic_winner": ranked[0]["profile_id"],
        "samples": samples_for_codex,
    }
    _write_json(run_dir / "benchmark.json", benchmark)
    codex = {"ok": True, "skipped": True, "recommended_profile": ranked[0]["profile_id"], "reason": "skip-codex"}
    if not args.skip_codex:
        codex = _run_codex_matrix_review(run_dir, benchmark)
    _write_json(run_dir / "codex-matrix-review.json", codex)
    winner = codex.get("recommended_profile") if codex.get("recommended_profile") in {item["profile_id"] for item in profiles} else ranked[0]["profile_id"]
    winner_report = next(item for item in profile_reports if item["profile_id"] == winner)
    games_per_minute = winner_report["games_per_minute"]
    eta_days = 881069 / max(games_per_minute, 0.0001) / 60 / 24
    shard_paths = sorted(shards_dir.glob("*.jsonl"))
    shard_sizes = {str(path): path.stat().st_size for path in shard_paths}
    report = {
        "ok": all(item["completed_games"] == args.sample_games for item in profile_reports),
        "status": "complete",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "profiles_tested": len(profiles),
        "games_per_profile": args.sample_games,
        "winner": winner,
        "winner_score": winner_report["score"],
        "games_per_minute": games_per_minute,
        "full_corpus_eta_days": round(eta_days, 3),
        "codex_matrix_review": codex,
        "shard_max_bytes": max_shard_bytes,
        "shard_sizes": shard_sizes,
        "max_observed_shard_bytes": max(shard_sizes.values(), default=0),
        "malformed_jsonl": _count_malformed(shard_paths),
        "profile_reports": profile_reports,
        "router_stats": {model: router.stats() for model, router in routers.items()},
    }
    _write_json(run_dir / "report.json", report)
    _write_report_md(run_dir / "report.md", report)
    return report


def _provider_preflight(args: argparse.Namespace, models: List[str]) -> Dict[str, Any]:
    from kituniverse_harness.providers import LMStudioProvider

    expected_context = args.context_per_slot * args.parallel_per_model
    values = {}
    for model in models:
        provider = LMStudioProvider(args.base_url, model, args.timeout_seconds)
        health = provider.health()
        load = provider.ensure_loaded(expected_context, args.parallel_per_model)
        values[model] = {"health": health, "load": load}
    return {
        "ok": all(value["health"].get("ok") and value["load"].get("ok") and value["load"].get("config_matches", True) for value in values.values()),
        "context_per_slot": args.context_per_slot,
        "expected_loaded_context": expected_context,
        "parallel_per_model": args.parallel_per_model,
        "models": values,
    }


async def _run_game(
    source: Dict[str, Any], profile: Dict[str, Any], routers: Dict[str, SmartRoutingService], args: argparse.Namespace
) -> Dict[str, Any]:
    units = _matrix_evidence_units(source)[:8]
    seed = _seed_text(source, units)
    beam_width = int(profile.get("beam_width") or args.beam_width)
    max_depth = int(profile.get("max_depth") or args.max_depth)
    seen = set()
    accepted_nodes: List[Dict[str, Any]] = []
    rejections: List[Dict[str, Any]] = []
    calls = []
    active = [{"node_id": "root", "label": seed, "level": 0, "lineage_evidence_source_ids": [item["source_id"] for item in units]}]
    started = time.monotonic()
    for depth in range(1, max_depth + 1):
        model = profile["seed_model"] if depth == 1 else profile["walk_model"]
        temperature = profile["seed_temperature"] if depth == 1 else profile["walk_temperature"]
        kept: List[Dict[str, Any]] = []
        if depth == 1:
            prompt = _seed_prompt(source, active, beam_width, units)
            response, attempts = await routers[model].chat(
                [{"role": "system", "content": "Return short list items only; no reasoning or prose."}, {"role": "user", "content": prompt}],
                temperature=temperature, max_tokens=args.max_tokens, retries=0,
            )
            proposals = _parse_output(response.content, active, beam_width)
            kept, rejected = _filter_nodes(proposals, seen, source, units, depth, active)
            calls.append({
                "depth": depth, "model": model, "temperature": temperature, "prompt": prompt,
                "response": response.to_dict(), "attempts": attempts, "proposals": proposals,
                "accepted_node_ids": [item["node_id"] for item in kept],
            })
            accepted_nodes.extend(kept)
            rejections.extend(rejected)
        else:
            async def walk_parent(parent: Dict[str, Any], call_temperature: float) -> Tuple[Dict[str, Any], str, Any, int]:
                prompt = _walk_prompt(source, [parent], units)
                response, attempts = await routers[model].chat(
                    [{"role": "system", "content": "Return one short grounded subdomain only; no reasoning."}, {"role": "user", "content": prompt}],
                    temperature=call_temperature, max_tokens=args.max_tokens, retries=0,
                )
                return parent, prompt, response, attempts

            first_pass = await asyncio.gather(*(walk_parent(parent, temperature) for parent in active))
            missing: List[Dict[str, Any]] = []
            for parent, prompt, response, attempts in first_pass:
                proposals = _parse_output(response.content, [parent], 3)
                child_kept, rejected = _filter_nodes(proposals, seen, source, units, depth, [parent])
                kept.extend(child_kept)
                accepted_nodes.extend(child_kept)
                rejections.extend(rejected)
                calls.append({
                    "depth": depth, "parent_id": parent["node_id"], "model": model,
                    "temperature": temperature, "prompt": prompt, "response": response.to_dict(),
                    "attempts": attempts, "proposals": proposals,
                    "accepted_node_ids": [item["node_id"] for item in child_kept],
                })
                if not child_kept:
                    missing.append(parent)
            if missing:
                retry_temperature = min(1.2, temperature + 0.2)
                retries = await asyncio.gather(*(walk_parent(parent, retry_temperature) for parent in missing))
                for parent, prompt, response, attempts in retries:
                    proposals = _parse_output(response.content, [parent], 3)
                    child_kept, rejected = _filter_nodes(proposals, seen, source, units, depth, [parent])
                    kept.extend(child_kept)
                    accepted_nodes.extend(child_kept)
                    rejections.extend(rejected)
                    calls.append({
                        "depth": depth, "retry_parent_id": parent["node_id"], "model": model,
                        "temperature": retry_temperature, "prompt": prompt, "response": response.to_dict(),
                        "attempts": attempts, "proposals": proposals,
                        "accepted_node_ids": [item["node_id"] for item in child_kept],
                    })
        active = kept[-beam_width:]
        if not active:
            break
    parent_ids = {item["node_id"] for item in accepted_nodes if 1 <= item["level"] < max_depth}
    expanded_parent_ids = {item["parent_id"] for item in accepted_nodes if 2 <= item["level"] <= max_depth}
    subdomain_coverage = len(parent_ids & expanded_parent_ids) / max(len(parent_ids), 1)
    has_subdomain = bool(parent_ids) and subdomain_coverage == 1.0 and any(item["level"] == max_depth for item in accepted_nodes)
    has_direct_evidence = any(item["direct_evidence_source_ids"] for item in accepted_nodes)
    return {
        "schema_version": "rawg.matrix-game-result.v1",
        "source_id": source["source_id"],
        "source_hash": source["source_hash"],
        "source_file": source.get("source_file"),
        "source_line": source.get("source_line"),
        "profile_id": profile["profile_id"],
        "profile": profile,
        "ok": bool(accepted_nodes) and has_subdomain and has_direct_evidence,
        "has_subdomain": has_subdomain,
        "has_direct_evidence": has_direct_evidence,
        "subdomain_parent_coverage": round(subdomain_coverage, 4),
        "max_depth": max_depth,
        "calls": calls,
        "accepted_nodes": accepted_nodes,
        "rejections": rejections,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def _seed_text(source: Dict[str, Any], units: List[Dict[str, str]]) -> str:
    parts = [source.get("name") or "", *((source.get("genres") or [])[:3]), *((source.get("tags") or [])[:8])]
    parts.extend(str(item.get("text") or "")[:90] for item in units[:4])
    return " | ".join(str(value) for value in parts if value)[:600]


def _seed_prompt(source: Dict[str, Any], active: List[Dict[str, Any]], beam: int, units: List[Dict[str, str]]) -> str:
    matrix = "; ".join(FOCUS_CELLS)
    anchors = _evidence_anchor_terms(units)
    return (
        f"GAME EVIDENCE: {active[0]['label']}\nIDEA MATRIX: {matrix}\n"
        f"CONCRETE EVIDENCE WORDS: {', '.join(anchors)}\n"
        f"Generate {beam} diverse reusable game-behavior domain seeds. Explore adjacent behaviors while remaining mechanically grounded. Every seed MUST contain at least one exact concrete evidence word from the list.\n"
        "RETURN: 2 to 6 word seed per line; no numbering, genres, story, technology, or explanation."
    )


def _walk_prompt(source: Dict[str, Any], active: List[Dict[str, Any]], units: List[Dict[str, str]]) -> str:
    if len(active) == 1:
        parent = active[0]
        parent_words = [value for value in slug(parent["label"]).split("-") if value not in GENERIC_TOKENS and value not in EVIDENCE_STOPWORDS]
        return (
            f"PARENT DOMAIN: {parent['label']}\n"
            f"REQUIRED PARENT WORDS: {', '.join(parent_words)}\n"
            f"EVIDENCE WORDS: {', '.join(_evidence_anchor_terms(units))}\n"
            "Invent three NEW narrower reusable subdomain options. Each keeps at least one exact required parent word and adds at least one new evidence-backed behavior word.\n"
            "RETURN: exactly three lines, each a 2 to 6 word subdomain. No id, numbering, or prose."
        )
    parents = "\n".join(f"{item['node_id']} | {item['label']}" for item in active)
    return (
        f"GAME: {source.get('name')}\nINPUT LINES:\n{parents}\n"
        "For EACH input line, invent one NEW narrower reusable subdomain. It owns a smaller rule, state, lifecycle, relationship, input, or output. Never repeat the parent.\n"
        "Every child MUST retain at least one meaningful parent word and add at least one new meaningful word.\n"
        f"RETURN EXACTLY {len(active)} LINES: parent_id | 2 to 6 word subdomain. No prose."
    )


def _parse_output(content: str, parents: List[Dict[str, Any]], count_per_parent: int) -> List[Dict[str, str]]:
    parent_ids = [item["node_id"] for item in parents]
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if len(lines) == 1 and "," in lines[0]:
        lines = [value.strip() for value in lines[0].split(",") if value.strip()]
    limit = max(1, len(parents) * count_per_parent)
    output = []
    for index, line in enumerate(lines[:limit]):
        cleaned = re.sub(r"^\s*(?:[-*]|\d+[.)])\s*", "", line).strip().strip("`\"")
        if "|" in cleaned:
            possible, label = [value.strip() for value in cleaned.split("|", 1)]
            parent_id = possible if possible in parent_ids else parent_ids[min(index, len(parent_ids) - 1)]
        else:
            parent_id = parent_ids[min(index, len(parent_ids) - 1)]
            label = cleaned
        if "→" in label:
            label = label.rsplit("→", 1)[-1].strip()
        label = re.sub(r"^(?:parent\s+)?[rl]\d{1,2}[-_ ]\d{1,2}(?:[-_ ]+)", "", label, flags=re.IGNORECASE)
        label = re.sub(r"([a-z])([A-Z])", r"\1 \2", label).replace("_", " ")
        label = re.sub(r"^seed[-_ ]+(?:\d+|[a-z])[-_ :]*", "", label, flags=re.IGNORECASE)
        output.append({"parent_id": parent_id, "label": " ".join(label.split()).strip(" .,:;|-")})
    return output


def _filter_nodes(
    proposals: List[Dict[str, str]], seen: set[str], source: Dict[str, Any], units: List[Dict[str, str]],
    level: int, parents: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    parent_map = {item["node_id"]: item for item in parents}
    kept, rejected = [], []
    for index, proposal in enumerate(proposals, start=1):
        label = proposal["label"]
        key = slug(label)
        tokens = [value for value in key.split("-") if value]
        parent = parent_map.get(proposal["parent_id"], {})
        parent_key = slug(parent.get("label"))
        parent_tokens = {value for value in parent_key.split("-") if value not in GENERIC_TOKENS and value not in EVIDENCE_STOPWORDS}
        node_tokens = {value for value in tokens if value not in GENERIC_TOKENS and value not in EVIDENCE_STOPWORDS}
        if level > 1 and len(tokens) >= 2 and parent_tokens and not (parent_tokens & node_tokens):
            anchor = sorted(parent_tokens)[0]
            label = f"{anchor} {label}"
            key = slug(label)
            tokens = [value for value in key.split("-") if value]
            node_tokens = {value for value in tokens if value not in GENERIC_TOKENS and value not in EVIDENCE_STOPWORDS}
        reason = None
        if not key or key in GENERIC_LABELS:
            reason = "empty-or-generic"
        elif re.fullmatch(r"(?:seed|item|domain|subdomain)-?\d+", key) or re.fullmatch(r"[a-z]{3,16}-\d{2,}", key):
            reason = "generated-id-artifact"
        elif not 2 <= len(tokens) <= 6:
            reason = "label-shape"
        elif any(any(character.isdigit() for character in value) for value in tokens):
            reason = "numeric-or-instruction-artifact"
        elif any(value in GENERIC_TOKENS for value in tokens):
            reason = "generic-token"
        elif any(_stem_token(value) in MATRIX_BANNED_TOKENS for value in tokens):
            reason = "presentation-or-story-token"
        elif set(tokens) & {value for value in slug(source.get("name")).split("-") if len(value) >= 4}:
            reason = "branded-source-token"
        elif key in seen:
            reason = "exact-duplicate"
        if not reason and level > 1 and (key == parent_key or set(tokens) == set(parent_key.split("-"))):
            reason = "renamed-parent"
        elif not reason and level > 1 and not (parent_tokens & node_tokens):
            reason = "disconnected-child"
        elif not reason and level > 1 and not (node_tokens - parent_tokens):
            reason = "no-new-subdomain-term"
        elif not reason and level > 1 and not _terms_have_evidence(node_tokens - parent_tokens, units):
            reason = "no-new-evidence-term"
        direct_ids = _direct_evidence(label, units)
        if not reason and not direct_ids:
            reason = "no-direct-evidence"
        node = {
            "node_id": f"l{level:02d}-{index:02d}-{key or 'invalid'}",
            "parent_id": proposal["parent_id"],
            "level": level,
            "kind": "domain" if level == 1 else "subdomain",
            "label": label,
            "semantic_key": key,
            "direct_evidence_source_ids": direct_ids,
            "lineage_evidence_source_ids": list(parent.get("lineage_evidence_source_ids") or parent.get("direct_evidence_source_ids") or []),
            "source_id": source["source_id"],
            "source_hash": source["source_hash"],
        }
        if reason:
            rejected.append({**node, "reason": reason})
        else:
            seen.add(key)
            kept.append(node)
    return kept, rejected


def _direct_evidence(label: str, units: List[Dict[str, str]]) -> List[str]:
    label_tokens = {_stem_token(value) for value in slug(label).split("-") if len(value) >= 4 and value not in EVIDENCE_STOPWORDS and value not in GENERIC_TOKENS}
    if not label_tokens:
        return []
    output = []
    for unit in units:
        unit_tokens = {_stem_token(value) for value in slug(unit.get("text")).split("-") if len(value) >= 4 and value not in EVIDENCE_STOPWORDS}
        overlap = label_tokens & unit_tokens
        required = 1 if len(label_tokens) == 1 else 2
        if len(overlap) >= required:
            output.append(unit["source_id"])
    return output


def _terms_have_evidence(terms: set[str], units: List[Dict[str, str]]) -> bool:
    if not terms:
        return False
    stemmed_terms = {_stem_token(value) for value in terms}
    for unit in units:
        unit_tokens = {_stem_token(value) for value in slug(unit.get("text")).split("-") if len(value) >= 4 and value not in EVIDENCE_STOPWORDS}
        if stemmed_terms & unit_tokens:
            return True
    return False


def _matrix_evidence_units(source: Dict[str, Any]) -> List[Dict[str, str]]:
    """Keep action-bearing prose and literal mechanic tags; discard platform/mode metadata."""
    output = []
    seen_text = set()
    for unit in select_evidence_units(source):
        if not _mechanical_seed_unit(unit):
            continue
        if unit.get("field") != "tags":
            output.append(unit)
            seen_text.add(slug(unit.get("text")))
            continue
        key = slug(unit.get("text"))
        if key in MATRIX_IGNORED_TAGS or key.startswith("steam-"):
            continue
        if any(marker == key or marker in key for marker in MATRIX_MECHANIC_TAG_MARKERS):
            output.append(unit)
            seen_text.add(key)
    description = " ".join(str(source.get("description") or "").split())
    for sentence_index, sentence in enumerate(re.split(r"(?<=[.!?])\s+", description), start=1):
        key = slug(sentence)
        stems = {_stem_token(value) for value in key.split("-") if value}
        if not key or key in seen_text or not (stems & MATRIX_ACTION_MARKERS):
            continue
        output.append({"source_id": f"MD{sentence_index:03d}", "field": "description", "text": sentence})
        seen_text.add(key)
    return output


def _stem_token(value: str) -> str:
    if value.endswith("ies") and len(value) > 4:
        return value[:-3] + "y"
    if value.endswith("ing") and len(value) > 5:
        base = value[:-3]
        return base[:-1] if len(base) > 2 and base[-1:] == base[-2:-1] else base
    if value.endswith("ed") and len(value) > 4:
        return value[:-2]
    if value.endswith("s") and not value.endswith("ss") and len(value) > 4:
        return value[:-1]
    return value


def _evidence_anchor_terms(units: List[Dict[str, str]]) -> List[str]:
    output = []
    for unit in units:
        for value in slug(unit.get("text")).split("-"):
            if len(value) < 4 or value in EVIDENCE_STOPWORDS or value in GENERIC_TOKENS or value in output:
                continue
            output.append(value)
            if len(output) >= 24:
                return output
    return output


def _score_profile(profile: Dict[str, Any], results: List[Dict[str, Any]], wall: float) -> Dict[str, Any]:
    nodes = [node for result in results for node in result["accepted_nodes"]]
    keys = [node["semantic_key"] for node in nodes]
    unique = len(set(keys))
    direct = sum(bool(node["direct_evidence_source_ids"]) for node in nodes)
    successful = sum(result["ok"] for result in results)
    grounded_games = sum(result["has_direct_evidence"] for result in results)
    parent_coverage = sum(result["subdomain_parent_coverage"] for result in results) / max(len(results), 1)
    calls = sum(len(result["calls"]) for result in results)
    games_per_minute = len(results) / wall * 60
    unique_ratio = unique / max(len(nodes), 1)
    direct_ratio = direct / max(len(nodes), 1)
    subdomain_rate = successful / max(len(results), 1)
    grounded_game_rate = grounded_games / max(len(results), 1)
    quality = 0.30 * subdomain_rate + 0.20 * grounded_game_rate + 0.20 * parent_coverage + 0.20 * unique_ratio + 0.10 * direct_ratio
    score = games_per_minute * quality / max(calls / max(len(results), 1), 1)
    return {
        **profile,
        "completed_games": len(results),
        "successful_subdomain_games": successful,
        "grounded_games": grounded_games,
        "grounded_game_rate": round(grounded_game_rate, 4),
        "subdomain_rate": round(subdomain_rate, 4),
        "subdomain_parent_coverage": round(parent_coverage, 4),
        "accepted_nodes": len(nodes),
        "unique_nodes": unique,
        "unique_ratio": round(unique_ratio, 4),
        "direct_evidence_nodes": direct,
        "direct_evidence_ratio": round(direct_ratio, 4),
        "calls": calls,
        "calls_per_game": round(calls / max(len(results), 1), 3),
        "wall_seconds": round(wall, 3),
        "games_per_minute": round(games_per_minute, 3),
        "score": round(score, 4),
        "top_labels": Counter(keys).most_common(12),
        "rejection_reasons": dict(Counter(item["reason"] for result in results for item in result["rejections"])),
    }


def _run_codex_matrix_review(run_dir: Path, benchmark: Dict[str, Any]) -> Dict[str, Any]:
    output = run_dir / "codex-matrix-review.raw.txt"
    profile_ids = [item["profile_id"] for item in benchmark["profiles"]]
    prompt = f"""
Act as the agentic matrix director for the RAWG-to-KitUniverse discovery pipeline.

READ:
- {run_dir / 'manifest.json'}
- {run_dir / 'benchmark.json'}
- {run_dir / 'matrix-feedback.json'}
- workflow_harnesses/rawg_matrix_optimizer/
- workflow_harnesses/rawg_chainstorm/STORAGE.md
- goal.md and memory.md

Select the fastest reliable profile that still produces genuinely new subdomains. Penalize generic drift, low direct-evidence rate, duplicates, missing subdomain levels, and excess calls. Use the accumulated rejection, repair, and systemic-error feedback to change matrix rules rather than repeating known failure patterns. Codex owns matrix strategy; local models own only idea generation. Suggest at most two concrete next profiles by changing seed model, walk model, temperatures, beam, or depth. Do not edit files.

Return only:
{{"ok":true,"recommended_profile":"one exact profile id","reasons":[],"next_profiles":[{{"profile_id":"new-id","seed_model":"exact model","walk_model":"exact model","seed_temperature":1.2,"walk_temperature":0.8,"beam_width":3,"max_depth":2}}],"matrix_rules":[]}}

Valid current profile ids: {json.dumps(profile_ids)}
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
    return {
        **parsed,
        "ok": bool(parsed.get("ok")) and parsed.get("recommended_profile") in profile_ids,
        "model": CODEX_MODEL,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "returncode": result.returncode,
        "stderr_tail": result.stderr[-2000:],
        "raw_output": str(output),
    }


def _parse_object(content: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", content, re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else content[content.find("{") : content.rfind("}") + 1]
    value = json.loads(candidate)
    if not isinstance(value, dict):
        raise ValueError("response is not an object")
    return value


def _load_matrix_feedback(workspace: Path) -> Dict[str, Any]:
    cluster_root = workspace / "clusters"
    shard_root = cluster_root / "shards"
    feedback_events = _read_jsonl_tail(shard_root.glob("quality-feedback-events-*.jsonl"), 200)
    review_events = _read_jsonl_tail(shard_root.glob("codex-review-ledger-*.jsonl"), 200)
    systemic_errors = []
    for path in sorted((cluster_root / "reviews").glob("*/codex-cluster-review.json"), reverse=True)[:20]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for error in value.get("systemic_errors") or []:
            normalized = " ".join(str(error).split())
            if normalized and normalized not in systemic_errors:
                systemic_errors.append(normalized)
    latest = {}
    latest_path = cluster_root / "latest-report.json"
    if latest_path.exists():
        try:
            latest = json.loads(latest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            latest = {}
    outcomes = Counter(item.get("outcome") or ("accepted" if item.get("accepted") else "deferred") for item in [*review_events, *feedback_events])
    reasons = Counter(
        str(reason)
        for item in [*review_events, *feedback_events]
        for reason in [*(item.get("reasons") or []), *(item.get("review_reasons") or []), *(item.get("repair_reasons") or [])]
    )
    return {
        "schema_version": "rawg.matrix-feedback.v1",
        "workspace": str(workspace),
        "quality_feedback_events": len(feedback_events),
        "review_events": len(review_events),
        "outcomes": dict(outcomes),
        "top_rejection_reasons": reasons.most_common(25),
        "systemic_errors": systemic_errors[:25],
        "latest_cluster_counts": {
            key: latest.get(key) for key in ["source_results_seen", "cluster_count", "eligible_candidates", "filtered_candidates", "new_kit_proposals"]
        },
        "representative_feedback": feedback_events[-20:],
    }


def _read_jsonl_tail(paths: Iterable[Path], limit: int) -> List[Dict[str, Any]]:
    output = []
    for path in sorted(paths):
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        output.append(json.loads(line))
        except (OSError, json.JSONDecodeError):
            continue
    return output[-limit:]


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
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _write_report_md(path: Path, report: Dict[str, Any]) -> None:
    lines = [
        "# RAWG Matrix Optimizer Report", "",
        f"- Winner: `{report['winner']}`",
        f"- Rate: `{report['games_per_minute']}` games/minute",
        f"- Full corpus estimate: `{report['full_corpus_eta_days']}` days",
        f"- Malformed JSONL: `{report['malformed_jsonl']}`", "", "## Profiles", "",
    ]
    for value in sorted(report["profile_reports"], key=lambda item: -item["score"]):
        lines.append(
            f"- `{value['profile_id']}`: score={value['score']}, rate={value['games_per_minute']}/min, "
            f"subdomains={value['subdomain_rate']}, direct={value['direct_evidence_ratio']}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
