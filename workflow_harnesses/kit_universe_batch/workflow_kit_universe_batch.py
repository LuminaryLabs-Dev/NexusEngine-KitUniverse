from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from kituniverse_harness.providers import LMStudioProvider
from kituniverse_harness.smart_router import SmartRoutingService
from workflow_harnesses.guided_kit_builder.workflow_guided_kit_builder import (
    DEFAULT_BASE_URL,
    DEFAULT_MODEL,
    _run_case_chain,
)
from workflow_harnesses.kit_universe_batch.batch_ledger import (
    append_quarantine,
    commit_batch,
    duplicate_report,
)
from workflow_harnesses.kit_universe_batch.batch_review import run_batch_review
from workflow_harnesses.kit_universe_batch.simulator_adapter import (
    resolve_simulator_cli,
    run_simulator,
)
from workflow_harnesses.kit_universe_batch.source_planner import build_matrix, load_sources


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUN_ROOT = Path("runs/workflow-harnesses/kit-universe-batch")
DEFAULT_UNIVERSE_ROOT = Path("runs/kit-universe-1000")


def configure_parser(parser: argparse.ArgumentParser) -> None:
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument("--source")
    source_group.add_argument("--source-file", type=Path)
    parser.add_argument("--count", type=int)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--max-attempts", type=int)
    parser.add_argument("--resume")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--context-length", type=int, default=8192)
    parser.add_argument("--max-context-tokens", type=int, default=1024)
    parser.add_argument("--max-tokens", type=int, default=700)
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--provider-retries", type=int, default=1)
    parser.add_argument("--repair-passes", type=int, default=2)
    parser.add_argument("--repair-delay-seconds", type=float, default=0.5)
    parser.add_argument("--max-repair-delay-seconds", type=float, default=4.0)
    parser.add_argument("--task-concurrency", type=int, default=10)
    parser.add_argument("--max-predictions", type=int, default=1)
    parser.add_argument("--simulator-cli")
    parser.add_argument("--simulator-timeout-seconds", type=int, default=300)
    parser.add_argument("--review-timeout-seconds", type=int, default=900)
    parser.add_argument("--skip-codex-review", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--universe-root", type=Path, default=DEFAULT_UNIVERSE_ROOT)
    parser.add_argument("--universe-target", type=int, default=1000)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="kituniverse batch",
        description="Generate and promote an exact requested count of simulator-validated kits.",
    )
    configure_parser(parser)
    args = parser.parse_args(argv)
    report = asyncio.run(run_batch(args))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


def run_from_namespace(args: argparse.Namespace) -> int:
    report = asyncio.run(run_batch(args))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


async def run_batch(args: argparse.Namespace) -> Dict[str, Any]:
    _validate_args(args)
    if args.resume:
        run_dir = _resolve_resume_dir(args.run_root, args.resume)
        manifest = _read_json(run_dir / "manifest.json")
        checkpoint = _read_json(run_dir / "checkpoint.json")
        sources = _read_json(run_dir / "inputs" / "source.json")["sources"]
        matrix = _read_json(run_dir / "inputs" / "coverage-matrix.json")["matrix"]
        requested_count = int(manifest["requested_count"])
        max_attempts = int(manifest["max_attempts"])
        args = _restore_args(args, manifest)
    else:
        requested_count = int(args.count)
        max_attempts = int(args.max_attempts or max(requested_count * 4, requested_count + 10))
        run_id = _run_id()
        run_dir = args.run_root / run_id
        sources = load_sources(args.source, args.source_file)
        matrix = build_matrix(sources, max_attempts)
        checkpoint = {
            "run_id": run_id,
            "status": "initialized",
            "stage": "idle",
            "attempted": 0,
            "achieved": 0,
            "promoted": 0,
            "batch_index": 0,
            "next_matrix_index": 0,
            "pending_batch": None,
        }
        _initialize_run(run_dir, args, sources, matrix, requested_count, max_attempts)
        _write_json(run_dir / "checkpoint.json", checkpoint)

    run_id = str(checkpoint["run_id"])
    simulator_cli = resolve_simulator_cli(args.simulator_cli)
    provider = LMStudioProvider(
        base_url=args.base_url, model=args.model, timeout_seconds=args.timeout_seconds
    )
    load_result = provider.ensure_loaded(context_length=args.context_length)
    health = provider.health()
    _write_json(run_dir / "provider-preflight.json", {"load": load_result, "health": health})
    if not load_result.get("ok") or not health.get("ok"):
        return _finish(run_dir, checkpoint, requested_count, False, "provider-preflight-failed")

    router = SmartRoutingService(
        base_url=args.base_url,
        model=args.model,
        timeout_seconds=args.timeout_seconds,
        max_predictions=args.max_predictions,
        max_context_tokens=args.max_context_tokens,
    )
    started = time.time()
    try:
        while checkpoint["achieved"] < requested_count and checkpoint["attempted"] < max_attempts:
            if checkpoint.get("pending_batch"):
                if checkpoint.get("stage") == "generating":
                    batch_id = str(checkpoint["pending_batch"])
                    entries = _read_json(run_dir / "batches" / batch_id / "matrix-entries.json")["entries"]
                    outcome = await _generate_and_continue(
                        args, run_dir, run_id, checkpoint, router, simulator_cli,
                        batch_id, entries, requested_count,
                    )
                else:
                    outcome = _continue_batch(
                        args, run_dir, run_id, checkpoint, simulator_cli, requested_count
                    )
            else:
                remaining = requested_count - checkpoint["achieved"]
                capacity = min(
                    args.batch_size,
                    remaining,
                    max_attempts - checkpoint["attempted"],
                )
                batch_id = f"batch-{checkpoint['batch_index']:04d}"
                entries = matrix[
                    checkpoint["next_matrix_index"] : checkpoint["next_matrix_index"] + capacity
                ]
                outcome = await _generate_and_continue(
                    args,
                    run_dir,
                    run_id,
                    checkpoint,
                    router,
                    simulator_cli,
                    batch_id,
                    entries,
                    requested_count,
                )
            if outcome.get("hold"):
                checkpoint["status"] = "hold"
                checkpoint["hold_reason"] = outcome.get("reason")
                _write_json(run_dir / "checkpoint.json", checkpoint)
                return _finish(
                    run_dir,
                    checkpoint,
                    requested_count,
                    False,
                    str(outcome.get("reason") or "batch-held"),
                    elapsed=time.time() - started,
                )
            checkpoint["batch_index"] += 1
            checkpoint["pending_batch"] = None
            checkpoint["stage"] = "idle"
            checkpoint["status"] = "running"
            checkpoint.pop("hold_reason", None)
            _write_json(run_dir / "checkpoint.json", checkpoint)
    finally:
        router_stats = router.stats()
        router.shutdown()
        _write_json(run_dir / "router-final.json", router_stats)

    success = checkpoint["achieved"] == requested_count
    reason = "requested-count-achieved" if success else "max-attempts-exhausted"
    checkpoint["status"] = "complete" if success else "partial"
    _write_json(run_dir / "checkpoint.json", checkpoint)
    return _finish(
        run_dir,
        checkpoint,
        requested_count,
        success,
        reason,
        elapsed=time.time() - started,
    )


async def _generate_and_continue(
    args: argparse.Namespace,
    run_dir: Path,
    run_id: str,
    checkpoint: Dict[str, Any],
    router: SmartRoutingService,
    simulator_cli: List[str],
    batch_id: str,
    entries: List[Dict[str, Any]],
    requested_count: int,
) -> Dict[str, Any]:
    batch_dir = run_dir / "batches" / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    _write_json(batch_dir / "matrix-entries.json", {"entries": entries})
    checkpoint["pending_batch"] = batch_id
    checkpoint["stage"] = "generating"
    checkpoint["status"] = "running"
    _write_json(run_dir / "checkpoint.json", checkpoint)
    semaphore = asyncio.Semaphore(args.task_concurrency)

    async def generate(entry: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            result = await _run_case_chain(
                router=router,
                case=entry["case"],
                max_tokens=args.max_tokens,
                temperature=args.temperature,
                provider_retries=args.provider_retries,
                repair_passes=args.repair_passes,
                repair_delay_seconds=args.repair_delay_seconds,
                max_repair_delay_seconds=args.max_repair_delay_seconds,
                min_case_coverage=0.72,
            )
            return {"matrix_id": entry["matrix_id"], "result": result}

    progress_path = batch_dir / "generation-results.jsonl"
    progress = _read_jsonl(progress_path)
    completed_ids = {item["matrix_id"] for item in progress}
    pending = [entry for entry in entries if entry["matrix_id"] not in completed_ids]
    tasks = [asyncio.create_task(generate(entry)) for entry in pending]
    for task in asyncio.as_completed(tasks):
        progress.append(await task)
        _write_jsonl(progress_path, progress)
        _write_json(
            run_dir / "checkpoint.json",
            {**checkpoint, "generation_completed": len(progress), "generation_total": len(entries)},
        )
    result_by_id = {item["matrix_id"]: item["result"] for item in progress}
    results = [result_by_id[entry["matrix_id"]] for entry in entries]
    candidates = [result["record"] for result in results if result.get("accepted")]
    attempts = [
        {"idea_id": result["idea_id"], **attempt}
        for result in results
        for attempt in result.get("attempts", [])
    ]
    _write_jsonl(batch_dir / "attempts.jsonl", attempts)
    _write_jsonl(batch_dir / "candidates.jsonl", candidates)
    rejected = []
    for result in results:
        if result.get("accepted"):
            continue
        rejected.append(
            _quarantine_entry(
                run_id,
                batch_id,
                result.get("idea_id"),
                "generation",
                result.get("validation", {}).get("errors", ["generation-rejected"]),
            )
        )
    _record_quarantine(run_dir, args.universe_root, rejected)
    checkpoint["attempted"] += len(entries)
    checkpoint["next_matrix_index"] += len(entries)
    checkpoint["pending_batch"] = batch_id
    checkpoint["stage"] = "generated"
    checkpoint["status"] = "running"
    _write_json(run_dir / "checkpoint.json", checkpoint)
    _event(run_dir, "batch-generated", {"batch_id": batch_id, "attempted": len(entries), "accepted": len(candidates)})
    return _continue_batch(args, run_dir, run_id, checkpoint, simulator_cli, requested_count)


def _continue_batch(
    args: argparse.Namespace,
    run_dir: Path,
    run_id: str,
    checkpoint: Dict[str, Any],
    simulator_cli: List[str],
    requested_count: int,
) -> Dict[str, Any]:
    batch_id = str(checkpoint["pending_batch"])
    batch_dir = run_dir / "batches" / batch_id
    stage = str(checkpoint.get("stage") or "generated")
    if stage == "generated":
        candidates = _read_jsonl(batch_dir / "candidates.jsonl")
        validation = _fresh_validation(batch_dir / "candidates.jsonl")
        _write_json(batch_dir / "fresh-validation.json", validation)
        valid_ids = {item["record_id"] for item in validation.get("decisions", []) if item["ok"]}
        valid = [record for record in candidates if record.get("record_id") in valid_ids]
        duplicate = duplicate_report(valid, args.universe_root)
        _write_json(batch_dir / "duplicate-report.json", duplicate)
        duplicate_ok = {item["record_id"] for item in duplicate["decisions"] if item["ok"]}
        eligible = [record for record in valid if record.get("record_id") in duplicate_ok]
        rejected = []
        for item in validation.get("decisions", []):
            if not item["ok"]:
                rejected.append(_quarantine_entry(run_id, batch_id, item["record_id"], "fresh-validation", item["errors"]))
        for item in duplicate["decisions"]:
            if not item["ok"]:
                rejected.append(_quarantine_entry(run_id, batch_id, item["record_id"], "duplicate", item["reasons"]))
        _record_quarantine(run_dir, args.universe_root, rejected)
        _write_jsonl(batch_dir / "eligible-candidates.jsonl", eligible)
        if eligible:
            simulator = run_simulator(
                simulator_cli,
                batch_dir / "eligible-candidates.jsonl",
                batch_dir / "nexus-simulator-report.json",
                f"{run_id}-{batch_id}",
                args.simulator_timeout_seconds,
            )
            _write_json(batch_dir / "nexus-simulator-report.json", simulator)
        else:
            simulator = {"ok": True, "status": "passed", "recordsTested": 0, "results": []}
            _write_json(batch_dir / "nexus-simulator-report.json", simulator)
        simulator_ids = {
            item.get("recordId") for item in simulator.get("results", []) if item.get("ok")
        }
        simulated = [record for record in eligible if record.get("record_id") in simulator_ids]
        if not eligible:
            simulated = []
        rejected = [
            _quarantine_entry(run_id, batch_id, item.get("recordId"), "nexus-simulator", item.get("errors", []))
            for item in simulator.get("results", [])
            if not item.get("ok")
        ]
        _record_quarantine(run_dir, args.universe_root, rejected)
        _write_jsonl(batch_dir / "simulator-passing.jsonl", simulated)
        _write_jsonl(
            batch_dir / "review-candidates.jsonl",
            [_review_candidate(record) for record in simulated],
        )
        _write_json(
            batch_dir / "simulator-review-summary.json",
            {
                "status": simulator.get("status"),
                "records_tested": simulator.get("recordsTested", 0),
                "accepted": simulator.get("accepted", 0),
                "rejected": simulator.get("rejected", 0),
                "results": [
                    {
                        "record_id": item.get("recordId"),
                        "ok": item.get("ok"),
                        "errors": item.get("errors", []),
                    }
                    for item in simulator.get("results", [])
                ],
                "full_report": str(batch_dir / "nexus-simulator-report.json"),
            },
        )
        checkpoint["stage"] = "simulated"
        _write_json(run_dir / "checkpoint.json", checkpoint)
        if eligible and not simulator.get("ok") and not simulated:
            if not simulator.get("results"):
                return {"hold": True, "reason": "nexus-simulator-unavailable-or-malformed"}
            return {"hold": False, "promoted": 0}
        stage = "simulated"

    if stage == "simulated":
        simulated = _read_jsonl(batch_dir / "simulator-passing.jsonl")
        if args.skip_codex_review:
            review = {
                "ok": True,
                "skipped": True,
                "promotion_allowed": False,
                "decisions": [
                    {"record_id": record["record_id"], "accepted": True, "reasons": ["review-skipped-dry-run"]}
                    for record in simulated
                ],
            }
        elif simulated:
            review = run_batch_review(
                REPO_ROOT,
                batch_dir,
                batch_dir / "review-candidates.jsonl",
                batch_dir / "simulator-review-summary.json",
                batch_dir / "duplicate-report.json",
                [record["record_id"] for record in simulated],
                args.review_timeout_seconds,
            )
            review["promotion_allowed"] = bool(review.get("ok"))
        else:
            review = {"ok": True, "promotion_allowed": True, "decisions": []}
        _write_json(batch_dir / "sol-review.json", review)
        if not review.get("ok"):
            checkpoint["stage"] = "simulated"
            _write_json(run_dir / "checkpoint.json", checkpoint)
            return {"hold": True, "reason": "sol-review-unavailable-or-malformed"}
        accepted_ids = {
            item["record_id"] for item in review["decisions"] if item.get("accepted") is True
        }
        reviewed = [record for record in simulated if record["record_id"] in accepted_ids]
        rejected = [
            _quarantine_entry(run_id, batch_id, item.get("record_id"), "sol-review", item.get("reasons", []))
            for item in review["decisions"]
            if item.get("accepted") is not True
        ]
        _record_quarantine(run_dir, args.universe_root, rejected)
        _write_jsonl(batch_dir / "review-passing.jsonl", reviewed)
        checkpoint["stage"] = "reviewed"
        _write_json(run_dir / "checkpoint.json", checkpoint)
        stage = "reviewed"

    reviewed = _read_jsonl(batch_dir / "review-passing.jsonl")
    if args.dry_run or args.skip_codex_review:
        promoted = 0
        achieved = len(reviewed)
        promotion = {
            "ok": True,
            "dry_run": True,
            "promoted": 0,
            "validated": achieved,
            "reason": "review-skipped" if args.skip_codex_review else "dry-run",
        }
    else:
        promotion = commit_batch(
            args.universe_root, reviewed, run_id, batch_id, args.universe_target
        )
        if not promotion.get("ok"):
            return {"hold": True, "reason": "batch-commit-failed", "promotion": promotion}
        promoted = int(promotion.get("promoted") or 0)
        achieved = promoted
    _write_json(batch_dir / "promotion.json", promotion)
    checkpoint["promoted"] += promoted
    checkpoint["achieved"] += achieved
    checkpoint["stage"] = "promoted"
    _write_json(run_dir / "checkpoint.json", checkpoint)
    _event(run_dir, "batch-finished", {"batch_id": batch_id, "promoted": promoted, "achieved": achieved})
    return {"hold": False, "promoted": promoted, "achieved": achieved}


def _fresh_validation(path: Path) -> Dict[str, Any]:
    command = [sys.executable, "-m", "workflow_harnesses.kit_universe_batch.batch_validation", str(path)]
    result = subprocess.run(
        command, cwd=REPO_ROOT, capture_output=True, text=True, timeout=120, check=False
    )
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError:
        report = {"ok": False, "error": "fresh batch validator returned malformed JSON", "decisions": []}
    report["returncode"] = result.returncode
    report["stderr_tail"] = result.stderr[-2000:]
    return report


def _review_candidate(record: Dict[str, Any]) -> Dict[str, Any]:
    payload = record.get("payload") or {}
    evidence = payload.get("source_evidence") or {}
    return {
        "record_id": record.get("record_id"),
        "name": payload.get("name"),
        "domain": payload.get("domain"),
        "purpose": payload.get("purpose"),
        "owned_state": payload.get("owned_state", []),
        "requires": payload.get("requires", []),
        "provides": payload.get("provides", []),
        "state_rules": payload.get("state_rules", []),
        "implementation_steps": payload.get("implementation_steps", []),
        "source_description": evidence.get("description"),
        "source_behavior_rules": evidence.get("behavior_rules", []),
        "source_context": evidence.get("source_context", {}),
    }


def _record_quarantine(run_dir: Path, universe_root: Path, entries: List[Dict[str, Any]]) -> None:
    if not entries:
        return
    path = run_dir / "quarantine.jsonl"
    existing = _read_jsonl(path) if path.exists() else []
    by_id = {entry["quarantine_id"]: entry for entry in [*existing, *entries]}
    _write_jsonl(path, by_id.values())
    append_quarantine(universe_root, entries)


def _quarantine_entry(
    run_id: str, batch_id: str, record_id: Any, stage: str, reasons: Iterable[Any]
) -> Dict[str, Any]:
    stable_id = str(record_id or "unknown")
    return {
        "quarantine_id": f"{run_id}:{batch_id}:{stage}:{stable_id}",
        "run_id": run_id,
        "batch_id": batch_id,
        "record_id": stable_id,
        "stage": stage,
        "reasons": [str(reason) for reason in reasons],
    }


def _initialize_run(
    run_dir: Path,
    args: argparse.Namespace,
    sources: List[Dict[str, Any]],
    matrix: List[Dict[str, Any]],
    requested_count: int,
    max_attempts: int,
) -> None:
    (run_dir / "inputs").mkdir(parents=True, exist_ok=True)
    (run_dir / "outputs").mkdir(parents=True, exist_ok=True)
    (run_dir / "decisions").mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": "kit-universe-batch.v1",
        "run_id": run_dir.name,
        "requested_count": requested_count,
        "batch_size": args.batch_size,
        "max_attempts": max_attempts,
        "base_url": args.base_url,
        "model": args.model,
        "max_predictions": args.max_predictions,
        "task_concurrency": args.task_concurrency,
        "dry_run": bool(args.dry_run),
        "skip_codex_review": bool(args.skip_codex_review),
        "simulator_cli": args.simulator_cli,
        "universe_root": str(args.universe_root),
        "universe_target": args.universe_target,
        "stage_graph": [
            "source-matrix",
            "guided-generation-repair",
            "fresh-validation",
            "duplicate-filter",
            "nexus-simulator",
            "sol-batch-review",
            "journaled-promotion",
        ],
    }
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(run_dir / "inputs" / "source.json", {"sources": sources})
    _write_json(run_dir / "inputs" / "coverage-matrix.json", {"matrix": matrix})
    _write_jsonl(run_dir / "events.jsonl", [])


def _restore_args(args: argparse.Namespace, manifest: Dict[str, Any]) -> argparse.Namespace:
    args.count = int(manifest["requested_count"])
    args.batch_size = int(manifest["batch_size"])
    args.max_attempts = int(manifest["max_attempts"])
    args.base_url = manifest["base_url"]
    args.model = manifest["model"]
    args.max_predictions = int(manifest["max_predictions"])
    args.task_concurrency = int(manifest["task_concurrency"])
    args.dry_run = bool(manifest["dry_run"])
    args.skip_codex_review = bool(manifest["skip_codex_review"])
    args.simulator_cli = args.simulator_cli or manifest.get("simulator_cli")
    args.universe_root = Path(manifest["universe_root"])
    args.universe_target = int(manifest["universe_target"])
    return args


def _finish(
    run_dir: Path,
    checkpoint: Dict[str, Any],
    requested_count: int,
    ok: bool,
    reason: str,
    elapsed: float = 0.0,
) -> Dict[str, Any]:
    report = {
        "ok": ok,
        "reason": reason,
        "run_id": run_dir.name,
        "run_dir": str(run_dir),
        "requested_count": requested_count,
        "achieved": checkpoint.get("achieved", 0),
        "promoted": checkpoint.get("promoted", 0),
        "attempted": checkpoint.get("attempted", 0),
        "remaining": max(0, requested_count - int(checkpoint.get("achieved", 0))),
        "status": checkpoint.get("status"),
        "stage": checkpoint.get("stage"),
        "pending_batch": checkpoint.get("pending_batch"),
        "elapsed_seconds": round(elapsed, 3),
        "resume_command": f"kituniverse batch --resume {run_dir.name}",
        "dry_run": bool(_read_json(run_dir / "manifest.json").get("dry_run")),
    }
    _write_json(run_dir / "validation-report.json", report)
    _write_json(run_dir / "final-report.json", report)
    _write_json(run_dir / "outputs" / "summary.json", report)
    return report


def _validate_args(args: argparse.Namespace) -> None:
    if args.resume:
        if args.source or args.source_file or args.count:
            raise ValueError("--resume cannot be combined with source or count")
    else:
        if bool(args.source) == bool(args.source_file):
            raise ValueError("provide exactly one of --source or --source-file")
        if not args.count or args.count < 1:
            raise ValueError("--count must be at least 1")
    if args.batch_size < 1:
        raise ValueError("--batch-size must be at least 1")
    if args.max_predictions < 1:
        raise ValueError("--max-predictions must be at least 1")
    if args.skip_codex_review and not args.dry_run:
        raise ValueError("--skip-codex-review requires --dry-run; reviewless runs cannot promote")


def _event(run_dir: Path, action: str, data: Dict[str, Any]) -> None:
    path = run_dir / "events.jsonl"
    events = _read_jsonl(path) if path.exists() else []
    event_id = f"event-{len(events) + 1:06d}"
    events.append(
        {
            "runId": run_dir.name,
            "eventId": event_id,
            "timestamp": datetime.now().astimezone().isoformat(),
            "layer": "workflow",
            "actorId": "kit-universe-batch",
            "actionId": action,
            "decisionId": event_id,
            "attempt": len(events) + 1,
            "data": data,
        }
    )
    _write_jsonl(path, events)


def _run_id() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]


def _resolve_resume_dir(run_root: Path, resume: str) -> Path:
    candidate = Path(resume).expanduser()
    if candidate.is_absolute() or (candidate / "manifest.json").exists():
        return candidate
    return run_root / candidate


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, values: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(value, sort_keys=True) + "\n" for value in values),
        encoding="utf-8",
    )


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


if __name__ == "__main__":
    raise SystemExit(main())
