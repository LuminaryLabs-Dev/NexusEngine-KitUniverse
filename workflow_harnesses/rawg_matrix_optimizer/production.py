from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from kituniverse_harness.smart_router import SmartRoutingService
from workflow_harnesses.rawg_capability_pipeline.contracts import stable_hash
from workflow_harnesses.rawg_capability_pipeline.source_adapter import stream_rawg_records

from .workflow_rawg_matrix_optimizer import (
    DEFAULT_BASE_URL,
    DEFAULT_SOURCE_ROOT,
    MODEL_12B,
    PROFILES,
    ShardedJsonlWriter,
    _provider_preflight,
    _run_game,
)


DEFAULT_WORKSPACE = Path("runs/rawg-881k/matrix-production")
WINNER_PROFILE_ID = "12b-seed-12b-walk-lean-beam2-d2"
TOTAL_RAWG_RECORDS = 881069


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--profile", default=WINNER_PROFILE_ID)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--parallel-per-model", type=int, default=8)
    parser.add_argument("--context-per-slot", type=int, default=2000)
    parser.add_argument("--max-tokens", type=int, default=96)
    parser.add_argument("--timeout-seconds", type=int, default=45)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--shard-max-mb", type=int, default=90)
    parser.add_argument("--min-free-gib", type=float, default=10.0)
    parser.add_argument("--max-records", type=int, default=0, help="new records this invocation; 0 means all remaining")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-rawg-matrix-production",
        description="Run the winning RAWG matrix profile with resumable 90 MB JSONL shards.",
    )
    configure_parser(parser)
    report = asyncio.run(run_production(parser.parse_args(argv)))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("status") in {"limit-complete", "complete"} else 1


async def run_production(args: argparse.Namespace) -> Dict[str, Any]:
    profiles = {item["profile_id"]: item for item in PROFILES}
    if args.profile not in profiles:
        raise ValueError(f"unknown profile: {args.profile}")
    if not 1 <= args.concurrency <= 8 or not 1 <= args.parallel_per_model <= 8:
        raise ValueError("concurrency and parallel-per-model must be between 1 and 8")
    if args.context_per_slot > 2000 or args.shard_max_mb > 90:
        raise ValueError("context-per-slot and shard-max-mb exceed production limits")
    profile = profiles[args.profile]
    workspace = args.workspace.resolve()
    shards = workspace / "shards"
    workspace.mkdir(parents=True, exist_ok=True)
    max_bytes = args.shard_max_mb * 1_000_000
    result_writer = ShardedJsonlWriter(shards, "production-results", max_bytes)
    ledger_writer = ShardedJsonlWriter(shards, "completion-ledger", max_bytes)
    completed = _read_completed(shards)
    previous_status = _read_json(workspace / "status.json", {})
    started_total = int(previous_status.get("completed_records") or len(completed))
    invocation_started = time.monotonic()
    new_completed = 0
    new_successful = 0
    skipped = 0
    failed = 0
    current_file = None
    current_line = 0
    router = SmartRoutingService(
        args.base_url, MODEL_12B, args.timeout_seconds,
        max_predictions=args.parallel_per_model,
        max_context_tokens=min(args.context_per_slot, 700),
    )
    preflight_args = argparse.Namespace(
        base_url=args.base_url,
        timeout_seconds=args.timeout_seconds,
        context_per_slot=args.context_per_slot,
        parallel_per_model=args.parallel_per_model,
    )
    preflight = _provider_preflight(preflight_args, [MODEL_12B])
    _write_json(workspace / "provider-preflight.json", preflight)
    manifest = {
        "schema_version": "rawg.matrix-production-manifest.v1",
        "dataset_records": TOTAL_RAWG_RECORDS,
        "profile": profile,
        "source_root": str(args.source_root),
        "workspace": str(workspace),
        "controls": {
            "concurrency": args.concurrency,
            "parallel_per_model": args.parallel_per_model,
            "context_per_slot": args.context_per_slot,
            "max_tokens": args.max_tokens,
            "batch_size": args.batch_size,
            "shard_max_bytes": max_bytes,
            "min_free_gib": args.min_free_gib,
        },
        "completion_identity": "source_hash + profile_id",
        "started_at": datetime.now().astimezone().isoformat(),
    }
    _write_json(workspace / "manifest.json", manifest)
    if not preflight.get("ok"):
        status = {"ok": False, "status": "hold", "reason": "provider-preflight-failed", "preflight": preflight}
        _write_json(workspace / "status.json", status)
        router.shutdown()
        return status

    worker_args = argparse.Namespace(
        beam_width=int(profile.get("beam_width") or 2),
        max_depth=int(profile.get("max_depth") or 2),
        max_tokens=args.max_tokens,
    )
    batch: List[Dict[str, Any]] = []

    async def flush(values: List[Dict[str, Any]]) -> bool:
        nonlocal new_completed, new_successful, failed
        if not values:
            return True
        if _free_gib(workspace) < args.min_free_gib:
            _write_status("hold", "low-disk-space")
            return False
        usable = [value for value in values if value.get("has_mechanic_evidence") and not value.get("error")]
        generated = await asyncio.gather(*( _run_game(value, profile, {MODEL_12B: router}, worker_args) for value in usable))
        generated_by_hash = {value["source_hash"]: value for value in generated}
        for source in values:
            result = generated_by_hash.get(source["source_hash"])
            if result is None:
                result = {
                    "schema_version": "rawg.matrix-game-result.v1",
                    "source_id": source["source_id"],
                    "source_hash": source["source_hash"],
                    "source_file": source.get("source_file"),
                    "source_line": source.get("source_line"),
                    "profile_id": profile["profile_id"],
                    "ok": False,
                    "status": "failed" if source.get("error") else "insufficient-evidence",
                    "error": source.get("error"),
                    "accepted_nodes": [],
                    "rejections": [],
                    "calls": [],
                }
            identity = stable_hash([source["source_hash"], profile["profile_id"]])
            result_writer.append(result)
            ledger_writer.append({
                "identity": identity,
                "source_id": source["source_id"],
                "source_hash": source["source_hash"],
                "source_file": source.get("source_file"),
                "source_line": source.get("source_line"),
                "profile_id": profile["profile_id"],
                "status": "complete" if result.get("ok") else result.get("status", "filtered"),
            })
            completed.add(identity)
            new_completed += 1
            if result.get("ok"):
                new_successful += 1
            elif result.get("status") == "failed":
                failed += 1
        _write_status("running")
        return True

    def _write_status(status: str, reason: Optional[str] = None) -> None:
        elapsed = max(0.001, time.monotonic() - invocation_started)
        rate = new_completed / elapsed * 60
        total = started_total + new_completed
        payload = {
            "ok": status in {"running", "limit-complete", "complete"},
            "status": status,
            "reason": reason,
            "profile_id": profile["profile_id"],
            "completed_records": total,
            "successful_hierarchies": int(previous_status.get("successful_hierarchies") or 0) + new_successful,
            "failed_records": int(previous_status.get("failed_records") or 0) + failed,
            "skipped_existing_this_invocation": skipped,
            "new_records_this_invocation": new_completed,
            "current_file": current_file,
            "current_line": current_line,
            "games_per_minute": round(rate, 3),
            "estimated_days_remaining": round((TOTAL_RAWG_RECORDS - total) / max(rate, 0.0001) / 60 / 24, 3),
            "free_gib": round(_free_gib(workspace), 3),
            "router": router.stats(),
            "result_shards": result_writer.paths,
            "ledger_shards": ledger_writer.paths,
            "max_shard_bytes": max_bytes,
            "updated_at": datetime.now().astimezone().isoformat(),
        }
        _write_json(workspace / "status.json", payload)

    try:
        for source, file_name, line_number in stream_rawg_records(args.source_root, "rawg-matrix-production"):
            current_file, current_line = file_name, line_number
            identity = stable_hash([source["source_hash"], profile["profile_id"]])
            if identity in completed:
                skipped += 1
                continue
            batch.append(source)
            remaining_limit = args.max_records - new_completed if args.max_records else args.batch_size
            target_batch_size = min(args.batch_size, remaining_limit) if args.max_records else args.batch_size
            if len(batch) >= target_batch_size:
                if not await flush(batch):
                    batch = []
                    break
                batch = []
                if args.max_records and new_completed >= args.max_records:
                    break
        if batch and (not args.max_records or new_completed < args.max_records):
            await flush(batch[: max(0, args.max_records - new_completed) if args.max_records else None])
        total = started_total + new_completed
        final_status = "complete" if total >= TOTAL_RAWG_RECORDS else ("limit-complete" if args.max_records and new_completed >= args.max_records else "hold")
        _write_status(final_status, None if final_status != "hold" else "stream-ended-before-total")
    finally:
        router.shutdown()
    return _read_json(workspace / "status.json", {})


def _read_completed(shards: Path) -> set[str]:
    output = set()
    for path in sorted(shards.glob("completion-ledger-*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    value = json.loads(line)
                    if value.get("identity"):
                        output.add(value["identity"])
    return output


def _free_gib(path: Path) -> float:
    return shutil.disk_usage(path).free / (1024 ** 3)


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
