from __future__ import annotations

from typing import Any, Dict, List

from workflow_harnesses.fractal_kit_pipeline.simulator_slot_smoke import REQUIRED_SLOTS


def run_build_batch_replay_smoke(
    final_records: List[Dict[str, Any]],
    build_batch_manifest: Dict[str, Any],
) -> Dict[str, Any]:
    record_by_id = {record.get("record_id"): record for record in final_records}
    failed_batches = []
    records_replayed = 0
    batch_summaries = []

    for batch in build_batch_manifest.get("batches", []):
        batch_result = _replay_batch(batch, record_by_id)
        records_replayed += batch_result["records_replayed"]
        batch_summaries.append(
            {
                "batch_id": batch_result["batch_id"],
                "ok": batch_result["ok"],
                "record_count": batch_result["record_count"],
                "records_replayed": batch_result["records_replayed"],
                "primary_dependency": batch_result["primary_dependency"],
            }
        )
        if not batch_result["ok"]:
            failed_batches.append(batch_result)

    checks = [
        _check(
            "all-batches-replayed",
            "every build batch replays successfully from final records",
            not failed_batches,
            {"failed_batch_count": len(failed_batches), "sample": failed_batches[:5]},
        ),
        _check(
            "all-records-replayed",
            "replayed record count matches the final record count",
            records_replayed == len(final_records),
            {"records_replayed": records_replayed, "final_records": len(final_records)},
        ),
        _check(
            "manifest-ok",
            "batch manifest passed its own assignment checks",
            bool(build_batch_manifest.get("ok")),
            {
                "batch_count": build_batch_manifest.get("batch_count"),
                "duplicate_assignments": build_batch_manifest.get("duplicate_assignments"),
                "missing_assignments": build_batch_manifest.get("missing_assignments"),
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "build-batch-replay-smoke",
        "batch_count": len(build_batch_manifest.get("batches", [])),
        "batches_replayed": len(batch_summaries),
        "records_replayed": records_replayed,
        "failed_batch_count": len(failed_batches),
        "required_slots": REQUIRED_SLOTS,
        "batch_summaries": batch_summaries[:200],
        "failed_batches": failed_batches[:20],
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _replay_batch(batch: Dict[str, Any], record_by_id: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    missing_records = []
    slot_failures = []
    records_replayed = 0
    for record_id in batch.get("record_ids", []):
        record = record_by_id.get(record_id)
        if not record:
            missing_records.append(record_id)
            continue
        records_replayed += 1
        payload = record.get("payload", {})
        missing_slots = [slot for slot in REQUIRED_SLOTS if not payload.get(slot)]
        if missing_slots:
            slot_failures.append({"record_id": record_id, "missing_slots": missing_slots})

    ok = (
        not missing_records
        and not slot_failures
        and records_replayed == batch.get("record_count")
        and len(batch.get("record_ids", [])) == batch.get("record_count")
    )
    return {
        "ok": ok,
        "batch_id": batch.get("batch_id"),
        "primary_dependency": batch.get("primary_dependency"),
        "record_count": batch.get("record_count"),
        "records_replayed": records_replayed,
        "missing_records": missing_records[:20],
        "slot_failures": slot_failures[:20],
    }


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
