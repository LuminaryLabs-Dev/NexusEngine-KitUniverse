from __future__ import annotations

from typing import Any, Dict, List


def build_promotion_index(
    batch_manifest: Dict[str, Any],
    packet_report: Dict[str, Any],
    dry_run_report: Dict[str, Any],
) -> Dict[str, Any]:
    batch_count = batch_manifest.get("batch_count", 0)
    packet_count = packet_report.get("packet_count", 0)
    dry_run_batch_count = dry_run_report.get("batch_count", 0)
    ready = []
    blocked = []

    for batch in batch_manifest.get("batches", []):
        entry = _promotion_entry(batch)
        if packet_report.get("ok") and dry_run_report.get("ok"):
            ready.append(entry)
        else:
            blocked.append({**entry, "blocked_reason": "packet-or-dry-run-report-failed"})

    checks = [
        _check(
            "counts-align",
            "batch, packet, and dry-run counts align",
            batch_count == packet_count == dry_run_batch_count,
            {
                "batch_count": batch_count,
                "packet_count": packet_count,
                "dry_run_batch_count": dry_run_batch_count,
            },
        ),
        _check(
            "all-ready",
            "every bounded batch is ready for downstream build promotion",
            len(ready) == batch_count and not blocked,
            {"ready_count": len(ready), "blocked_count": len(blocked), "batch_count": batch_count},
        ),
        _check(
            "record-coverage",
            "ready promotion entries cover every final record exactly once through batch assignment",
            sum(entry["record_count"] for entry in ready) == batch_manifest.get("record_count", 0),
            {
                "ready_records": sum(entry["record_count"] for entry in ready),
                "manifest_records": batch_manifest.get("record_count", 0),
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "build-promotion-index",
        "ready_count": len(ready),
        "blocked_count": len(blocked),
        "record_count": sum(entry["record_count"] for entry in ready),
        "ready": ready,
        "blocked": blocked[:200],
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _promotion_entry(batch: Dict[str, Any]) -> Dict[str, Any]:
    batch_id = batch.get("batch_id")
    return {
        "promotion_id": f"promote-{batch_id}",
        "batch_id": batch_id,
        "primary_dependency": batch.get("primary_dependency"),
        "record_count": batch.get("record_count", 0),
        "source_packet_dir": f"build-inputs/{batch_id}",
        "source_kit_records_jsonl": f"build-inputs/{batch_id}/kit-records.jsonl",
        "source_work_order_json": f"build-inputs/{batch_id}/work-order.json",
        "source_build_report_json": f"batch-results/{batch_id}/build-report.json",
        "expected_next_artifact": f"promoted-batches/{batch_id}/promotion-report.json",
        "resume_hint": "Use this entry as the single-batch promotion queue item for a downstream builder.",
    }


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
