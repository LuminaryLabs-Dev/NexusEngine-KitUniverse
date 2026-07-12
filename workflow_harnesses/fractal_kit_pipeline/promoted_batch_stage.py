from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.fractal_kit_pipeline.run_artifacts import write_json


def write_promoted_batches(run_dir: Path, promotion_index: Dict[str, Any]) -> Dict[str, Any]:
    promoted_root = run_dir / "promoted-batches"
    promoted = []
    blocked = []

    for entry in promotion_index.get("ready", []):
        report = _promote_entry(promoted_root, entry)
        promoted.append(report)

    for entry in promotion_index.get("blocked", []):
        blocked.append(
            {
                "batch_id": entry.get("batch_id"),
                "promotion_id": entry.get("promotion_id"),
                "blocked_reason": entry.get("blocked_reason", "blocked-upstream"),
            }
        )

    write_json(promoted_root / "index.json", {"promoted": promoted, "blocked": blocked})
    checks = [
        _check(
            "all-ready-promoted",
            "every ready promotion index entry produced a promotion report",
            len(promoted) == promotion_index.get("ready_count", 0),
            {"promoted_count": len(promoted), "ready_count": promotion_index.get("ready_count", 0)},
        ),
        _check(
            "zero-blocked-promotions",
            "no promotion index entries are blocked",
            not blocked and promotion_index.get("blocked_count", 0) == 0,
            {"blocked_count": len(blocked), "index_blocked_count": promotion_index.get("blocked_count", 0)},
        ),
        _check(
            "promotion-record-coverage",
            "promoted batch reports cover every promotion-index record",
            sum(report["record_count"] for report in promoted) == promotion_index.get("record_count", 0),
            {
                "promoted_records": sum(report["record_count"] for report in promoted),
                "promotion_index_records": promotion_index.get("record_count", 0),
            },
        ),
        _check(
            "promotion-reports-ok",
            "every written promotion report passes its own gate",
            all(report.get("ok") for report in promoted),
            {"failed_promotions": [report["batch_id"] for report in promoted if not report.get("ok")][:20]},
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "promoted-batches",
        "promoted_root": str(promoted_root),
        "index_json": str(promoted_root / "index.json"),
        "promoted_count": len(promoted),
        "blocked_count": len(blocked),
        "record_count": sum(report["record_count"] for report in promoted),
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _promote_entry(promoted_root: Path, entry: Dict[str, Any]) -> Dict[str, Any]:
    batch_id = entry.get("batch_id")
    report = {
        "ok": bool(batch_id) and entry.get("record_count", 0) > 0,
        "stage": "promoted-batch",
        "promotion_id": entry.get("promotion_id"),
        "batch_id": batch_id,
        "primary_dependency": entry.get("primary_dependency"),
        "record_count": entry.get("record_count", 0),
        "source_packet_dir": entry.get("source_packet_dir"),
        "source_kit_records_jsonl": entry.get("source_kit_records_jsonl"),
        "source_work_order_json": entry.get("source_work_order_json"),
        "source_build_report_json": entry.get("source_build_report_json"),
        "next_artifact": entry.get("expected_next_artifact"),
        "resume_hint": "This promoted batch is ready for a real external builder or implementation worker.",
    }
    write_json(promoted_root / str(batch_id) / "promotion-report.json", report)
    return report


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
