from __future__ import annotations

from typing import Any, Dict, List, Tuple


def build_work_orders(
    build_batch_manifest: Dict[str, Any],
    final_jsonl: str,
    run_dir: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    orders = []
    for batch in build_batch_manifest.get("batches", []):
        orders.append(_order(batch, final_jsonl, run_dir))

    batch_ids = [order["batch_id"] for order in orders]
    duplicate_batch_ids = len(batch_ids) - len(set(batch_ids))
    assigned_records = sum(order["record_count"] for order in orders)
    checks = [
        _check(
            "one-order-per-batch",
            "each build batch has exactly one work order",
            len(orders) == build_batch_manifest.get("batch_count", 0),
            {"orders": len(orders), "batch_count": build_batch_manifest.get("batch_count", 0)},
        ),
        _check(
            "unique-work-order-ids",
            "work order ids are unique",
            duplicate_batch_ids == 0,
            {"duplicate_batch_ids": duplicate_batch_ids},
        ),
        _check(
            "all-records-covered",
            "work orders cover every assigned batch record",
            assigned_records == build_batch_manifest.get("record_count", 0),
            {"assigned_records": assigned_records, "record_count": build_batch_manifest.get("record_count", 0)},
        ),
        _check(
            "manifest-ok",
            "source build batch manifest passed before work order emission",
            bool(build_batch_manifest.get("ok")),
            {"manifest_ok": build_batch_manifest.get("ok")},
        ),
    ]
    report = {
        "ok": all(check["ok"] for check in checks),
        "stage": "build-work-orders",
        "work_order_count": len(orders),
        "batch_count": build_batch_manifest.get("batch_count", 0),
        "record_count": build_batch_manifest.get("record_count", 0),
        "assigned_records": assigned_records,
        "duplicate_batch_ids": duplicate_batch_ids,
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }
    return orders, report


def _order(batch: Dict[str, Any], final_jsonl: str, run_dir: str) -> Dict[str, Any]:
    return {
        "work_order_id": f"build-{batch.get('batch_id')}",
        "batch_id": batch.get("batch_id"),
        "primary_dependency": batch.get("primary_dependency"),
        "record_count": batch.get("record_count"),
        "record_ids": batch.get("record_ids", []),
        "source_final_jsonl": final_jsonl,
        "source_run_dir": run_dir,
        "expected_artifacts": [
            f"build-inputs/{batch.get('batch_id')}/kit-records.jsonl",
            f"build-inputs/{batch.get('batch_id')}/work-order.json",
            f"build-inputs/{batch.get('batch_id')}/packet-report.json",
            f"batch-results/{batch.get('batch_id')}/build-report.json",
        ],
        "controls": {
            "max_context_tokens": 100,
            "max_predictions": 128,
            "build_scope": "single bounded batch",
        },
        "resume_hint": "Load source_final_jsonl, select record_ids in order, then emit expected_artifacts.",
    }


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
