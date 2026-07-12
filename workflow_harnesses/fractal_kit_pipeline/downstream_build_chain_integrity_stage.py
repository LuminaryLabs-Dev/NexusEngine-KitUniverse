from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from workflow_harnesses.fractal_kit_pipeline.run_artifacts import count_jsonl


def build_downstream_build_chain_integrity(run_dir: Path, target_count: int) -> Dict[str, Any]:
    final_records = _read_jsonl(run_dir / "final-kits.jsonl")
    final_ids = [str(record.get("record_id", "")) for record in final_records]
    final_id_set = set(final_ids)
    build_batches = _read_json(run_dir / "build-batches.json")
    work_orders = _read_jsonl(run_dir / "build-work-orders.jsonl")
    build_inputs_index = _read_json(run_dir / "build-inputs" / "index.json")
    batch_results_index = _read_json(run_dir / "batch-results" / "index.json")
    promotion_index = _read_json(run_dir / "build-promotion-index.json")
    promoted_index = _read_json(run_dir / "promoted-batches" / "index.json")
    build_packet_report = _read_json(run_dir / "build-batch-packets-report.json")
    dry_run_report = _read_json(run_dir / "build-batch-dry-run-report.json")
    promoted_report = _read_json(run_dir / "promoted-batches-report.json")

    batches = build_batches.get("batches", [])
    packets = build_inputs_index.get("packets", [])
    batch_results = batch_results_index.get("batches", [])
    ready_entries = promotion_index.get("ready", [])
    promoted_entries = promoted_index.get("promoted", [])
    batch_ids = [str(batch.get("batch_id", "")) for batch in batches]
    work_order_ids = [str(order.get("batch_id", "")) for order in work_orders]
    packet_ids = [str(packet.get("batch_id", "")) for packet in packets]
    result_ids = [str(result.get("batch_id", "")) for result in batch_results]
    ready_ids = [str(entry.get("batch_id", "")) for entry in ready_entries]
    promoted_ids = [str(entry.get("batch_id", "")) for entry in promoted_entries]

    batch_record_ids = _flatten_record_ids(batches)
    work_order_record_ids = _flatten_record_ids(work_orders)
    packet_record_ids, packet_failures = _packet_record_ids(run_dir, packets)
    result_record_count = sum(int(result.get("records_checked", 0)) for result in batch_results)
    ready_record_count = sum(int(entry.get("record_count", 0)) for entry in ready_entries)
    promoted_record_count = sum(int(entry.get("record_count", 0)) for entry in promoted_entries)
    promotion_report_failures = _promotion_report_failures(run_dir, promoted_entries)

    checks = [
        _check(
            "final-count",
            "final JSONL has the configured target count and no duplicate record ids",
            len(final_records) == target_count and len(final_ids) == len(final_id_set),
            {
                "target_count": target_count,
                "final_records": len(final_records),
                "final_unique_ids": len(final_id_set),
                "final_duplicate_ids": _duplicates(final_ids)[:20],
            },
        ),
        _coverage_check(
            "build-batch-id-coverage",
            "build batches assign every final record exactly once",
            final_id_set,
            batch_record_ids,
            build_batches.get("ok"),
        ),
        _coverage_check(
            "work-order-id-coverage",
            "build work orders carry every final record exactly once",
            final_id_set,
            work_order_record_ids,
            _read_json(run_dir / "build-work-orders-report.json").get("ok"),
        ),
        _coverage_check(
            "packet-id-coverage",
            "materialized packet JSONL files carry every final record exactly once",
            final_id_set,
            packet_record_ids,
            build_packet_report.get("ok") and not packet_failures,
            {"packet_failures": packet_failures[:20]},
        ),
        _check(
            "batch-id-chain",
            "batch ids match across batches, work orders, packets, dry-run results, promotion queue, and promoted reports",
            _same_multiset(batch_ids, work_order_ids, packet_ids, result_ids, ready_ids, promoted_ids),
            {
                "batch_count": len(batch_ids),
                "work_order_count": len(work_order_ids),
                "packet_count": len(packet_ids),
                "result_count": len(result_ids),
                "ready_count": len(ready_ids),
                "promoted_count": len(promoted_ids),
                "mismatches": _batch_id_mismatches(
                    {
                        "work_orders": work_order_ids,
                        "packets": packet_ids,
                        "batch_results": result_ids,
                        "promotion_ready": ready_ids,
                        "promoted": promoted_ids,
                    },
                    batch_ids,
                ),
            },
        ),
        _check(
            "packet-artifacts-exist",
            "each packet has kit-records JSONL, work-order JSON, and packet report artifacts with safe JSONL",
            not packet_failures,
            {"packet_failures": packet_failures[:50]},
        ),
        _check(
            "dry-run-counts",
            "dry-run results checked every packet record and reported zero failed batches",
            bool(dry_run_report.get("ok"))
            and result_record_count == target_count
            and int(dry_run_report.get("records_checked", 0)) == target_count
            and int(dry_run_report.get("failed_batch_count", 0)) == 0,
            {
                "index_records_checked": result_record_count,
                "report_records_checked": dry_run_report.get("records_checked"),
                "failed_batch_count": dry_run_report.get("failed_batch_count"),
            },
        ),
        _check(
            "promotion-counts",
            "promotion index and promoted batches cover the full target with zero blocked entries",
            bool(promotion_index.get("ok"))
            and bool(promoted_report.get("ok"))
            and ready_record_count == target_count
            and promoted_record_count == target_count
            and int(promotion_index.get("blocked_count", 0)) == 0
            and int(promoted_report.get("blocked_count", 0)) == 0
            and not promotion_report_failures,
            {
                "ready_record_count": ready_record_count,
                "promoted_record_count": promoted_record_count,
                "promotion_blocked_count": promotion_index.get("blocked_count"),
                "promoted_blocked_count": promoted_report.get("blocked_count"),
                "promotion_report_failures": promotion_report_failures[:20],
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "downstream-build-chain-integrity",
        "target_count": target_count,
        "counts": {
            "final_records": len(final_records),
            "build_batches": len(batch_ids),
            "work_orders": len(work_order_ids),
            "packets": len(packet_ids),
            "batch_results": len(result_ids),
            "promotion_ready": len(ready_ids),
            "promoted": len(promoted_ids),
            "batch_record_ids": len(batch_record_ids),
            "work_order_record_ids": len(work_order_record_ids),
            "packet_record_ids": len(packet_record_ids),
            "dry_run_records_checked": result_record_count,
            "promotion_ready_records": ready_record_count,
            "promoted_records": promoted_record_count,
        },
        "artifact_paths": {
            "final_jsonl": "final-kits.jsonl",
            "build_batches": "build-batches.json",
            "build_work_orders": "build-work-orders.jsonl",
            "build_inputs_index": "build-inputs/index.json",
            "batch_results_index": "batch-results/index.json",
            "promotion_index": "build-promotion-index.json",
            "promoted_index": "promoted-batches/index.json",
        },
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    records = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return records


def _flatten_record_ids(records: Iterable[Dict[str, Any]]) -> List[str]:
    output = []
    for record in records:
        output.extend(str(record_id) for record_id in record.get("record_ids", []))
    return output


def _packet_record_ids(run_dir: Path, packets: List[Dict[str, Any]]) -> Tuple[List[str], List[Dict[str, Any]]]:
    record_ids = []
    failures = []
    for packet in packets:
        batch_id = str(packet.get("batch_id", ""))
        kit_records_path = _resolve_artifact_path(run_dir, str(packet.get("kit_records_jsonl", "")))
        work_order_path = _resolve_artifact_path(run_dir, str(packet.get("work_order_json", "")))
        packet_report_path = _resolve_artifact_path(run_dir, str(packet.get("packet_report_json", "")))
        missing = [
            str(path)
            for path in [kit_records_path, work_order_path, packet_report_path]
            if not path.exists()
        ]
        if missing:
            failures.append({"batch_id": batch_id, "missing": missing})
            continue
        line_count, malformed = count_jsonl(kit_records_path)
        packet_records = _read_jsonl(kit_records_path)
        packet_record_ids = [str(record.get("record_id", "")) for record in packet_records]
        record_ids.extend(packet_record_ids)
        expected = int(packet.get("record_count", 0))
        if line_count != expected or malformed != 0 or len(packet_record_ids) != expected or not packet.get("ok"):
            failures.append(
                {
                    "batch_id": batch_id,
                    "line_count": line_count,
                    "malformed": malformed,
                    "expected_record_count": expected,
                    "actual_record_count": len(packet_record_ids),
                    "packet_ok": packet.get("ok"),
                }
            )
    return record_ids, failures


def _promotion_report_failures(run_dir: Path, promoted_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    failures = []
    for entry in promoted_entries:
        batch_id = str(entry.get("batch_id", ""))
        report_path = run_dir / "promoted-batches" / batch_id / "promotion-report.json"
        if not report_path.exists():
            failures.append({"batch_id": batch_id, "missing": str(report_path)})
            continue
        report = _read_json(report_path)
        if not report.get("ok") or report.get("record_count") != entry.get("record_count"):
            failures.append(
                {
                    "batch_id": batch_id,
                    "report_ok": report.get("ok"),
                    "entry_record_count": entry.get("record_count"),
                    "report_record_count": report.get("record_count"),
                }
            )
    return failures


def _resolve_artifact_path(run_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.exists() or path.is_absolute():
        return path
    return run_dir / path


def _coverage_check(
    name: str,
    requirement: str,
    final_id_set: set[str],
    ids: List[str],
    upstream_ok: Any,
    extra: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    counts = Counter(ids)
    duplicate_ids = sorted(record_id for record_id, count in counts.items() if count > 1)
    id_set = set(ids)
    missing = sorted(final_id_set - id_set)
    extra_ids = sorted(id_set - final_id_set)
    evidence = {
        "upstream_ok": bool(upstream_ok),
        "record_ids": len(ids),
        "unique_record_ids": len(id_set),
        "missing": len(missing),
        "extra": len(extra_ids),
        "duplicates": len(duplicate_ids),
        "missing_sample": missing[:20],
        "extra_sample": extra_ids[:20],
        "duplicate_sample": duplicate_ids[:20],
    }
    if extra:
        evidence.update(extra)
    return _check(
        name,
        requirement,
        bool(upstream_ok) and not missing and not extra_ids and not duplicate_ids and len(ids) == len(final_id_set),
        evidence,
    )


def _same_multiset(reference: List[str], *others: List[str]) -> bool:
    reference_counts = Counter(reference)
    return all(Counter(other) == reference_counts for other in others)


def _batch_id_mismatches(named_ids: Dict[str, List[str]], reference: List[str]) -> Dict[str, Any]:
    reference_counts = Counter(reference)
    mismatches = {}
    for name, ids in named_ids.items():
        counts = Counter(ids)
        missing = sorted((reference_counts - counts).elements())
        extra = sorted((counts - reference_counts).elements())
        if missing or extra:
            mismatches[name] = {"missing": missing[:20], "extra": extra[:20]}
    return mismatches


def _duplicates(values: List[str]) -> List[str]:
    counts = Counter(values)
    return sorted(value for value, count in counts.items() if count > 1)


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
