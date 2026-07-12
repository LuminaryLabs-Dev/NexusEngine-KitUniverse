from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.fractal_kit_pipeline.run_artifacts import count_jsonl, write_json
from workflow_harnesses.fractal_kit_pipeline.simulator_slot_smoke import REQUIRED_SLOTS


def run_build_batch_dry_run(run_dir: Path, packets: List[Dict[str, Any]]) -> Dict[str, Any]:
    results_root = run_dir / "batch-results"
    batch_reports = []
    failed_batches = []
    records_checked = 0

    for packet in packets:
        batch_report = _run_packet(results_root, packet)
        batch_reports.append(batch_report)
        records_checked += batch_report["records_checked"]
        if not batch_report["ok"]:
            failed_batches.append(batch_report)

    write_json(results_root / "index.json", {"batches": batch_reports})
    checks = [
        _check(
            "one-result-per-packet",
            "each build input packet has exactly one dry-run build result",
            len(batch_reports) == len(packets),
            {"batch_results": len(batch_reports), "packets": len(packets)},
        ),
        _check(
            "all-packet-records-checked",
            "dry-run checked every record materialized in packets",
            records_checked == sum(packet.get("record_count", 0) for packet in packets),
            {
                "records_checked": records_checked,
                "packet_records": sum(packet.get("record_count", 0) for packet in packets),
            },
        ),
        _check(
            "zero-failed-batches",
            "every dry-run batch passed slot, idempotent, atomic, and renderer-boundary checks",
            not failed_batches,
            {"failed_batch_count": len(failed_batches), "sample": _failure_sample(failed_batches)},
        ),
    ]
    report = {
        "ok": all(check["ok"] for check in checks),
        "stage": "build-batch-dry-run",
        "results_root": str(results_root),
        "index_json": str(results_root / "index.json"),
        "batch_count": len(batch_reports),
        "packet_count": len(packets),
        "records_checked": records_checked,
        "failed_batch_count": len(failed_batches),
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }
    return report


def _run_packet(results_root: Path, packet: Dict[str, Any]) -> Dict[str, Any]:
    batch_id = packet["batch_id"]
    records = _read_jsonl(Path(packet["kit_records_jsonl"]))
    line_count, malformed = count_jsonl(Path(packet["kit_records_jsonl"]))
    record_checks = [_check_record(record) for record in records]
    failed_records = [check for check in record_checks if not check["ok"]]
    report = {
        "ok": (
            line_count == packet.get("record_count", 0)
            and malformed == 0
            and not failed_records
            and bool(packet.get("ok"))
        ),
        "stage": "batch-build-dry-run",
        "batch_id": batch_id,
        "packet_id": packet.get("packet_id"),
        "source_packet": packet.get("packet_dir"),
        "source_kit_records_jsonl": packet.get("kit_records_jsonl"),
        "records_checked": len(records),
        "line_count": line_count,
        "malformed": malformed,
        "failed_record_count": len(failed_records),
        "failed_records": failed_records[:20],
        "summary": {
            "domain_count": len({record.get("payload", {}).get("domain_path") for record in records}),
            "requires_count": len(
                {
                    required
                    for record in records
                    for required in record.get("payload", {}).get("requires", [])
                }
            ),
            "provides_count": len(
                {
                    provided
                    for record in records
                    for provided in record.get("payload", {}).get("provides", [])
                }
            ),
        },
        "resume_hint": "This batch can advance to a real builder from batch-results/<batch-id>/build-report.json.",
    }
    batch_dir = results_root / batch_id
    write_json(batch_dir / "build-report.json", report)
    return report


def _check_record(record: Dict[str, Any]) -> Dict[str, Any]:
    payload = record.get("payload", {})
    missing = [slot for slot in REQUIRED_SLOTS if not payload.get(slot)]
    renderer_owned = any(
        payload.get("renderer_boundary", {}).get(key)
        for key in ["ownsDom", "ownsCanvas", "ownsThreeObjects"]
    )
    ok = not missing and bool(payload.get("idempotent")) and bool(payload.get("atomic")) and not renderer_owned
    return {
        "ok": ok,
        "record_id": record.get("record_id"),
        "name": payload.get("name"),
        "missing": missing,
        "idempotent": bool(payload.get("idempotent")),
        "atomic": bool(payload.get("atomic")),
        "renderer_owned": renderer_owned,
    }


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    records = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return records


def _failure_sample(failed_batches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "batch_id": batch.get("batch_id"),
            "records_checked": batch.get("records_checked"),
            "failed_record_count": batch.get("failed_record_count"),
            "malformed": batch.get("malformed"),
        }
        for batch in failed_batches[:20]
    ]


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
