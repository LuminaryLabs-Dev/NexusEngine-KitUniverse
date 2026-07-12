from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

from workflow_harnesses.fractal_kit_pipeline.run_artifacts import count_jsonl, write_json, write_jsonl


def write_build_batch_packets(
    run_dir: Path,
    final_records: List[Dict[str, Any]],
    work_orders: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    packets_root = run_dir / "build-inputs"
    records_by_id = {record["record_id"]: record for record in final_records}
    packets = []
    missing_record_refs = []
    malformed_packets = []
    total_packet_records = 0

    for order in work_orders:
        batch_id = str(order.get("batch_id"))
        packet_dir = packets_root / batch_id
        selected_records = []
        missing_ids = []
        for record_id in order.get("record_ids", []):
            record = records_by_id.get(record_id)
            if record is None:
                missing_ids.append(record_id)
                continue
            selected_records.append(record)

        kit_records_path = packet_dir / "kit-records.jsonl"
        work_order_path = packet_dir / "work-order.json"
        packet_report_path = packet_dir / "packet-report.json"
        write_jsonl(kit_records_path, selected_records)
        write_json(work_order_path, order)
        line_count, malformed = count_jsonl(kit_records_path)
        packet = {
            "packet_id": f"packet-{batch_id}",
            "batch_id": batch_id,
            "work_order_id": order.get("work_order_id"),
            "primary_dependency": order.get("primary_dependency"),
            "packet_dir": str(packet_dir),
            "kit_records_jsonl": str(kit_records_path),
            "work_order_json": str(work_order_path),
            "record_count": len(selected_records),
            "expected_record_count": order.get("record_count", 0),
            "line_count": line_count,
            "malformed": malformed,
            "missing_record_ids": missing_ids,
            "resume_hint": "Run one downstream worker from this packet directory without loading the full 10k JSONL.",
        }
        packet["ok"] = (
            len(selected_records) == order.get("record_count", 0)
            and line_count == len(selected_records)
            and malformed == 0
            and not missing_ids
        )
        write_json(packet_report_path, packet)
        packet["packet_report_json"] = str(packet_report_path)
        packets.append(packet)
        missing_record_refs.extend({"batch_id": batch_id, "record_id": record_id} for record_id in missing_ids)
        if malformed:
            malformed_packets.append({"batch_id": batch_id, "malformed": malformed})
        total_packet_records += len(selected_records)

    write_json(packets_root / "index.json", {"packets": packets})
    packet_ids = [packet["packet_id"] for packet in packets]
    duplicate_packet_ids = len(packet_ids) - len(set(packet_ids))
    checks = [
        _check(
            "one-packet-per-work-order",
            "each work order has exactly one materialized build input packet",
            len(packets) == len(work_orders),
            {"packet_count": len(packets), "work_order_count": len(work_orders)},
        ),
        _check(
            "unique-packet-ids",
            "packet ids are unique",
            duplicate_packet_ids == 0,
            {"duplicate_packet_ids": duplicate_packet_ids},
        ),
        _check(
            "all-records-materialized",
            "packets materialize every work-order record reference",
            total_packet_records == len(final_records) and not missing_record_refs,
            {
                "packet_records": total_packet_records,
                "final_records": len(final_records),
                "missing_record_refs": len(missing_record_refs),
            },
        ),
        _check(
            "packet-jsonl-safe",
            "every packet JSONL has the expected line count and zero malformed records",
            not malformed_packets and all(packet["ok"] for packet in packets),
            {"malformed_packets": malformed_packets, "failed_packets": _failed_packets(packets)},
        ),
    ]
    report = {
        "ok": all(check["ok"] for check in checks),
        "stage": "build-batch-packets",
        "packets_root": str(packets_root),
        "index_json": str(packets_root / "index.json"),
        "packet_count": len(packets),
        "work_order_count": len(work_orders),
        "record_count": len(final_records),
        "packet_records": total_packet_records,
        "duplicate_packet_ids": duplicate_packet_ids,
        "missing_record_refs": missing_record_refs[:200],
        "malformed_packets": malformed_packets[:200],
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }
    return packets, report


def _failed_packets(packets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "batch_id": packet.get("batch_id"),
            "record_count": packet.get("record_count"),
            "expected_record_count": packet.get("expected_record_count"),
            "line_count": packet.get("line_count"),
            "malformed": packet.get("malformed"),
            "missing_record_ids": packet.get("missing_record_ids", [])[:10],
        }
        for packet in packets
        if not packet.get("ok")
    ][:20]


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
