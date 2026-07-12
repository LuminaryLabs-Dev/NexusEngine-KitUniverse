from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.guided_kit_builder.contract import compare_and_inject, validate_guided_kit


def commit_universe_turn(
    universe_root: Path,
    record: Dict[str, Any],
    run_report: Dict[str, Any],
    target: int,
) -> Dict[str, Any]:
    universe_root.mkdir(parents=True, exist_ok=True)
    kits_path = universe_root / "kits.jsonl"
    links_path = universe_root / "links.jsonl"
    turns_path = universe_root / "turns.jsonl"
    reviews_path = universe_root / "cli-reviews.jsonl"
    existing = _read_jsonl(kits_path)
    record_id = str(record.get("record_id", ""))
    signature = _semantic_signature(record)
    exact_duplicate = next((item for item in existing if item.get("record_id") == record_id), None)
    semantic_duplicate = next(
        (item for item in existing if _semantic_signature(item) == signature),
        None,
    )
    if exact_duplicate or (semantic_duplicate and validate_guided_kit(semantic_duplicate)["ok"]):
        duplicate = exact_duplicate or semantic_duplicate
        return {
            "ok": False,
            "committed": False,
            "reason": "duplicate-kit",
            "duplicate_record_id": duplicate.get("record_id"),
            "count": len(existing),
            "target": target,
        }
    record = deepcopy(record)
    if semantic_duplicate:
        record.setdefault("payload", {}).setdefault("source_evidence", {})[
            "supersedes_record_id"
        ] = semantic_duplicate.get("record_id")

    all_kits = [*existing, record]
    audit = _promotion_audit(all_kits)
    current_validation = next(
        item for item in audit["records"] if item["record_id"] == record_id
    )
    if not current_validation["promotion_ready"]:
        return {
            "ok": False,
            "committed": False,
            "reason": "current-validator-rejected-kit",
            "validation": current_validation,
            "count": len(existing),
            "target": target,
        }
    promotion_ready_ids = set(audit["promotion_ready_record_ids"])
    promotion_ready_records = [
        item for item in all_kits if item.get("record_id") in promotion_ready_ids
    ]
    _, all_links = compare_and_inject(promotion_ready_records)
    new_links = [
        link
        for link in all_links
        if record_id in {link.get("source_kit"), link.get("target_kit")}
    ]
    connection_quality = _connection_quality(record, new_links)
    efficiency = _efficiency_report(run_report)
    _write_jsonl_atomic(kits_path, all_kits)
    _write_jsonl_atomic(links_path, all_links)
    _write_json_atomic(universe_root / "promotion-audit.json", audit)
    turn_number = len(existing) + 1
    turn = {
        "turn": turn_number,
        "record_id": record_id,
        "run_id": run_report.get("run_id"),
        "run_dir": run_report.get("run_dir"),
        "model_calls": run_report.get("model_calls"),
        "repair_calls": run_report.get("repair_calls"),
        "new_links": len(new_links),
        "connection_quality": connection_quality,
        "efficiency": efficiency,
        "promotion_ready": True,
        "target": target,
    }
    review = {
        "turn": turn_number,
        "command": "guided-kit-builder --universe-turn",
        "checks": {
            "one_kit_only": True,
            "duplicate_gate": True,
            "validation_gate": True,
            "single_active_prediction": run_report.get("concurrency_gate") is True,
            "immutable_ledger_commit": True,
            "connection_quality_reported": True,
            "token_latency_reported": True,
        },
        "upgrade_policy": "review every turn; apply code changes only between completed turns",
        "next_review_focus": _next_review_focus(turn_number),
    }
    _append_jsonl_atomic(turns_path, [turn])
    _append_jsonl_atomic(reviews_path, [review])
    manifest = reconcile_universe_manifest(universe_root, target)
    return {
        "ok": True,
        "committed": True,
        "turn": turn_number,
        "count": manifest["completed"],
        "raw_committed": manifest["raw_committed"],
        "promotion_ready": manifest["completed"],
        "quarantined": manifest["quarantined"],
        "target": target,
        "remaining": manifest["remaining"],
        "new_links": len(new_links),
        "connection_quality": connection_quality,
        "efficiency": efficiency,
        "manifest": str(universe_root / "manifest.json"),
        "cli_review": review,
    }


def reconcile_universe_manifest(universe_root: Path, target: int = 1000) -> Dict[str, Any]:
    kits_path = universe_root / "kits.jsonl"
    links_path = universe_root / "links.jsonl"
    turns_path = universe_root / "turns.jsonl"
    reviews_path = universe_root / "cli-reviews.jsonl"
    records = _read_jsonl(kits_path)
    batch_quarantine = _read_jsonl(universe_root / "batch-quarantine.jsonl")
    audit = _promotion_audit(records)
    promotion_ready_ids = set(audit["promotion_ready_record_ids"])
    promotion_ready_records = [
        record for record in records if record.get("record_id") in promotion_ready_ids
    ]
    _, links = compare_and_inject(promotion_ready_records)
    _write_jsonl_atomic(links_path, links)
    _write_json_atomic(universe_root / "promotion-audit.json", audit)
    completed = audit["promotion_ready"]
    manifest = {
        "name": "KitUniverse-1000",
        "target": target,
        "completed": completed,
        "promotion_ready": completed,
        "raw_committed": len(records),
        "quarantined": audit["quarantined"],
        "batch_quarantined": len(batch_quarantine),
        "total_quarantined": audit["quarantined"] + len(batch_quarantine),
        "remaining": max(0, target - completed),
        "last_record_id": records[-1].get("record_id") if records else None,
        "last_promotion_ready_record_id": (
            promotion_ready_records[-1].get("record_id") if promotion_ready_records else None
        ),
        "kits_jsonl": str(kits_path),
        "links_jsonl": str(links_path),
        "turns_jsonl": str(turns_path),
        "cli_reviews_jsonl": str(reviews_path),
        "promotion_audit": str(universe_root / "promotion-audit.json"),
    }
    _write_json_atomic(universe_root / "manifest.json", manifest)
    return manifest


def _promotion_audit(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    entries = []
    for record in records:
        validation = validate_guided_kit(record)
        entries.append(
            {
                "record_id": record.get("record_id"),
                "promotion_ready": validation["ok"],
                "errors": validation["errors"],
            }
        )
    promotion_ready_ids = [
        item["record_id"] for item in entries if item["promotion_ready"]
    ]
    return {
        "raw_committed": len(records),
        "promotion_ready": len(promotion_ready_ids),
        "quarantined": len(records) - len(promotion_ready_ids),
        "promotion_ready_record_ids": promotion_ready_ids,
        "records": entries,
    }


def _semantic_signature(record: Dict[str, Any]) -> str:
    payload = record.get("payload") or {}
    parts = [payload.get("domain"), payload.get("name"), payload.get("purpose")]
    return "|".join(str(part or "").strip().lower() for part in parts)


def _connection_quality(record: Dict[str, Any], links: List[Dict[str, Any]]) -> Dict[str, Any]:
    record_id = str(record.get("record_id", ""))
    payload = record.get("payload") or {}
    incoming = [link for link in links if link.get("target_kit") == record_id]
    outgoing = [link for link in links if link.get("source_kit") == record_id]
    incoming_tokens = sorted({str(link.get("token", "")) for link in incoming})
    outgoing_tokens = sorted({str(link.get("token", "")) for link in outgoing})
    return {
        "validated": all(link.get("status") == "validated" for link in links),
        "reference_only": all(
            link.get("mutation_policy") == "reference-only; source kits remain immutable"
            for link in links
        ),
        "validated_link_count": len(links),
        "incoming_count": len(incoming),
        "incoming_tokens": incoming_tokens,
        "incoming_source_kits": sorted(
            {str(link.get("source_kit", "")) for link in incoming}
        ),
        "outgoing_count": len(outgoing),
        "outgoing_tokens": outgoing_tokens,
        "outgoing_target_kits": sorted(
            {str(link.get("target_kit", "")) for link in outgoing}
        ),
        "unmatched_requires": sorted(set(payload.get("requires", [])) - set(incoming_tokens)),
        "unmatched_provides": sorted(set(payload.get("provides", [])) - set(outgoing_tokens)),
    }


def _efficiency_report(run_report: Dict[str, Any]) -> Dict[str, Any]:
    usage = run_report.get("usage") or {}
    review = run_report.get("codex_cli_review") or {}
    model_calls = int(run_report.get("model_calls") or 0)
    generation_seconds = float(run_report.get("elapsed_seconds") or 0.0)
    review_seconds = float(review.get("elapsed_seconds") or 0.0)
    total_tokens = int(usage.get("total_tokens") or 0)
    return {
        "latency_seconds": {
            "generation": round(generation_seconds, 3),
            "codex_review": round(review_seconds, 3),
            "observed_total": round(generation_seconds + review_seconds, 3),
            "repair_delay": float(run_report.get("repair_delay_seconds_total") or 0.0),
        },
        "lm_studio_tokens": {
            "prompt": int(usage.get("prompt_tokens") or 0),
            "completion": int(usage.get("completion_tokens") or 0),
            "total": total_tokens,
            "per_model_call": round(total_tokens / model_calls, 3) if model_calls else 0.0,
            "per_generation_second": (
                round(total_tokens / generation_seconds, 3) if generation_seconds else 0.0
            ),
        },
        "model_calls": model_calls,
        "repair_calls": int(run_report.get("repair_calls") or 0),
        "scope": "LM Studio token usage; Codex review latency only because Sol token usage is not structured",
    }


def _next_review_focus(turn: int) -> str:
    focuses = [
        "error messages and recovery",
        "ledger inspection ergonomics",
        "duplicate diagnostics",
        "connection quality reporting",
        "token and latency reporting",
    ]
    return focuses[(turn - 1) % len(focuses)]


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError(f"malformed JSONL at {path}:{line_number}: {error}") from error
    return rows


def _append_jsonl_atomic(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    _write_jsonl_atomic(path, [*_read_jsonl(path), *rows])


def _write_jsonl_atomic(path: Path, rows: List[Dict[str, Any]]) -> None:
    content = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    _replace_atomic(path, content)


def _write_json_atomic(path: Path, value: Dict[str, Any]) -> None:
    _replace_atomic(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def _replace_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)
