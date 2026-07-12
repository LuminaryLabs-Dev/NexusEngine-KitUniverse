from __future__ import annotations

import fcntl
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from workflow_harnesses.guided_kit_builder.contract import compare_and_inject, validate_guided_kit
from workflow_harnesses.guided_kit_builder.universe_ledger import (
    _promotion_audit,
    _read_jsonl,
    _write_json_atomic,
    _write_jsonl_atomic,
    reconcile_universe_manifest,
)


def canonical_signature(record: Dict[str, Any]) -> str:
    payload = record.get("payload") or {}
    value = {
        "domain": payload.get("domain"),
        "owned_state": sorted(payload.get("owned_state") or []),
        "requires": sorted(payload.get("requires") or []),
        "provides": sorted(payload.get("provides") or []),
    }
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


def duplicate_report(records: List[Dict[str, Any]], universe_root: Path) -> Dict[str, Any]:
    existing = _read_jsonl(universe_root / "kits.jsonl")
    seen_ids = {str(item.get("record_id", "")) for item in existing}
    seen_semantic = {str((item.get("payload") or {}).get("semantic_key", "")) for item in existing}
    seen_merge = {str((item.get("payload") or {}).get("merge_key", "")) for item in existing}
    seen_signatures = {canonical_signature(item) for item in existing}
    decisions = []
    for record in records:
        payload = record.get("payload") or {}
        reasons = []
        record_id = str(record.get("record_id", ""))
        semantic_key = str(payload.get("semantic_key", ""))
        merge_key = str(payload.get("merge_key", ""))
        signature = canonical_signature(record)
        if record_id in seen_ids:
            reasons.append("duplicate-record-id")
        if semantic_key and semantic_key in seen_semantic:
            reasons.append("duplicate-semantic-key")
        if merge_key and merge_key in seen_merge:
            reasons.append("duplicate-merge-key")
        if signature in seen_signatures:
            reasons.append("duplicate-canonical-contract")
        decisions.append({"record_id": record_id, "ok": not reasons, "reasons": reasons})
        seen_ids.add(record_id)
        seen_semantic.add(semantic_key)
        seen_merge.add(merge_key)
        seen_signatures.add(signature)
    return {
        "ok": all(item["ok"] for item in decisions),
        "existing_count": len(existing),
        "candidate_count": len(records),
        "accepted": sum(item["ok"] for item in decisions),
        "rejected": sum(not item["ok"] for item in decisions),
        "decisions": decisions,
    }


def append_quarantine(universe_root: Path, entries: Iterable[Dict[str, Any]]) -> None:
    path = universe_root / "batch-quarantine.jsonl"
    existing = _read_jsonl(path)
    by_id = {str(item.get("quarantine_id", "")): item for item in existing}
    for entry in entries:
        by_id[str(entry["quarantine_id"])] = entry
    _write_jsonl_atomic(path, list(by_id.values()))


def commit_batch(
    universe_root: Path,
    records: List[Dict[str, Any]],
    run_id: str,
    batch_id: str,
    target: int,
) -> Dict[str, Any]:
    universe_root.mkdir(parents=True, exist_ok=True)
    lock_path = universe_root / ".batch.lock"
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        journal_path = universe_root / "batch-commits.jsonl"
        journal = _read_jsonl(journal_path)
        transaction_id = f"{run_id}:{batch_id}"
        committed = next(
            (
                item
                for item in journal
                if item.get("transaction_id") == transaction_id and item.get("state") == "committed"
            ),
            None,
        )
        if committed:
            return {"ok": True, "replayed": True, **committed}

        existing = _read_jsonl(universe_root / "kits.jsonl")
        existing_ids = {str(item.get("record_id", "")) for item in existing}
        accepted = [record for record in records if str(record.get("record_id", "")) not in existing_ids]
        if not accepted:
            return {
                "ok": True,
                "state": "no-op",
                "transaction_id": transaction_id,
                "run_id": run_id,
                "batch_id": batch_id,
                "record_ids": [],
                "promoted": 0,
            }
        invalid = [
            {"record_id": record.get("record_id"), "errors": validate_guided_kit(record)["errors"]}
            for record in accepted
            if not validate_guided_kit(record)["ok"]
        ]
        if invalid:
            return {"ok": False, "state": "rejected", "reason": "validator-rejected", "invalid": invalid}
        prepared = {
            "transaction_id": transaction_id,
            "state": "prepared",
            "run_id": run_id,
            "batch_id": batch_id,
            "record_ids": [record.get("record_id") for record in accepted],
        }
        journal.append(prepared)
        _write_jsonl_atomic(journal_path, journal)

        all_kits = [*existing, *accepted]
        audit = _promotion_audit(all_kits)
        ready_ids = set(audit["promotion_ready_record_ids"])
        ready_records = [record for record in all_kits if record.get("record_id") in ready_ids]
        _, links = compare_and_inject(ready_records)
        _write_jsonl_atomic(universe_root / "kits.jsonl", all_kits)
        _write_jsonl_atomic(universe_root / "links.jsonl", links)
        _write_json_atomic(universe_root / "promotion-audit.json", audit)
        manifest = reconcile_universe_manifest(universe_root, target)

        committed_entry = {
            **prepared,
            "state": "committed",
            "promoted": len(accepted),
            "promotion_ready": manifest["promotion_ready"],
            "remaining": manifest["remaining"],
        }
        journal.append(committed_entry)
        _write_jsonl_atomic(journal_path, journal)
        return {"ok": True, **committed_entry, "manifest": manifest}
