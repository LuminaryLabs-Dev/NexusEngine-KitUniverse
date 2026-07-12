from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple


def build_final_bucket_reconciliation(
    final_jsonl: Path,
    final_bucket_run_dir: str,
    target_count: int,
) -> Dict[str, Any]:
    bucket_dir = Path(final_bucket_run_dir)
    final_ids, final_malformed = _read_record_ids(final_jsonl)
    shard_ids, shard_malformed, shard_files = _read_shard_record_ids(bucket_dir / "shards")
    final_counts = Counter(final_ids)
    shard_counts = Counter(shard_ids)
    final_duplicate_ids = sorted(record_id for record_id, count in final_counts.items() if count > 1)
    shard_duplicate_ids = sorted(record_id for record_id, count in shard_counts.items() if count > 1)
    missing_from_bucket = sorted(set(final_counts) - set(shard_counts))
    extra_in_bucket = sorted(set(shard_counts) - set(final_counts))
    count_mismatches = sorted(
        record_id
        for record_id in set(final_counts) & set(shard_counts)
        if final_counts[record_id] != shard_counts[record_id]
    )
    checks = [
        _check(
            "final-jsonl-readable",
            "final JSONL is readable and has no malformed records",
            len(final_ids) == target_count and final_malformed == 0,
            {"final_records": len(final_ids), "target_count": target_count, "malformed": final_malformed},
        ),
        _check(
            "bucket-shards-readable",
            "final bucket shards are readable and have no malformed records",
            len(shard_ids) == target_count and shard_malformed == 0,
            {
                "bucket_records": len(shard_ids),
                "target_count": target_count,
                "malformed": shard_malformed,
                "shard_files": shard_files,
            },
        ),
        _check(
            "no-duplicate-ids",
            "final JSONL and bucket shards have no duplicate record ids",
            not final_duplicate_ids and not shard_duplicate_ids,
            {
                "final_duplicate_ids": final_duplicate_ids[:50],
                "bucket_duplicate_ids": shard_duplicate_ids[:50],
            },
        ),
        _check(
            "exact-id-match",
            "bucket record ids exactly match final-kits.jsonl record ids",
            not missing_from_bucket and not extra_in_bucket and not count_mismatches,
            {
                "missing_from_bucket": missing_from_bucket[:50],
                "extra_in_bucket": extra_in_bucket[:50],
                "count_mismatches": count_mismatches[:50],
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "final-bucket-reconciliation",
        "target_count": target_count,
        "final_jsonl": str(final_jsonl),
        "final_bucket_run_dir": str(bucket_dir),
        "final_records": len(final_ids),
        "bucket_records": len(shard_ids),
        "final_malformed": final_malformed,
        "bucket_malformed": shard_malformed,
        "active_shards": shard_files,
        "final_duplicate_ids": len(final_duplicate_ids),
        "bucket_duplicate_ids": len(shard_duplicate_ids),
        "missing_from_bucket": len(missing_from_bucket),
        "extra_in_bucket": len(extra_in_bucket),
        "count_mismatches": len(count_mismatches),
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _read_shard_record_ids(shards_dir: Path) -> Tuple[List[str], int, int]:
    ids: List[str] = []
    malformed = 0
    shard_files = 0
    for path in sorted(shards_dir.glob("*.jsonl")):
        path_ids, path_malformed = _read_record_ids(path)
        ids.extend(path_ids)
        malformed += path_malformed
        if path_ids or path_malformed:
            shard_files += 1
    return ids, malformed, shard_files


def _read_record_ids(path: Path) -> Tuple[List[str], int]:
    ids: List[str] = []
    malformed = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            record_id = str(record.get("record_id", "")).strip()
            if record_id:
                ids.append(record_id)
            else:
                malformed += 1
    return ids, malformed


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
