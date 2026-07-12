from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Tuple


def apply_kit_canonicalization(
    records: List[Dict[str, Any]],
    merge_report: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    groups = _review_groups(records, merge_report)
    output = []
    canonicalized = 0
    preserved = 0
    for record in records:
        next_record = deepcopy(record)
        payload = next_record.setdefault("payload", {})
        record_id = str(next_record.get("record_id", ""))
        group = groups.get(record_id)
        if group:
            payload["canonical_kit_record_id"] = group["canonical_record_id"]
            payload["canonical_kit_name"] = group["canonical_name"]
            payload["kit_alias_record_ids"] = group["record_ids"]
            payload["kit_alias_names"] = group["names"]
            payload["kit_merge_group_size"] = len(group["record_ids"])
            payload["kit_merge_evidence"] = {
                "reviewed_same_pairs": group["reviewed_same_pairs"][:16],
                "needs": group["needs"][:8],
                "dependencies": group["dependencies"][:8],
            }
            canonicalized += 1
        else:
            payload["canonical_kit_record_id"] = record_id
            payload["canonical_kit_name"] = payload.get("name", "")
            payload["kit_alias_record_ids"] = [record_id] if record_id else []
            payload["kit_alias_names"] = [payload.get("name", "")] if payload.get("name") else []
            payload["kit_merge_group_size"] = 1
            payload["kit_merge_evidence"] = {
                "reviewed_same_pairs": [],
                "needs": [payload.get("need", "")] if payload.get("need") else [],
                "dependencies": payload.get("requires", [])[:8],
            }
            preserved += 1
        output.append(next_record)
    report = {
        "stage": "kit-canonicalization",
        "input_records": len(records),
        "output_records": len(output),
        "canonicalized_records": canonicalized,
        "preserved_records": preserved,
        "canonical_groups": len({group["canonical_record_id"] for group in groups.values()}),
        "reviewed_same_pairs": merge_report.get("same_pairs", 0),
        "merge_rule": "apply reviewed same-kit groups while preserving aliases, needs, dependencies, and source ids",
        "ok": len(output) == len(records),
    }
    return output, report


def _review_groups(
    records: List[Dict[str, Any]],
    merge_report: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    by_id = {str(record.get("record_id", "")): record for record in records}
    parent = {record_id: record_id for record_id in by_id}
    reviewed_pairs: Dict[str, List[str]] = {record_id: [] for record_id in by_id}

    def find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        if left not in parent or right not in parent:
            return
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for review in merge_report.get("reviews", []):
        if review.get("same") != "Y":
            continue
        left = str(review.get("left_record_id", ""))
        right = str(review.get("right_record_id", ""))
        union(left, right)
        if left in reviewed_pairs:
            reviewed_pairs[left].append(right)
        if right in reviewed_pairs:
            reviewed_pairs[right].append(left)

    members_by_root: Dict[str, List[str]] = {}
    for record_id in by_id:
        members_by_root.setdefault(find(record_id), []).append(record_id)

    groups = {}
    for members in members_by_root.values():
        canonical_id = _canonical_record_id(members, by_id)
        names = _unique(by_id[record_id]["payload"].get("name", "") for record_id in members)
        needs = _unique(by_id[record_id]["payload"].get("need", "") for record_id in members)
        dependencies = _unique(
            dep for record_id in members for dep in by_id[record_id]["payload"].get("requires", [])
        )
        reviewed_same_pairs = [
            f"{record_id}->{linked_id}"
            for record_id in members
            for linked_id in reviewed_pairs.get(record_id, [])
        ]
        group = {
            "canonical_record_id": canonical_id,
            "canonical_name": by_id[canonical_id]["payload"].get("name", ""),
            "record_ids": sorted(members),
            "names": names,
            "needs": needs,
            "dependencies": dependencies,
            "reviewed_same_pairs": sorted(set(reviewed_same_pairs)),
        }
        for record_id in members:
            groups[record_id] = group
    return groups


def _canonical_record_id(record_ids: List[str], by_id: Dict[str, Dict[str, Any]]) -> str:
    return sorted(
        record_ids,
        key=lambda record_id: (
            by_id[record_id]["payload"].get("canonical_domain_path", ""),
            by_id[record_id]["payload"].get("name", ""),
            record_id,
        ),
    )[0]


def _unique(values: Any) -> List[str]:
    seen = set()
    output = []
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output
