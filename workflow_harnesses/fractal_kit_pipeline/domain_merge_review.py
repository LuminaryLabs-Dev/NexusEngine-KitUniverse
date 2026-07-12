from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Tuple

from kituniverse_harness.smart_router import SmartRoutingService
from workflow_harnesses.fractal_kit_pipeline.temperature_schedule import temperature_for


async def run_recursive_domain_merge_review(
    router: SmartRoutingService,
    records: List[Dict[str, Any]],
    review_pairs: int,
    review_depth: int,
    concurrency: int,
    provider_retries: int,
) -> Dict[str, Any]:
    domain_index = _domain_index(records)
    pairs = _candidate_pairs(domain_index, review_pairs, review_depth)
    semaphore = asyncio.Semaphore(concurrency)

    async def review_one(pair: Tuple[int, int, int]) -> Dict[str, Any]:
        depth, left_index, right_index = pair
        left = domain_index[left_index]
        right = domain_index[right_index]
        prompt = (
            f"A: {left['domain_path']} needs {left['needs_sample']}\n"
            f"B: {right['domain_path']} needs {right['needs_sample']}\n"
            "Q: same reusable domain area? Y or N."
        )
        async with semaphore:
            response, attempts = await router.chat(
                [{"role": "system", "content": "Reply only Y or N."}, {"role": "user", "content": prompt}],
                temperature=temperature_for("domain_merge_review"),
                max_tokens=2,
                retries=provider_retries,
            )
        verdict = _clean_yes_no(response.content)
        return {
            "depth": depth,
            "left_domain": left["domain_path"],
            "right_domain": right["domain_path"],
            "same": verdict,
            "raw": response.content,
            "attempts": attempts,
        }

    reviews = await asyncio.gather(*(review_one(pair) for pair in pairs))
    groups = _merge_groups(domain_index, reviews)
    return {
        "stage": "recursive-domain-merge-review",
        "review_depth": review_depth,
        "pairs_requested": review_pairs,
        "pairs_reviewed": len(reviews),
        "domain_count": len(domain_index),
        "canonical_group_count": len(groups),
        "same_pairs": sum(1 for review in reviews if review["same"] == "Y"),
        "groups": groups,
        "reviews": reviews,
        "merge_rule": "merge similar domain areas only when the Y/N reviewer agrees; preserve aliases and source records",
    }


def _domain_index(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_domain: Dict[str, Dict[str, Any]] = {}
    for record in records:
        payload = record["payload"]
        domain_path = payload["domain_path"]
        entry = by_domain.setdefault(
            domain_path,
            {
                "domain_path": domain_path,
                "domain": payload["domain"],
                "parent_domain": payload.get("parent_domain", ""),
                "category": payload.get("category", ""),
                "record_ids": [],
                "needs": [],
                "dependencies": [],
                "aliases": set(),
            },
        )
        entry["record_ids"].append(record["record_id"])
        entry["needs"].append(payload.get("need", ""))
        entry["dependencies"].extend(payload.get("requires", []))
        entry["aliases"].update(payload.get("aliases", []))
    domains = []
    for entry in by_domain.values():
        needs = _unique(entry["needs"])
        dependencies = _unique(entry["dependencies"])
        domains.append(
            {
                "domain_path": entry["domain_path"],
                "domain": entry["domain"],
                "parent_domain": entry["parent_domain"],
                "category": entry["category"],
                "record_ids": entry["record_ids"],
                "record_count": len(entry["record_ids"]),
                "needs": needs,
                "dependencies": dependencies,
                "aliases": sorted(entry["aliases"]),
                "needs_sample": ", ".join(needs[:4]),
            }
        )
    return sorted(domains, key=lambda item: (item["parent_domain"], item["domain_path"]))


def _candidate_pairs(
    domains: List[Dict[str, Any]],
    review_pairs: int,
    review_depth: int,
) -> List[Tuple[int, int, int]]:
    by_parent: Dict[str, List[int]] = {}
    for index, domain in enumerate(domains):
        by_parent.setdefault(domain["parent_domain"], []).append(index)
    pairs: List[Tuple[int, int, int]] = []
    for depth in range(1, review_depth + 1):
        for indexes in by_parent.values():
            if len(indexes) < 2:
                continue
            step = max(1, depth)
            for offset in range(0, len(indexes) - step, step + 1):
                pairs.append((depth, indexes[offset], indexes[offset + step]))
                if len(pairs) >= review_pairs:
                    return pairs
    return pairs[:review_pairs]


def _merge_groups(domains: List[Dict[str, Any]], reviews: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    parent: Dict[str, str] = {domain["domain_path"]: domain["domain_path"] for domain in domains}

    def find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        root_left = find(left)
        root_right = find(right)
        if root_left != root_right:
            parent[root_right] = root_left

    for review in reviews:
        if review["same"] == "Y":
            union(review["left_domain"], review["right_domain"])

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for domain in domains:
        grouped.setdefault(find(domain["domain_path"]), []).append(domain)

    groups = []
    for members in grouped.values():
        members = sorted(members, key=lambda item: (-item["record_count"], item["domain_path"]))
        canonical = members[0]
        aliases = _unique(alias for member in members for alias in [member["domain_path"], *member["aliases"]])
        groups.append(
            {
                "canonical_domain": canonical["domain_path"],
                "aliases": aliases,
                "source_domain_count": len(members),
                "source_record_count": sum(member["record_count"] for member in members),
                "source_record_ids": [
                    record_id for member in members for record_id in member["record_ids"][:8]
                ][:64],
                "needs": _unique(need for member in members for need in member["needs"])[:24],
                "dependencies": _unique(dep for member in members for dep in member["dependencies"])[:24],
            }
        )
    return sorted(groups, key=lambda item: (-item["source_record_count"], item["canonical_domain"]))


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


def _clean_yes_no(value: str) -> str:
    value = value.strip().upper()
    if value.startswith("Y"):
        return "Y"
    return "N"
