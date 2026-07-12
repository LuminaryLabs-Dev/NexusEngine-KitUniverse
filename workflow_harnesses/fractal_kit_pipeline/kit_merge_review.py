from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Tuple

from kituniverse_harness.smart_router import SmartRoutingService
from workflow_harnesses.fractal_kit_pipeline.temperature_schedule import temperature_for


async def run_recursive_kit_merge_review(
    router: SmartRoutingService,
    records: List[Dict[str, Any]],
    review_pairs: int,
    review_depth: int,
    provider_retries: int,
    concurrency: int,
) -> Dict[str, Any]:
    candidate_pairs = _candidate_pairs(records, review_pairs, review_depth)
    semaphore = asyncio.Semaphore(concurrency)

    async def review_one(pair: Tuple[int, int, int]) -> Dict[str, Any]:
        depth, left_index, right_index = pair
        left = records[left_index]["payload"]
        right = records[right_index]["payload"]
        prompt = (
            f"A: {left['name']} needs {left['need']}\n"
            f"B: {right['name']} needs {right['need']}\n"
            f"DEPS: {left['primary_dependency']} | {right['primary_dependency']}\n"
            "Q: same atomic reusable capability? Y or N."
        )
        async with semaphore:
            response, attempts = await router.chat(
                [{"role": "system", "content": "Reply only Y or N."}, {"role": "user", "content": prompt}],
                temperature=temperature_for("kit_merge_review"),
                max_tokens=2,
                retries=provider_retries,
            )
        return {
            "depth": depth,
            "left_record_id": records[left_index]["record_id"],
            "right_record_id": records[right_index]["record_id"],
            "left_name": left["name"],
            "right_name": right["name"],
            "same": _clean_yes_no(response.content),
            "raw": response.content,
            "attempts": attempts,
        }

    reviews = await asyncio.gather(*(review_one(pair) for pair in candidate_pairs))
    same_pairs = [review for review in reviews if review["same"] == "Y"]
    return {
        "stage": "recursive-kit-merge-review",
        "review_depth": review_depth,
        "pairs_requested": review_pairs,
        "pairs_reviewed": len(reviews),
        "same_pairs": len(same_pairs),
        "reviews": reviews,
        "merge_rule": "same needs and dependency pairs are reviewed, aliases preserved, target keeps unique record ids",
    }


def select_final_records(
    records: List[Dict[str, Any]],
    merge_report: Dict[str, Any],
    target_count: int,
) -> List[Dict[str, Any]]:
    same_ids = set()
    for review in merge_report.get("reviews", []):
        if review.get("same") == "Y":
            same_ids.add(review["right_record_id"])
    selected = []
    for record in records:
        if record["record_id"] in same_ids and len(records) - len(same_ids) >= target_count:
            continue
        selected.append(record)
        if len(selected) >= target_count:
            break
    return selected


def _candidate_pairs(
    records: List[Dict[str, Any]],
    review_pairs: int,
    review_depth: int,
) -> List[Tuple[int, int, int]]:
    by_key: Dict[str, List[int]] = {}
    for index, record in enumerate(records):
        key = record["payload"]["merge_key"]
        by_key.setdefault(key, []).append(index)
    pairs = []
    for depth in range(1, review_depth + 1):
        for indexes in by_key.values():
            if len(indexes) < 2:
                continue
            step = max(1, depth)
            for offset in range(0, len(indexes) - step, step + 1):
                pairs.append((depth, indexes[offset], indexes[offset + step]))
                if len(pairs) >= review_pairs:
                    return pairs
    if len(pairs) < review_pairs:
        for index in range(0, min(len(records) - 1, review_pairs - len(pairs))):
            pairs.append((review_depth, index, index + 1))
    return pairs[:review_pairs]


def _clean_yes_no(value: str) -> str:
    value = value.strip().upper()
    if value.startswith("Y"):
        return "Y"
    return "N"
