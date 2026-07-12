from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from kituniverse_harness.smart_router import SmartRoutingService
from workflow_harnesses.fractal_kit_pipeline.expansion_stage import clean_item
from workflow_harnesses.fractal_kit_pipeline.kit_contract import GENERIC_FILLER, key
from workflow_harnesses.fractal_kit_pipeline.temperature_schedule import temperature_for


DEFAULT_SYSTEM = "Reply only with the requested short output."
REDUCE_GENERIC_FILLER = GENERIC_FILLER | {
    "reuse",
    "reusable",
    "reusability",
    "reusable-module",
    "reusable-data",
    "generic",
    "capability",
    "module",
}


async def run_reveal_reduce_stage(
    router: SmartRoutingService,
    source: str,
    list_goal: str,
    expansion_points: List[Dict[str, Any]],
    max_tokens: int,
    provider_retries: int,
    concurrency: int,
) -> List[Dict[str, Any]]:
    semaphore = asyncio.Semaphore(concurrency)

    async def run_one(record: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            point = record["value"]
            reveal_prompt = (
                f"SOURCE: {source}\n"
                f"POINT: {point}\n"
                "TASK: what reusable information is revealed?\n"
                f"RETURN: one short phrase <= {max_tokens} tokens."
            )
            reveal, reveal_attempts = await router.chat(
                [{"role": "system", "content": DEFAULT_SYSTEM}, {"role": "user", "content": reveal_prompt}],
                temperature=temperature_for("reveal"),
                max_tokens=max_tokens,
                retries=provider_retries,
            )
            reveal_value = clean_item(reveal.content) or f"{point} state"
            reduce_prompt = (
                f'LIST GOAL: {{"{list_goal}"}}\n'
                f"POINT: {point}\n"
                f"REVEALS: {reveal_value}\n"
                "TASK: reduce to reusable capability name.\n"
                "RULE: preserve a specific word from POINT or REVEALS; do not answer reusable.\n"
                f"RETURN: one short phrase <= {max_tokens} tokens."
            )
            reduced, reduce_attempts = await router.chat(
                [{"role": "system", "content": DEFAULT_SYSTEM}, {"role": "user", "content": reduce_prompt}],
                temperature=temperature_for("reduce"),
                max_tokens=max_tokens,
                retries=provider_retries,
            )
            reduced_value = clean_item(reduced.content) or point
            if key(reduced_value) in REDUCE_GENERIC_FILLER:
                reduced_value = point
            return {
                "stage": "revealed-reduced",
                "source_index": record["index"],
                "expansion_point": point,
                "revealed": reveal_value,
                "reduced": reduced_value,
                "reveal_attempts": reveal_attempts,
                "reduce_attempts": reduce_attempts,
                "reveal_raw": reveal.content,
                "reduce_raw": reduced.content,
                "ok": bool(reveal.ok and reduced.ok),
            }

    return await asyncio.gather(*(run_one(record) for record in expansion_points))
