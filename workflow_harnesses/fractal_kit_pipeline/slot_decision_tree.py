from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from kituniverse_harness.smart_router import SmartRoutingService
from workflow_harnesses.fractal_kit_pipeline.simulator_slot_smoke import REQUIRED_SLOTS
from workflow_harnesses.fractal_kit_pipeline.temperature_schedule import temperature_for


DEFAULT_SLOT_SYSTEM = "Reply only Y or N."


async def run_lfm_slot_decision_tree(
    router: SmartRoutingService,
    records: List[Dict[str, Any]],
    sample_size: int,
    concurrency: int,
    provider_retries: int,
) -> Dict[str, Any]:
    sample = records[: max(0, sample_size)]
    semaphore = asyncio.Semaphore(concurrency)

    async def validate_record(index: int, record: Dict[str, Any]) -> Dict[str, Any]:
        kit = record.get("payload", {})
        nodes = []
        accepted = True
        for slot in REQUIRED_SLOTS:
            prompt = _slot_prompt(kit, slot)
            async with semaphore:
                response, attempts = await router.chat(
                    [
                        {"role": "system", "content": DEFAULT_SLOT_SYSTEM},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=temperature_for("slot_decision"),
                    max_tokens=2,
                    retries=provider_retries,
                )
            verdict = _yes_no(response.content)
            node = {
                "slot": slot,
                "value_preview": _preview(kit.get(slot)),
                "verdict": verdict,
                "raw": response.content,
                "attempts": attempts,
                "prompt": prompt,
            }
            nodes.append(node)
            if verdict != "Y":
                accepted = False
                break
        return {
            "index": index,
            "record_id": record.get("record_id"),
            "name": kit.get("name"),
            "accepted": accepted,
            "nodes": nodes,
        }

    decisions = await asyncio.gather(*(validate_record(index, record) for index, record in enumerate(sample)))
    accepted_count = sum(1 for decision in decisions if decision["accepted"])
    return {
        "ok": accepted_count == len(sample),
        "stage": "lfm-slot-decision-tree",
        "records_requested": sample_size,
        "records_tested": len(sample),
        "records_accepted": accepted_count,
        "records_rejected": len(sample) - accepted_count,
        "slots": REQUIRED_SLOTS,
        "decision_tree": "record -> required slot -> Y/N gate -> continue or stop",
        "decisions": decisions,
    }


def _slot_prompt(kit: Dict[str, Any], slot: str) -> str:
    value = _preview(kit.get(slot))
    return (
        f"NAME: {kit.get('name', '')}\n"
        f"DOMAIN: {kit.get('domain_path', '')}\n"
        f"SLOT: {slot}\n"
        f"VALUE: {value}\n"
        "Q: slot is filled and reusable? Y or N."
    )


def _preview(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value[:4])[:120]
    if isinstance(value, dict):
        return ", ".join(f"{key}={val}" for key, val in list(value.items())[:4])[:120]
    return str(value or "")[:120]


def _yes_no(value: str) -> str:
    return "Y" if value.strip().upper().startswith("Y") else "N"
