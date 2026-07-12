from __future__ import annotations

from typing import Dict


TEMPERATURE_SCHEDULE: Dict[str, float] = {
    "expand": 1.4,
    "relevance_check": 0.1,
    "reveal": 0.9,
    "reduce": 0.45,
    "domain_merge_review": 0.1,
    "kit_merge_review": 0.1,
    "slot_decision": 0.1,
}


def temperature_for(stage: str) -> float:
    try:
        return TEMPERATURE_SCHEDULE[stage]
    except KeyError as exc:
        raise ValueError(f"unknown temperature stage: {stage}") from exc
