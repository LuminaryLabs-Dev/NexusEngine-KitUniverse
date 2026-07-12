from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from kituniverse_harness.smart_router import SmartRoutingService

from .contracts import MECHANIC_EVIDENCE_SCHEMA, slug, stable_hash


CAPABILITY_RULES = {
    "multiplayer": "multiplayer-session",
    "singleplayer": "single-player-session",
    "single-player": "single-player-session",
    "co-op": "cooperative-session",
    "coop": "cooperative-session",
    "open world": "open-world-state",
    "crafting": "crafting-recipe",
    "inventory": "inventory-state",
    "dialogue": "dialogue-choice",
    "choices matter": "branching-choice",
    "turn-based": "turn-order",
    "turn based": "turn-order",
    "procedural": "procedural-generation",
    "roguelike": "run-progression",
    "rogue-like": "run-progression",
    "stealth": "stealth-detection",
    "survival": "survival-resource-pressure",
    "fighting": "combat-resolution",
    "shooter": "projectile-combat",
    "racing": "race-progression",
    "puzzle": "puzzle-state",
    "platformer": "platforming-movement",
    "role-playing": "role-progression",
    "rpg": "role-progression",
    "strategy": "strategy-command",
    "simulation": "simulation-state",
    "physics": "physics-interaction",
    "virtual reality": "immersive-session",
    "vr": "immersive-session",
}
CAPABILITY_SLUG_RULES = {slug(key): value for key, value in CAPABILITY_RULES.items()}

MODEL_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "rawg_mechanic_evidence",
        "strict": "true",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "mechanics": {
                    "type": "array",
                    "maxItems": 6,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "capability_id": {"type": "string"},
                            "label": {"type": "string"},
                            "evidence_fields": {"type": "array", "items": {"type": "string"}},
                            "evidence_summary": {"type": "string"},
                        },
                        "required": ["capability_id", "label", "evidence_fields", "evidence_summary"],
                    },
                }
            },
            "required": ["mechanics"],
        },
    },
}


def deterministic_mechanics(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    fields = source.get("evidence_fields") or {}
    haystacks = {
        "description": str(fields.get("description") or "").lower(),
        "genres": " ".join(fields.get("genres") or []).lower(),
        "tags": " ".join(fields.get("tags") or []).lower(),
    }
    output = []
    seen = set()
    for phrase, capability in CAPABILITY_RULES.items():
        cited = [field for field, text in haystacks.items() if phrase in text]
        if not cited or capability in seen:
            continue
        seen.add(capability)
        output.append(
            {
                "capability_id": capability,
                "label": capability.replace("-", " ").title(),
                "evidence_fields": cited,
                "evidence_summary": f"{phrase} appears in {', '.join(cited)} metadata",
                "extractor": "deterministic",
            }
        )
    return output[:6]


def _parse_object(text: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else text[text.find("{") : text.rfind("}") + 1]
    parsed = json.loads(candidate)
    return parsed if isinstance(parsed, dict) else {"mechanics": []}


async def extract_mechanics(
    source: Dict[str, Any], router: Optional[SmartRoutingService], max_tokens: int = 420
) -> Dict[str, Any]:
    deterministic = deterministic_mechanics(source)
    model_mechanics: List[Dict[str, Any]] = []
    model_error = None
    raw_model_output = None
    if router is not None:
        fields = source.get("evidence_fields") or {}
        prompt = (
            "Return zero to three reusable game-engine mechanics supported by this metadata. "
            "Ignore story names and do not repeat broad genres as mechanics. Cite only description, genres, or tags.\n"
            f"NAME: {source.get('name')}\n"
            f"GENRES: {fields.get('genres', [])}\nTAGS: {fields.get('tags', [])}\n"
            f"DESCRIPTION: {str(fields.get('description', ''))[:1000]}"
        )
        response, _ = await router.chat(
            [{"role": "system", "content": "Return only the requested JSON."}, {"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=max_tokens,
            retries=1,
            response_format=MODEL_SCHEMA,
        )
        if response.ok:
            raw_model_output = response.content[:4000]
            try:
                parsed = _parse_object(response.content)
                allowed = {"description", "genres", "tags"}
                for item in parsed.get("mechanics") or []:
                    cited = [field for field in item.get("evidence_fields") or [] if field in allowed and fields.get(field)]
                    capability = slug(item.get("capability_id"))
                    label = str(item.get("label") or "")
                    summary = str(item.get("evidence_summary") or "")
                    claim_terms = {
                        term for term in slug(f"{capability} {label} {summary}").split("-")
                        if len(term) >= 4 and term not in {"game", "with", "from", "that", "this"}
                    }
                    for field in allowed:
                        field_terms = set(slug(fields.get(field) or "").split("-"))
                        if claim_terms & field_terms and fields.get(field) and field not in cited:
                            cited.append(field)
                    if not capability or capability.isdigit():
                        capability = slug(label)
                    capability = CAPABILITY_SLUG_RULES.get(capability, capability)
                    if capability and cited:
                        model_mechanics.append(
                            {
                                "capability_id": capability,
                                "label": label or capability.replace("-", " ").title(),
                                "evidence_fields": cited,
                                "evidence_summary": summary[:320],
                                "extractor": "lfm",
                            }
                        )
            except (ValueError, json.JSONDecodeError) as error:
                model_error = str(error)
        else:
            model_error = response.error or "empty model response"
    by_id = {item["capability_id"]: item for item in [*deterministic, *model_mechanics]}
    mechanics = list(by_id.values())[:8]
    payload = {
        "schema_version": MECHANIC_EVIDENCE_SCHEMA,
        "dataset": source.get("dataset"),
        "source_id": source.get("source_id"),
        "source_hash": source.get("source_hash"),
        "source_file": source.get("source_file"),
        "source_line": source.get("source_line"),
        "source_url": source.get("source_url"),
        "pipeline_epoch": source.get("pipeline_epoch"),
        "evidence_fingerprint": source.get("evidence_fingerprint"),
        "mechanics": mechanics,
        "model_error": model_error,
        "model_status": "accepted" if model_mechanics else ("failed" if model_error else "no-new-mechanics"),
        "raw_model_output": raw_model_output,
    }
    payload["evidence_hash"] = stable_hash(payload)
    return payload
