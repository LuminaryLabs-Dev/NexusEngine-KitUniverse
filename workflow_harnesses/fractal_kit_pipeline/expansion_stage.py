from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Tuple

from kituniverse_harness.smart_router import SmartRoutingService
from workflow_harnesses.fractal_kit_pipeline.kit_contract import (
    DOMAIN_FAMILIES,
    OPERATIONS,
    base_signal_triplets,
    key,
)
from workflow_harnesses.fractal_kit_pipeline.temperature_schedule import temperature_for


DEFAULT_SYSTEM = "Reply only with the requested short output."
FIRST_STAGE_POLICY = "expand aggressively, reject only obvious waste"
FIRST_STAGE_PROMPT_RULE = "Generate unusual but relevant new items."
FIRST_STAGE_PROMPT_AVOID = "exact repeats only"
FIRST_STAGE_PROMPT_ALLOW = "rough names, strange angles, partial concepts"
FIRST_STAGE_PROMPT_RETURN = "comma-separated list"
FIRST_STAGE_REJECT_ONLY = [
    "empty output",
    "exact duplicate",
    "pure formatting or numeric junk",
    "record-unsafe text",
    "direct target-term leakage",
    "Y/N relevance failure",
]
FIRST_STAGE_KEEP_IF = [
    "non-empty",
    "not exact duplicate",
    "passes Y/N relevance check",
    "loosely connected to LIST GOAL",
    "record-safe bounded text",
]
FIRST_STAGE_DO_NOT_REJECT_FOR = [
    "awkward wording",
    "near-duplicate meaning",
    "weak grammar",
    "rough labels",
    "odd but connected ideas",
]
FORBIDDEN_TARGET_TERMS = [
    "nexus",
    "protokit",
    "proto kit",
    "domain service kit",
    "domain-service-kit",
    "atomic-domain-service-kit",
]


async def run_expansion_stage(
    router: SmartRoutingService,
    list_goal: str,
    model_seed_count: int,
    max_tokens: int,
    provider_retries: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    tasks = []
    seeds = base_signal_triplets()
    items_per_prompt = 3
    for index in range(model_seed_count):
        a, b, c = seeds[index % len(seeds)]
        axis = OPERATIONS[index % len(OPERATIONS)]
        prompt = (
            f'LIST GOAL: {{"{list_goal}"}}\n'
            f"INPUT: {a}, {b}, {c}; AXIS: {axis}\n"
            f"RULE: {FIRST_STAGE_PROMPT_RULE}\n"
            f"AVOID: {FIRST_STAGE_PROMPT_AVOID}.\n"
            f"ALLOW: {FIRST_STAGE_PROMPT_ALLOW}.\n"
            "DO NOT POLISH: preserve awkward, partial, or weird connected ideas.\n"
            f"RETURN: {FIRST_STAGE_PROMPT_RETURN}."
        )
        tasks.append(
            _chat_item(
                router,
                prompt,
                temperature_for("expand"),
                max_tokens * items_per_prompt,
                provider_retries,
                index,
            )
        )
    raw = await asyncio.gather(*tasks)
    records = []
    seen = set()
    rejected = []
    relevance_tasks = []
    for item in raw:
        candidates = clean_items(item["content"], limit=items_per_prompt)
        if not candidates:
            rejected.append({**item, "stage": "expansion-point-rejected", "value": "", "reason": "empty-output"})
            continue
        for candidate in candidates:
            candidate_key = key(candidate)
            heuristic = _first_stage_acceptance(candidate, seen)
            if not heuristic["ok"]:
                rejected.append(
                    {
                        **item,
                        "stage": "expansion-point-rejected",
                        "value": candidate,
                        "key": candidate_key,
                        "reason": ",".join(heuristic["errors"]),
                        "heuristic": heuristic,
                    }
                )
                continue
            seen.add(candidate_key)
            relevance_tasks.append(
                _check_expansion_relevance(
                    router=router,
                    list_goal=list_goal,
                    candidate=candidate,
                    provider_retries=provider_retries,
                    item=item,
                    heuristic=heuristic,
                )
            )
    relevance_results = await asyncio.gather(*relevance_tasks)
    for result in relevance_results:
        if result["accepted"]:
            seen.add(result["key"])
            records.append(result["record"])
        else:
            rejected.append(result["rejection"])
    if not records:
        for index in range(min(model_seed_count, len(seeds))):
            value = _fallback_expansion(index)
            item_key = key(value)
            if item_key in seen:
                continue
            seen.add(item_key)
            records.append(
                {
                    "index": index,
                    "prompt": "fallback after first-stage rejection",
                    "content": value,
                    "ok": True,
                    "error": None,
                    "attempts": 0,
                    "usage": {},
                    "stage": "expansion-point",
                    "value": value,
                    "key": item_key,
                    "accepted_by": "fallback",
                }
            )
    report = {
        "stage": "expansion-points",
        "policy": FIRST_STAGE_POLICY,
        "prompt_stance": {
            "list_goal": list_goal,
            "rule": FIRST_STAGE_PROMPT_RULE,
            "avoid": FIRST_STAGE_PROMPT_AVOID,
            "allow": FIRST_STAGE_PROMPT_ALLOW,
            "do_not_polish": "preserve awkward, partial, or weird connected ideas",
            "return": FIRST_STAGE_PROMPT_RETURN,
        },
        "prompt_template": (
            'LIST GOAL: {"<goal>"}\n'
            "RULE: Generate unusual but relevant new items.\n"
            "AVOID: exact repeats only.\n"
            "ALLOW: rough names, strange angles, partial concepts.\n"
            "RETURN: comma-separated list."
        ),
        "acceptance_rule": "keep if non-empty, exact-unique, record-safe, and loosely relevant to LIST GOAL",
        "reject_only": FIRST_STAGE_REJECT_ONLY,
        "keep_if": FIRST_STAGE_KEEP_IF,
        "do_not_reject_for": FIRST_STAGE_DO_NOT_REJECT_FOR,
        "later_stages": ["filter", "merge", "reduce", "map"],
        "raw_generations": len(raw),
        "candidate_count": sum(len(clean_items(item["content"], limit=items_per_prompt)) for item in raw),
        "accepted_count": len(records),
        "rejected_count": len(rejected),
        "rejections": rejected[:200],
    }
    return records, report


async def _chat_item(
    router: SmartRoutingService,
    prompt: str,
    temperature: float,
    max_tokens: int,
    provider_retries: int,
    index: int,
) -> Dict[str, Any]:
    response, attempts = await router.chat(
        [{"role": "system", "content": DEFAULT_SYSTEM}, {"role": "user", "content": prompt}],
        temperature=temperature,
        max_tokens=max_tokens,
        retries=provider_retries,
    )
    return {
        "index": index,
        "prompt": prompt,
        "content": response.content,
        "ok": response.ok,
        "error": response.error,
        "attempts": attempts,
        "usage": response.usage,
    }


async def _check_expansion_relevance(
    router: SmartRoutingService,
    list_goal: str,
    candidate: str,
    provider_retries: int,
    item: Dict[str, Any],
    heuristic: Dict[str, Any],
) -> Dict[str, Any]:
    prompt = (
        f'LIST GOAL: {{"{list_goal}"}}\n'
        f"CANDIDATE: {candidate}\n"
        "Y if loosely connected, including odd but relevant tangents.\n"
        "N only if empty, exact repeat, pure formatting junk, or unrelated."
    )
    response, attempts = await router.chat(
        [{"role": "system", "content": "Reply only Y or N."}, {"role": "user", "content": prompt}],
        temperature=temperature_for("relevance_check"),
        max_tokens=2,
        retries=provider_retries,
    )
    item_key = key(candidate)
    accepted = bool(response.ok and _clean_yes_no(response.content) == "Y")
    record = {
        **item,
        "stage": "expansion-point",
        "value": candidate,
        "key": item_key,
        "accepted_by": "first-stage-expansive-policy",
        "heuristic": heuristic,
        "relevance_prompt": prompt,
        "relevance_raw": response.content,
        "relevance_attempts": attempts,
    }
    rejection = {
        **item,
        "stage": "expansion-point-rejected",
        "value": candidate,
        "key": item_key,
        "reason": "relevance-check-n",
        "heuristic": heuristic,
        "relevance_prompt": prompt,
        "relevance_raw": response.content,
        "relevance_error": response.error,
        "relevance_attempts": attempts,
    }
    return {"accepted": accepted, "key": item_key, "record": record, "rejection": rejection}


def _first_stage_acceptance(candidate: str, seen: set[str]) -> Dict[str, Any]:
    candidate_key = key(candidate)
    errors = []
    if not candidate_key:
        errors.append("empty")
    if candidate_key in seen:
        errors.append("exact-duplicate")
    if candidate_key.isdigit():
        errors.append("numeric-only")
    if _is_pure_formatting_junk(candidate):
        errors.append("formatting-junk")
    if not _is_record_safe(candidate):
        errors.append("record-unsafe")
    if _contains_forbidden_target_term(candidate):
        errors.append("direct-target-leakage")
    return {
        "ok": not errors,
        "errors": errors,
        "key": candidate_key,
        "policy": "first-stage-expansive",
        "rejects_only_obvious_waste": True,
        "semantic_merging_deferred": True,
    }


def clean_item(value: str) -> str:
    first = value.strip().splitlines()[0] if value.strip() else ""
    first = first.split(",", 1)[0]
    first = re.sub(r"^\s*(?:[-*]|\d+[.)])\s*", "", first)
    first = re.sub(r"^\s*(?:ITEM|IDEA|OUTPUT|RETURN|LIST)\s*[:=-]\s*", "", first, flags=re.IGNORECASE)
    return " ".join(first.strip(" .;:\"'`").split())[:96]


def clean_items(value: str, limit: int) -> List[str]:
    values = []
    seen = set()
    if "," in value or "\n" in value:
        parts = re.split(r",|\n", value.strip())
    else:
        parts = [value.strip()]
    for part in parts:
        cleaned = clean_item(part)
        item_key = key(cleaned)
        if cleaned and item_key not in seen:
            seen.add(item_key)
            values.append(cleaned)
        if len(values) >= limit:
            break
    return values


def _is_pure_formatting_junk(value: str) -> bool:
    stripped = value.strip()
    if not stripped:
        return True
    return not any(character.isalnum() for character in stripped)


def _is_record_safe(value: str) -> bool:
    if not value or len(value) > 120:
        return False
    return all(character.isprintable() or character.isspace() for character in value)


def _contains_forbidden_target_term(value: str) -> bool:
    text = value.lower()
    return any(term in text for term in FORBIDDEN_TARGET_TERMS)


def _fallback_expansion(index: int) -> str:
    family, subdomains = DOMAIN_FAMILIES[index % len(DOMAIN_FAMILIES)]
    return f"{family} {subdomains[index % len(subdomains)]}"


def _clean_yes_no(value: str) -> str:
    value = value.strip().upper()
    if value.startswith("Y"):
        return "Y"
    return "N"
