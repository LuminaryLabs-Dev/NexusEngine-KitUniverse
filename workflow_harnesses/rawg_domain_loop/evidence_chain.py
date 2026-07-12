from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from kituniverse_harness.providers import LMStudioProvider, ProviderResponse
from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash


STRONG_ACTION_MARKERS = {
    "add", "break", "build", "buy", "choose", "climb", "collect", "configure", "configured",
    "craft", "dip", "experiment", "explore", "grant", "interact", "jump", "landing", "modify",
    "move", "moving", "play", "react", "remove", "roll", "rolling", "select", "shove", "sneak",
    "spend", "start", "support", "supports", "take", "use", "win", "wins",
}
IGNORED_TAGS = {
    "adventure", "captions available", "crpg", "fantasy", "isometric", "pc", "role playing game",
    "role-playing", "rpg", "steam achievements", "steam cloud", "steam trading cards", "strategy",
}
FACT_GENERIC = {"gameplay", "game-mechanic", "metadata-support", "player-action", "rpg-feature"}
FACT_STOP = {
    "and", "can", "from", "game", "has", "into", "mechanic", "metadata", "player", "players",
    "system", "that", "the", "their", "this", "with",
}
NARRATIVE_MARKERS = {"betrayal", "cinematic", "darkness", "evil", "fate", "greatest", "lore", "power", "price", "story", "tale"}

FACT_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "mechanical_fact_extraction",
        "strict": "true",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "facts": {
                    "type": "array",
                    "maxItems": 32,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "statement": {"type": "string"},
                            "source_ids": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["statement", "source_ids"],
                    },
                }
            },
            "required": ["facts"],
        },
    },
}

FACT_REVIEW_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "mechanical_fact_review",
        "strict": "true",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "accepted": {"type": "boolean"},
                "reason": {"type": "string"}
            },
            "required": ["accepted", "reason"],
        },
    },
}

FACT_LABEL_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "fact_capability_labels",
        "strict": "true",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "labels": {"type": "array", "minItems": 0, "maxItems": 5, "items": {"type": "string"}}
            },
            "required": ["labels"],
        },
    },
}


def run_evidence_chain(
    provider: LMStudioProvider,
    record: Dict[str, Any],
    run_dir: Path,
    max_tokens: int,
    review_temperature: float,
) -> Dict[str, Any]:
    chain_dir = run_dir / "mechanical-evidence-chain"
    chain_dir.mkdir(parents=True, exist_ok=True)
    units = select_evidence_units(record)
    _write_json(chain_dir / "01-selected-evidence.json", units)
    if not units:
        result = {"ok": False, "calls_completed": 0, "accepted_facts": [], "reason": "no-mechanical-evidence-units"}
        _write_json(chain_dir / "report.json", result)
        return result

    extraction_prompt = build_extraction_prompt(record, units)
    extraction = provider.chat(
        messages=[
            {"role": "system", "content": "Convert explicit source units into mechanical facts. Return only the supplied JSON schema."},
            {"role": "user", "content": extraction_prompt},
        ],
        temperature=0.2,
        max_tokens=max_tokens,
        response_format=FACT_SCHEMA,
    )
    facts, parse_error = parse_facts(extraction)
    _write_json(chain_dir / "02-extracted-facts.json", _response_artifact(extraction_prompt, extraction, {"facts": facts, "error": parse_error}))

    grounded, rejected = ground_facts(facts, units)
    _write_json(chain_dir / "03-grounded-facts.json", {"accepted_for_review": grounded, "rejected": rejected})
    if not grounded:
        result = {
            "ok": False,
            "calls_completed": 1,
            "accepted_facts": [],
            "rejected_facts": rejected,
            "reason": "no-facts-passed-grounding",
        }
        _write_json(chain_dir / "report.json", result)
        return result

    all_decisions: List[Dict[str, Any]] = []
    review_artifacts = []
    review_failures = []
    for fact_index, fact in enumerate(grounded, start=1):
        fact_units = [unit for unit in units if unit["source_id"] in set(fact["source_ids"])]
        review_prompt = build_fact_review_prompt(fact, fact_units)
        review = provider.chat(
            messages=[
                {"role": "system", "content": "Conservatively verify direct mechanical entailment. Return only the supplied JSON schema."},
                {"role": "user", "content": review_prompt},
            ],
            temperature=review_temperature,
            max_tokens=min(max_tokens, 1200),
            response_format=FACT_REVIEW_SCHEMA,
        )
        decision, review_error = parse_fact_review(review, fact["fact_id"])
        review_artifacts.append(_response_artifact(review_prompt, review, {"decision": decision, "error": review_error, "fact_index": fact_index}))
        if decision is None:
            review_failures.append({"fact_id": fact["fact_id"], "reason": f"invalid-fact-review:{review_error}"})
        else:
            all_decisions.append(decision)
    _write_json(chain_dir / "04-fact-review.json", {"batches": review_artifacts, "decisions": all_decisions, "failures": review_failures})
    accepted_ids = {item["fact_id"] for item in all_decisions if item["accepted"] is True}
    accepted = [item for item in grounded if item["fact_id"] in accepted_ids]
    reason = "accepted" if accepted else ("invalid-fact-review" if review_failures else "review-rejected-all-facts")
    packet = {
        "schema_version": "mechanical.evidence-packet.v1",
        "source_id": record.get("source_id"),
        "source_hash": record.get("source_hash"),
        "facts": accepted,
        "packet_hash": stable_hash(accepted),
    }
    _write_json(chain_dir / "mechanical-evidence.json", packet)
    result = {
        "ok": bool(accepted),
        "calls_completed": 1 + len(review_artifacts),
        "selected_unit_count": len(units),
        "extracted_fact_count": len(facts),
        "grounded_fact_count": len(grounded),
        "accepted_fact_count": len(accepted),
        "accepted_facts": accepted,
        "rejected_facts": [*rejected, *review_failures, *({"fact_id": item["fact_id"], "reason": item["reason"]} for item in all_decisions if item["accepted"] is False)],
        "reason": reason,
        "packet_path": str(chain_dir / "mechanical-evidence.json"),
        "packet_hash": packet["packet_hash"],
    }
    _write_json(chain_dir / "report.json", result)
    return result


def select_evidence_units(record: Dict[str, Any]) -> List[Dict[str, str]]:
    units: List[Dict[str, str]] = []
    description = " ".join(str(record.get("description") or "").split())
    sentences = [value.strip() for value in re.split(r"(?<=[.!?])\s+", description) if value.strip()]
    for sentence in sentences:
        tokens = set(slug(sentence).split("-"))
        explicit_mode = any(phrase in sentence.lower() for phrase in ["local multiplayer", "online multiplayer", "split screen", "co-op", "up to four"])
        explicit_structure = "verticality" in tokens and "exploration" in tokens
        if tokens & STRONG_ACTION_MARKERS or explicit_mode or explicit_structure:
            units.append({"source_id": f"D{len(units) + 1:03d}", "field": "description", "text": sentence})
    tag_index = 0
    for tag in record.get("tags") or []:
        normalized = str(tag).strip()
        if not normalized or normalized.lower() in IGNORED_TAGS:
            continue
        tag_index += 1
        units.append({"source_id": f"T{tag_index:03d}", "field": "tags", "text": normalized})
    return units[:48]


def build_extraction_prompt(record: Dict[str, Any], units: List[Dict[str, str]]) -> str:
    return f"""
Extract every distinct player-facing mechanical fact directly entailed by the indexed source units.

Rules:
- A fact is one short subject-action-state/rule statement.
- Cite one or more source_ids that directly entail the whole statement.
- Use only the supplied source IDs.
- Split different actions, states, conditions, multiplayer modes, limits, choices, and outcomes.
- Tags support only what they literally name.
- Do not infer ordinary RPG features, implementation details, causality, or mechanics from narrative tone.
- Do not create domains or capability names yet.
- Return zero facts when nothing mechanical is explicit.

GAME: {record.get('name')} ({record.get('source_id')})
SOURCE UNITS: {json.dumps(units, ensure_ascii=False)}

The response schema is supplied separately. Populate it with source-specific facts only.
""".strip()


def build_fact_review_prompt(fact: Dict[str, Any], units: List[Dict[str, str]]) -> str:
    return f"""
Decide whether this one extracted fact is a concrete reusable game mechanic directly and completely entailed by its cited source units.

Accept only explicit player-facing actions, state changes, rules, limits, choices, outcomes, or multiplayer modes. Reject plot, advertising language, theme, lore, emotional metaphor, broad genre, graphics-engine claims, implementation inference, or a fact whose source supports only part of the statement. A sentence copied exactly from the source can still be non-mechanical.

FACT: {json.dumps(fact, ensure_ascii=False)}
SOURCE UNITS: {json.dumps(units, ensure_ascii=False)}

The response schema is supplied separately.
""".strip()


def parse_facts(response: ProviderResponse) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if not response.ok:
        return [], response.error or "provider-failed"
    try:
        parsed = _parse_object(response.content)
        output = []
        seen = set()
        for index, value in enumerate(parsed.get("facts") or [], start=1):
            if not isinstance(value, dict):
                continue
            statement = " ".join(str(value.get("statement") or "").split()).strip(" .")
            semantic_key = slug(statement)
            fact_id = f"fact-{index:03d}"
            if not statement or not semantic_key or semantic_key in seen:
                continue
            seen.add(semantic_key)
            output.append({"fact_id": fact_id, "statement": statement, "source_ids": [str(item) for item in value.get("source_ids") or []]})
        return output, None
    except (ValueError, json.JSONDecodeError) as error:
        return [], str(error)


def ground_facts(facts: List[Dict[str, Any]], units: List[Dict[str, str]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    unit_map = {item["source_id"]: item for item in units}
    accepted = []
    rejected = []
    for fact in facts:
        fact_id = fact["fact_id"]
        source_ids = list(dict.fromkeys(fact.get("source_ids") or []))
        reason = None
        if fact_id in FACT_GENERIC or not source_ids or any(value not in unit_map for value in source_ids):
            reason = "invalid-or-missing-source-ids"
        else:
            fact_tokens = _tokens(fact["statement"]) - FACT_STOP
            source_tokens = set().union(*(_tokens(unit_map[value]["text"]) for value in source_ids)) - FACT_STOP
            matched = sorted(fact_tokens & source_tokens)
            if not matched:
                reason = "no-grounded-term-overlap"
            action_count = len(fact_tokens & STRONG_ACTION_MARKERS)
            explicit_mode = bool(fact_tokens & {"multiplayer", "online", "local", "coop"})
            explicit_structure = "verticality" in fact_tokens and "exploration" in fact_tokens
            if not reason and not action_count and not explicit_mode and not explicit_structure:
                reason = "no-concrete-mechanical-predicate"
            if not reason and fact_tokens & NARRATIVE_MARKERS and ({"may", "could"} & fact_tokens or action_count < 2):
                reason = "narrative-or-promotional-claim"
        if reason:
            rejected.append({"fact_id": fact_id, "reason": reason})
            continue
        accepted.append({**fact, "source_ids": source_ids, "matched_source_terms": matched, "source_units": [unit_map[value] for value in source_ids], "grounding_hash": stable_hash([fact_id, source_ids, matched])})
    return accepted, rejected


def parse_fact_review(response: ProviderResponse, fact_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not response.ok:
        return None, response.error or "provider-failed"
    try:
        parsed = _parse_object(response.content)
        if not isinstance(parsed.get("accepted"), bool):
            raise ValueError("fact review must return a typed accepted decision")
        return {"fact_id": fact_id, "accepted": parsed["accepted"], "reason": str(parsed.get("reason") or "")}, None
    except (ValueError, json.JSONDecodeError) as error:
        return None, str(error)


def evidence_packet_tokens(facts: List[Dict[str, Any]]) -> set[str]:
    return set().union(*(_tokens(item.get("statement") or "") for item in facts)) if facts else set()


def fact_ids_for_candidate(candidate_id: str, facts: List[Dict[str, Any]]) -> List[str]:
    candidate_tokens = _tokens(candidate_id) - FACT_STOP
    return [item["fact_id"] for item in facts if candidate_tokens & (_tokens(item.get("statement") or "") - FACT_STOP)]


def seed_capability_candidates(
    provider: LMStudioProvider, facts: List[Dict[str, Any]], run_dir: Path
) -> Dict[str, Any]:
    artifacts = []
    proposals = []
    for fact in facts:
        prompt = f"""
Convert this one accepted mechanical fact into 1 to 5 atomic reusable game-engine capability labels.

Split different owned behavior. Every label must contain 2 to 6 meaningful words and must reuse at least two concrete words from the fact, or their direct singular/verb form. Never return a generic word such as action, state, rule, limit, choice, feature, mechanic, gameplay, explore, discover, or navigate by itself. Do not repeat the fact ID, story nouns, genres, broad categories, or implementation guesses. It is valid to return no labels.

FACT: {json.dumps({'fact_id': fact['fact_id'], 'statement': fact['statement']}, ensure_ascii=False)}

The response schema is supplied separately.
""".strip()
        response = provider.chat(
            messages=[
                {"role": "system", "content": "Return only source-grounded capability labels in the supplied JSON schema."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.4,
            max_tokens=600,
            response_format=FACT_LABEL_SCHEMA,
        )
        labels: List[str] = []
        error = None
        try:
            parsed = _parse_object(response.content) if response.ok else {}
            labels = [" ".join(str(value).split()) for value in (parsed.get("labels") or [])[:5] if str(value).strip()]
        except (ValueError, json.JSONDecodeError) as exc:
            error = str(exc)
        for label in labels:
            proposals.append({"candidate_id": slug(label), "label": label, "evidence_fact_ids": [fact["fact_id"]]})
        artifacts.append(_response_artifact(prompt, response, {"fact_id": fact["fact_id"], "labels": labels, "error": error}))
    path = run_dir / "mechanical-evidence-chain" / "05-fact-capability-seeds.json"
    _write_json(path, {"calls_completed": len(facts), "proposals": proposals, "artifacts": artifacts})
    return {"calls_completed": len(facts), "proposals": proposals, "artifact_path": str(path)}


def _tokens(value: str) -> set[str]:
    output = set()
    for token in slug(value).split("-"):
        if len(token) < 3:
            continue
        output.add(token)
        if token.endswith("ies") and len(token) > 4:
            output.add(token[:-3] + "y")
        elif token.endswith("s") and not token.endswith("ss") and len(token) > 3:
            output.add(token[:-1])
    return output


def _parse_object(content: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", content, re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else content[content.find("{") : content.rfind("}") + 1]
    parsed = json.loads(candidate)
    if not isinstance(parsed, dict):
        raise ValueError("response is not an object")
    return parsed


def _response_artifact(prompt: str, response: ProviderResponse, extra: Dict[str, Any]) -> Dict[str, Any]:
    return {"prompt": prompt, "response": response.to_dict(), **extra}


def _write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
