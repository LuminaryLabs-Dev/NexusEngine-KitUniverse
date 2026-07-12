from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Tuple


MODEL_FIELDS = [
    "name",
    "domain",
    "purpose",
    "owned_state",
    "inputs",
    "outputs",
    "requires",
    "provides",
    "idempotency_key",
    "invariants",
    "reset_behavior",
    "snapshot_fields",
    "implementation_steps",
    "tests",
]

MODEL_DRAFT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "name": {"type": "string"},
        "domain": {"type": "string"},
        "purpose": {"type": "string"},
        "owned_state": {"type": "array", "items": {"type": "string"}},
        "inputs": {"type": "array", "items": {"type": "string"}},
        "outputs": {"type": "array", "items": {"type": "string"}},
        "requires": {"type": "array", "items": {"type": "string"}},
        "provides": {"type": "array", "items": {"type": "string"}},
        "idempotency_key": {"type": "string"},
        "invariants": {"type": "array", "items": {"type": "string"}},
        "reset_behavior": {"type": "string"},
        "snapshot_fields": {"type": "array", "items": {"type": "string"}},
        "implementation_steps": {"type": "array", "items": {"type": "string"}},
        "tests": {"type": "array", "items": {"type": "string"}},
    },
    "required": MODEL_FIELDS,
}

RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "guided_kit_slots",
        "strict": "true",
        "schema": MODEL_DRAFT_SCHEMA,
    },
}

REQUIRED_PAYLOAD_FIELDS = [
    "name",
    "domain",
    "domain_path",
    "purpose",
    "owned_state",
    "inputs",
    "outputs",
    "requires",
    "provides",
    "resources",
    "events",
    "systems",
    "public_api",
    "state_rules",
    "tests",
    "snapshot",
    "renderer_boundary",
    "idempotency_key",
    "atomic_evidence",
    "implementation_steps",
    "source_evidence",
    "slots",
]

GENERIC_VALUES = {"", "none", "unknown", "value", "id", "key", "test", "state", "com"}
GENERIC_DOMAINS = {
    "atomic",
    "behavior",
    "capability",
    "description",
    "domain",
    "kit",
    "service",
    "timers",
    "title",
}
GENERIC_SLOT_VALUES = {
    "data",
    "input",
    "inputs",
    "item",
    "items",
    "output",
    "outputs",
    "process",
    "processes",
    "snapshot",
    "snapshots",
    "state",
    "thing",
    "things",
    "value",
    "values",
}
GENERIC_STATE_METADATA = {"entity-id", "id", "timestamp"}
INVALID_SEMANTIC_TERMS = {"invulneration"}
SEMANTIC_CORRECTIONS = {"invulneration": "invulnerability"}
PROMPT_LEAKAGE = {
    "choose one to three specific subject words",
    "fill every schema",
    "schema slot",
    "atomic kit",
    "return a complete",
    "goal:",
    "omit generic words",
    "extract reusable behavior",
    "extract one reusable",
    "summarize owned behavior",
}


def build_guided_kit(
    case: Dict[str, Any],
    draft: Dict[str, Any],
    raw_output: str,
    model_attempt: int,
) -> Dict[str, Any]:
    title = str(case.get("title") or draft.get("name") or "guided kit").strip()
    idea_id = slug(str(case.get("idea_id") or title))
    name = slug(title)
    if not name.endswith("-kit"):
        name = f"{name}-kit"
    domain_hint = slug(str(case.get("domain_hint") or ""))
    draft_domain = slug(str(draft.get("domain") or ""))
    inferred_domain = infer_domain(str(case.get("description", "")))
    domain_from_description = not domain_hint and bool(inferred_domain)
    domain_from_title = not domain_hint and not inferred_domain and _generic_domain(draft_domain)
    domain = domain_hint or inferred_domain or (
        infer_domain_from_title(title) if domain_from_title else draft_domain
    )
    description = str(case.get("description", ""))
    purpose, purpose_corrected = correct_semantic_sentence(
        clean_sentence(draft.get("purpose")),
        description,
    )
    purpose_from_description = len(purpose.split()) < 3 or any(
        marker in purpose.lower() for marker in PROMPT_LEAKAGE
    )
    if purpose_from_description:
        purpose = clean_sentence(str(case.get("description", "")).split(".", 1)[0])

    slot_hints = case.get("slot_hints") or {}
    owned_state_hint = clean_list(slot_hints.get("owned_state"))
    inputs_hint = clean_list(slot_hints.get("inputs"))
    outputs_hint = clean_list(slot_hints.get("outputs"))
    inferred_inputs = infer_inputs(str(case.get("description", "")))
    inferred_owned_state = infer_owned_state(str(case.get("description", "")))
    owned_state, owned_state_corrected = correct_semantic_terms(
        owned_state_hint or inferred_owned_state or clean_list(draft.get("owned_state")),
        description,
    )
    inputs, inputs_corrected = correct_semantic_terms(
        inputs_hint or inferred_inputs or clean_list(draft.get("inputs")),
        description,
    )
    outputs, outputs_corrected = correct_semantic_terms(
        outputs_hint or clean_list(draft.get("outputs")),
        description,
    )
    input_role_keys = {_semantic_role_key(value) for value in inputs}
    role_filtered_state = [
        value for value in owned_state if _semantic_role_key(value) not in input_role_keys
    ]
    owned_state_role_corrected = role_filtered_state != owned_state and bool(role_filtered_state)
    if owned_state_role_corrected:
        owned_state = role_filtered_state
    model_requires = clean_token_list(draft.get("requires"))
    requires_hint = clean_token_list(case.get("requires_hint"))
    provides_hint = clean_token_list(case.get("provides_hint"))
    inferred_requires = infer_namespaced_tokens(str(case.get("description", "")), "requires")
    inferred_provides = infer_namespaced_tokens(str(case.get("description", "")), "provides")
    input_requires = infer_requires_from_inputs(inputs)
    normalized_model_requires = [
        token
        for token in namespace_tokens(model_requires, domain)
        if not _placeholder_contract_token(token)
    ]
    requires = requires_hint or inferred_requires or input_requires or normalized_model_requires
    provides = provides_hint or inferred_provides or namespace_tokens(outputs, domain)
    outputs_from_provides = False
    if not outputs_hint and (provides_hint or inferred_provides):
        provided_outputs = unique(
            token.split(":", 1)[-1] for token in provides if ":" in token
        )
        outputs_from_provides = bool(provided_outputs and provided_outputs != outputs)
        if provided_outputs:
            outputs = provided_outputs
    requires_from_contract = not requires_hint and not inferred_requires and bool(input_requires)
    provides_from_contract = not provides_hint and not inferred_provides
    if requires_from_contract:
        requires = unique(requires)
    idempotency_hint = slug(str(slot_hints.get("idempotency_key") or ""))
    idempotency_key = idempotency_hint or slug(str(draft.get("idempotency_key") or ""))
    inferred_idempotency_key = infer_idempotency_key(str(case.get("description", "")))
    idempotency_from_description = (
        not idempotency_hint
        and (
            idempotency_key in GENERIC_VALUES
            or any(character.isdigit() for character in idempotency_key)
            or idempotency_key.startswith("replay")
        )
    )
    if not idempotency_hint and inferred_idempotency_key:
        idempotency_key = inferred_idempotency_key
        idempotency_from_description = True
    state_without_metadata = [
        value for value in owned_state if value not in GENERIC_STATE_METADATA
    ]
    idempotency_state = f"applied-{idempotency_key}s" if idempotency_key else ""
    owned_state_contract_corrected = (
        state_without_metadata != owned_state
        or (bool(idempotency_state) and idempotency_state not in state_without_metadata)
    )
    owned_state = unique([*state_without_metadata, idempotency_state])
    invariants, _ = correct_semantic_sentences(
        clean_sentences(draft.get("invariants")),
        description,
    )
    invariants = [value for value in invariants if not _weak_invariant(value)]
    source_behavior_rules = clean_sentences(case.get("behavior_rules"))
    invariants = unique([*source_behavior_rules, *invariants])
    reset_behavior = clean_sentence(draft.get("reset_behavior"))
    reset_from_contract = not _valid_reset_behavior(reset_behavior, owned_state)
    if reset_from_contract:
        reset_behavior = (
            f"clear {', '.join(owned_state)} and restore configured defaults"
        )
    model_snapshot_fields, snapshot_terms_corrected = correct_semantic_terms(
        clean_list(draft.get("snapshot_fields")),
        description,
    )
    snapshot_from_contract = set(model_snapshot_fields) != set(owned_state)
    snapshot_fields = list(owned_state)
    guided_steps = clean_sentences(case.get("build_plan"))
    implementation_from_contract = not guided_steps
    if implementation_from_contract:
        implementation_steps = [
            f"define state schema for {', '.join(owned_state)}",
            f"implement deterministic input reducer for {', '.join(inputs)}",
            *[f"enforce behavior rule: {rule}" for rule in source_behavior_rules],
            f"emit and validate {', '.join(provides or outputs)}",
            f"prove replay safety with {idempotency_key or 'the semantic request id'}",
        ]
    else:
        implementation_steps = guided_steps
    tests_from_contract = True
    tests = _contract_tests(inputs, outputs, idempotency_key)

    if idempotency_key and not any(idempotency_key in slug(rule) for rule in invariants):
        invariants.append(f"Repeated {idempotency_key} values are ignored")
    invariants.extend(
        rule
        for rule in [
            "State is serializable and resettable",
            "Runtime truth is not owned by renderer or host UI",
        ]
        if rule not in invariants
    )
    tests = unique(
        [
            *tests,
            *[f"behavior-rule-{slug(rule)[:72]}" for rule in source_behavior_rules],
            "idempotency-replay",
            "snapshot-reset-smoke",
            "contract-shape-check",
        ]
    )
    resource = f"{pascal(name.removesuffix('-kit'))}State"
    system = f"{pascal(name.removesuffix('-kit'))}System"
    api_base = camel(name.removesuffix("-kit"))
    public_api = unique(
        [f"engine.n.{api_base}.getState", *[f"engine.n.{api_base}.{camel(value)}" for value in inputs]]
    )
    atomic_checks = {
        "single_domain": bool(domain),
        "owned_state_present": bool(owned_state),
        "input_boundary_present": bool(inputs),
        "output_boundary_present": bool(outputs or provides),
        "bounded_state_slots": 0 < len(owned_state) <= 8,
    }
    atomic = all(atomic_checks.values())
    idempotent = bool(idempotency_key and idempotency_key not in GENERIC_VALUES)
    semantic_key = slug(
        "|".join([domain, purpose, *owned_state, *requires, *provides, idempotency_key])
    )
    suffix = hashlib.sha1(f"{idea_id}|{semantic_key}".encode("utf-8")).hexdigest()[:10]
    record_id = f"guided-kit-{idea_id}-{suffix}"

    source_by_slot = {
        "name": "intake-title",
        "domain": "intake-domain-hint" if domain_hint else "model-draft",
        "requires": "intake-requires-hint" if requires_hint else "model-draft",
        "provides": "intake-provides-hint" if provides_hint else "model-draft",
        "implementation_steps": "intake-build-plan" if guided_steps else "model-draft",
    }
    for field, has_hint in [
        ("owned_state", bool(owned_state_hint)),
        ("inputs", bool(inputs_hint)),
        ("outputs", bool(outputs_hint)),
        ("idempotency_key", bool(idempotency_hint)),
    ]:
        if has_hint:
            source_by_slot[field] = "intake-slot-hint"
    for field, corrected in [
        (
            "owned_state",
            owned_state_corrected or owned_state_role_corrected or owned_state_contract_corrected,
        ),
        ("inputs", inputs_corrected),
        ("outputs", outputs_corrected),
    ]:
        if corrected:
            source_by_slot[field] = "deterministic-source-correction"
    if owned_state_contract_corrected:
        source_by_slot["owned_state"] = "deterministic-contract-state"
    if purpose_from_description:
        source_by_slot["purpose"] = "intake-description"
    elif purpose_corrected:
        source_by_slot["purpose"] = "deterministic-source-correction"
    if domain_from_description:
        source_by_slot["domain"] = "intake-description"
    if domain_from_title:
        source_by_slot["domain"] = "deterministic-title-domain"
    if not inputs_hint and inferred_inputs:
        source_by_slot["inputs"] = "intake-description"
    if not owned_state_hint and inferred_owned_state:
        source_by_slot["owned_state"] = "intake-description"
    if not requires_hint and inferred_requires:
        source_by_slot["requires"] = "intake-description"
    if not provides_hint and inferred_provides:
        source_by_slot["provides"] = "intake-description"
    if outputs_from_provides:
        source_by_slot["outputs"] = (
            "intake-provides-hint" if provides_hint else "intake-description"
        )
    if requires_from_contract:
        source_by_slot["requires"] = "deterministic-contract-interface"
    if provides_from_contract:
        source_by_slot["provides"] = "deterministic-contract-interface"
    if idempotency_from_description and inferred_idempotency_key:
        source_by_slot["idempotency_key"] = "intake-description"
    if implementation_from_contract:
        source_by_slot["implementation_steps"] = "deterministic-contract-plan"
    if tests_from_contract:
        source_by_slot["tests"] = "deterministic-contract-tests"
    if reset_from_contract:
        source_by_slot["reset_behavior"] = "deterministic-contract-reset"
    if snapshot_from_contract or snapshot_terms_corrected:
        source_by_slot["snapshot"] = "deterministic-contract-snapshot"
    payload: Dict[str, Any] = {
        "name": name,
        "domain": domain,
        "domain_path": f"n:{domain}" if domain else "",
        "type": "atomic-domain-service-kit",
        "status": "generated-candidate",
        "purpose": purpose,
        "owned_meaning": purpose,
        "owned_state": owned_state,
        "inputs": inputs,
        "outputs": outputs,
        "requires": requires,
        "provides": provides,
        "resources": [resource],
        "events": provides,
        "systems": [system],
        "public_api": public_api,
        "state_rules": unique(invariants),
        "tests": tests,
        "reset_behavior": reset_behavior,
        "snapshot": {
            "supportsSnapshot": bool(snapshot_fields),
            "supportsReset": bool(reset_behavior),
            "supportsLoadSnapshot": bool(snapshot_fields),
            "fields": snapshot_fields,
        },
        "renderer_boundary": {
            "outputsDescriptors": any("descriptor" in token for token in provides),
            "ownsDom": False,
            "ownsCanvas": False,
            "ownsThreeObjects": False,
        },
        "forbidden_imports": ["browser-host-lifecycle", "canvas", "dom", "renderer-runtime"],
        "idempotency_key": idempotency_key,
        "atomic": atomic,
        "idempotent": idempotent,
        "atomic_evidence": atomic_checks,
        "implementation_steps": implementation_steps,
        "promotion": {
            "level": "proof-only",
            "criteria": ["slot validation", "idempotency replay", "snapshot/reset smoke"],
        },
        "merge_key": slug("|".join([domain, *owned_state, *requires, *provides])),
        "semantic_key": semantic_key,
        "source_evidence": {
            "idea_id": idea_id,
            "title": title,
            "description": case.get("description", ""),
            "model_attempt": model_attempt,
            "raw_model_output": raw_output,
            "behavior_rules": source_behavior_rules,
            "source_context": case.get("source_context", {}),
        },
        "slots": {},
    }
    for field in [
        "name",
        "domain",
        "purpose",
        "owned_state",
        "inputs",
        "outputs",
        "requires",
        "provides",
        "idempotency_key",
        "reset_behavior",
        "snapshot",
        "implementation_steps",
        "tests",
    ]:
        payload["slots"][field] = {
            "value": deepcopy(payload[field]),
            "status": "proposed",
            "source": source_by_slot.get(field, "model-draft"),
        }
    return {"record_id": record_id, "source": "GuidedKitBuilder", "payload": payload}


def validate_guided_kit(record: Dict[str, Any]) -> Dict[str, Any]:
    payload = record.get("payload") or {}
    errors = []
    for field in REQUIRED_PAYLOAD_FIELDS:
        if not payload.get(field):
            errors.append(f"missing-{field}")
    for field in ["owned_state", "inputs", "outputs", "requires", "provides", "state_rules", "tests"]:
        if not isinstance(payload.get(field), list) or not payload.get(field):
            errors.append(f"invalid-list-{field}")
    slots = (payload.get("slots") or {}).values()
    if not any(slot.get("source") == "model-draft" for slot in slots):
        errors.append("no-model-draft-contribution")
    for field in ["requires", "provides"]:
        malformed = [value for value in payload.get(field, []) if ":" not in value]
        if malformed:
            errors.append(f"unnamespaced-{field}")
        placeholders = [value for value in payload.get(field, []) if _placeholder_contract_token(value)]
        if placeholders:
            errors.append(f"placeholder-{field}:{','.join(placeholders)}")
    if set(payload.get("requires", [])) & set(payload.get("provides", [])):
        errors.append("direct-dependency-self-edge")
    if payload.get("idempotency_key") in GENERIC_VALUES:
        errors.append("generic-idempotency-key")
    purpose = str(payload.get("purpose", "")).lower()
    if len(purpose.split()) < 3 or any(marker in purpose for marker in PROMPT_LEAKAGE):
        errors.append("generic-or-leaked-purpose")
    if len(payload.get("implementation_steps", [])) < 3:
        errors.append("implementation-plan-too-short")
    if _generic_domain(str(payload.get("domain", ""))):
        errors.append("generic-domain")
    provided_tokens = {str(value).split(":", 1)[-1] for value in payload.get("provides", [])}
    missing_output_provisions = sorted(set(payload.get("outputs", [])) - provided_tokens)
    if missing_output_provisions:
        errors.append(f"outputs-not-provided:{','.join(missing_output_provisions)}")
    for field in ["owned_state", "inputs", "outputs"]:
        generic_slots = sorted(set(payload.get(field, [])) & GENERIC_SLOT_VALUES)
        if generic_slots:
            errors.append(f"generic-{field}:{','.join(generic_slots)}")
        invalid_slots = sorted(
            value
            for value in payload.get(field, [])
            if set(str(value).lower().replace("_", "-").split("-")) & INVALID_SEMANTIC_TERMS
        )
        if invalid_slots:
            errors.append(f"invalid-semantic-{field}:{','.join(invalid_slots)}")
    metadata_state = sorted(set(payload.get("owned_state", [])) & GENERIC_STATE_METADATA)
    if metadata_state:
        errors.append(f"generic-state-metadata:{','.join(metadata_state)}")
    idempotency_key = slug(payload.get("idempotency_key", ""))
    if idempotency_key and not any(
        idempotency_key in slug(value) for value in payload.get("owned_state", [])
    ):
        errors.append("idempotency-state-not-owned")
    source_description = str((payload.get("source_evidence") or {}).get("description", ""))
    source_behavior_rules = (payload.get("source_evidence") or {}).get("behavior_rules") or []
    source_context = (payload.get("source_evidence") or {}).get("source_context") or {}
    if source_context.get("schema_version") == "kit.build-request.v1":
        if not source_context.get("capability_cluster_id"):
            errors.append("missing-capability-cluster-provenance")
        if not source_context.get("rawg_source_ids"):
            errors.append("missing-rawg-source-provenance")
        if not source_context.get("evidence_hash"):
            errors.append("missing-capability-evidence-hash")
    missing_behavior_rules = sorted(
        set(source_behavior_rules) - set(payload.get("state_rules", []))
    )
    if missing_behavior_rules:
        errors.append("source-behavior-rules-missing")
    source_owned_state = set(infer_owned_state(source_description))
    missing_source_state = sorted(source_owned_state - set(payload.get("owned_state", [])))
    if missing_source_state:
        errors.append(f"source-owned-state-missing:{','.join(missing_source_state)}")
    semantic_text_fields = {
        "state-rules": payload.get("state_rules", []),
        "implementation-steps": payload.get("implementation_steps", []),
        "tests": payload.get("tests", []),
        "reset-behavior": [payload.get("reset_behavior", "")],
        "snapshot-fields": (payload.get("snapshot") or {}).get("fields", []),
    }
    for field, values in semantic_text_fields.items():
        invalid_terms = sorted(
            {
                term
                for value in values
                for term in INVALID_SEMANTIC_TERMS
                if term in set(slug(value).split("-"))
            }
        )
        if invalid_terms:
            errors.append(f"invalid-semantic-{field}:{','.join(invalid_terms)}")
    state_roles = {_semantic_role_key(value) for value in payload.get("owned_state", [])}
    input_roles = {_semantic_role_key(value) for value in payload.get("inputs", [])}
    overlapping_roles = sorted(state_roles & input_roles)
    if overlapping_roles:
        errors.append(f"state-input-role-overlap:{','.join(overlapping_roles)}")
    reset_behavior = str(payload.get("reset_behavior", ""))
    if not _valid_reset_behavior(reset_behavior, payload.get("owned_state", [])):
        errors.append("generic-reset-behavior")
    if any(_placeholder_step(step) for step in payload.get("implementation_steps", [])):
        errors.append("placeholder-implementation-step")
    if any(_weak_implementation_step(step) for step in payload.get("implementation_steps", [])):
        errors.append("weak-implementation-step")
    implementation_source = (
        (payload.get("slots") or {}).get("implementation_steps") or {}
    ).get("source")
    if implementation_source not in {"intake-build-plan", "deterministic-contract-plan"}:
        errors.append("untrusted-implementation-plan")
    if any(_placeholder_test(test) for test in payload.get("tests", [])):
        errors.append("placeholder-test")
    required_tests = set(
        _contract_tests(
            payload.get("inputs", []),
            payload.get("outputs", []),
            str(payload.get("idempotency_key", "")),
        )
    )
    missing_contract_tests = sorted(required_tests - set(payload.get("tests", [])))
    if missing_contract_tests:
        errors.append(f"missing-contract-tests:{','.join(missing_contract_tests)}")
    if any(_weak_invariant(rule) for rule in payload.get("state_rules", [])):
        errors.append("weak-state-rule")
    if payload.get("atomic") is not True or not all(payload.get("atomic_evidence", {}).values()):
        errors.append("atomicity-not-proven")
    if payload.get("idempotent") is not True:
        errors.append("idempotency-not-proven")
    boundary = payload.get("renderer_boundary") or {}
    if any(boundary.get(key) for key in ["ownsDom", "ownsCanvas", "ownsThreeObjects"]):
        errors.append("renderer-owned")
    snapshot = payload.get("snapshot") or {}
    if not snapshot.get("supportsSnapshot") or not snapshot.get("supportsReset"):
        errors.append("snapshot-reset-incomplete")
    snapshot_fields = set(snapshot.get("fields") or [])
    missing_snapshot_fields = sorted(set(payload.get("owned_state", [])) - snapshot_fields)
    if missing_snapshot_fields:
        errors.append(f"snapshot-missing-owned-state:{','.join(missing_snapshot_fields)}")
    unexpected_snapshot_fields = sorted(snapshot_fields - set(payload.get("owned_state", [])))
    if unexpected_snapshot_fields:
        errors.append(f"snapshot-non-owned-state:{','.join(unexpected_snapshot_fields)}")
    try:
        json.dumps(record, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        errors.append("not-json-serializable")
    return {
        "ok": not errors,
        "record_id": record.get("record_id"),
        "errors": errors,
        "checks": len(REQUIRED_PAYLOAD_FIELDS) + 9,
    }


def apply_slot_status(record: Dict[str, Any], validation: Dict[str, Any]) -> Dict[str, Any]:
    result = deepcopy(record)
    status = "validated" if validation.get("ok") else "needs-repair"
    for slot in result.get("payload", {}).get("slots", {}).values():
        slot["status"] = status
    result["payload"]["status"] = "accepted" if validation.get("ok") else "needs-repair"
    return result


def score_benchmark_case(case: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    expected = case.get("expected") or {}
    if not expected:
        return {"coverage": 1.0, "checks": [], "passed": 0, "total": 0}
    payload = record.get("payload") or {}
    checks = []
    for field in ["owned_state", "inputs"]:
        expected_values = {slug(value) for value in expected.get(field, [])}
        actual_values = {slug(value) for value in payload.get(field, [])}
        matched = sorted(expected_values & actual_values)
        checks.append(
            {
                "name": field,
                "score": len(matched) / len(expected_values) if expected_values else 1.0,
                "expected": sorted(expected_values),
                "actual": sorted(actual_values),
                "matched": matched,
            }
        )
    for field, hint_field in [("requires", "requires_hint"), ("provides", "provides_hint")]:
        expected_values = set(clean_token_list(case.get(hint_field)))
        actual_values = set(payload.get(field, []))
        matched = sorted(expected_values & actual_values)
        checks.append(
            {
                "name": field,
                "score": len(matched) / len(expected_values) if expected_values else 1.0,
                "expected": sorted(expected_values),
                "actual": sorted(actual_values),
                "matched": matched,
            }
        )
    idempotency = slug(payload.get("idempotency_key", ""))
    terms = [slug(value) for value in expected.get("idempotency_terms", [])]
    matched_terms = [term for term in terms if term and term in idempotency]
    checks.append(
        {
            "name": "idempotency-key",
            "score": len(matched_terms) / len(terms) if terms else 1.0,
            "expected": terms,
            "actual": idempotency,
            "matched": matched_terms,
        }
    )
    checks.append(
        {
            "name": "implementation-plan",
            "score": 1.0 if len(payload.get("implementation_steps", [])) >= 3 else 0.0,
            "expected": "at least 3 steps",
            "actual": len(payload.get("implementation_steps", [])),
        }
    )
    coverage = sum(check["score"] for check in checks) / len(checks)
    return {
        "coverage": round(coverage, 4),
        "checks": checks,
        "passed": sum(1 for check in checks if check["score"] == 1.0),
        "total": len(checks),
    }


def compare_and_inject(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    comparisons = []
    injections = []
    for left_index, left in enumerate(records):
        left_payload = left.get("payload") or {}
        for right_index, right in enumerate(records):
            if left_index == right_index:
                continue
            right_payload = right.get("payload") or {}
            shared = sorted(set(left_payload.get("provides", [])) & set(right_payload.get("requires", [])))
            if not shared:
                continue
            comparison = {
                "left_record_id": left.get("record_id"),
                "right_record_id": right.get("record_id"),
                "relation": "provides-to",
                "shared_tokens": shared,
            }
            comparisons.append(comparison)
            for token in shared:
                injection_id = slug(f"{left['record_id']}|{right['record_id']}|{token}")
                injections.append(
                    {
                        "injection_id": f"kit-link-{injection_id}",
                        "type": "kit-link",
                        "relation": "provides-to",
                        "source_kit": left.get("record_id"),
                        "target_kit": right.get("record_id"),
                        "token": token,
                        "status": "validated",
                        "mutation_policy": "reference-only; source kits remain immutable",
                    }
                )
    return comparisons, injections


def parse_json_object(value: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    text = value.strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        text = fenced.group(1)
    else:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start : end + 1]
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, str(exc)
    if not isinstance(parsed, dict):
        return None, "model output was not a JSON object"
    return parsed, None


def infer_idempotency_key(description: str) -> str:
    text = " ".join(description.lower().split())
    match = re.search(
        r"(?:duplicate|repeated|repeating|same)\s+([a-z0-9 -]{1,48}?\bids?)\b",
        text,
    )
    if not match:
        return ""
    value = slug(match.group(1))
    for prefix in ["a-", "an-", "the-"]:
        if value.startswith(prefix):
            value = value[len(prefix) :]
            break
    return value[:-1] if value.endswith("-ids") else value


def infer_inputs(description: str) -> List[str]:
    text = " ".join(description.split())
    match = re.search(
        r"\baccepts?\s+(.+?)(?=,\s*(?:emits?|ignores?|supports?)\b|\. |\.$)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return []
    value = match.group(1)
    parts = re.split(r"\s+and\s+|\s*,\s*", value, flags=re.IGNORECASE)
    boundary_suffixes = {
        "action",
        "actions",
        "command",
        "commands",
        "event",
        "events",
        "message",
        "messages",
        "request",
        "requests",
        "signal",
        "signals",
        "update",
        "updates",
    }
    final_words = parts[-1].split() if parts else []
    shared_suffix = final_words[-1].lower() if final_words else ""
    if shared_suffix in boundary_suffixes:
        parts = [
            part
            if part.split() and part.split()[-1].lower() in boundary_suffixes
            else f"{part} {shared_suffix}"
            for part in parts
        ]
    value = ", ".join(parts)
    inputs = clean_list(value)
    if re.search(r"\bper\s+entity\b", text, flags=re.IGNORECASE):
        inputs.insert(0, "entity-id")
    return unique(inputs)


def infer_owned_state(description: str) -> List[str]:
    text = " ".join(description.split())
    match = re.search(
        r"\bowns?\s+(.+?)(?=\s+per\s+entity\b|[.;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return []
    value = re.sub(r"\s+and\s+", ", ", match.group(1), flags=re.IGNORECASE)
    return clean_list(value)


def infer_domain(description: str) -> str:
    text = " ".join(description.split())
    match = re.search(
        r"\b(?:is|as)\s+(?:an?\s+)?(?:atomic\s+)?([a-z][a-z -]{2,48}?)\s+(?:capability|domain|kit)\b",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    value = slug(match.group(1))
    return "" if value in GENERIC_DOMAINS else value


def infer_domain_from_title(title: str) -> str:
    ignored = {"a", "an", "kit", "the", "that", "tracks", "tracker"}
    parts = [part for part in slug(title).split("-") if part and part not in ignored]
    return "-".join(parts[:2]) or "guided-capability"


def _generic_domain(value: str) -> bool:
    normalized = slug(value)
    return not normalized or normalized in GENERIC_DOMAINS or normalized.endswith("-kit")


def infer_requires_from_inputs(inputs: List[str]) -> List[str]:
    requirements = []
    for value in inputs:
        token = slug(value)
        if token in {"entity", "entity-id", "target-entity"}:
            requirements.append("entity:identity")
        elif token in {"tick", "time-tick", "time-ticks"}:
            requirements.append("time:tick")
        elif token.endswith("-attempt") or token.endswith("-attempts"):
            requirements.append(f"attempt:{token}")
    return unique(requirements)


def infer_namespaced_tokens(description: str, keyword: str) -> List[str]:
    text = " ".join(description.split())
    match = re.search(
        rf"\b{re.escape(keyword)}\s+(.+?)(?=(?:\band\s+)?\b(?:requires|provides)\b|[.;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return []
    tokens = re.findall(r"\b[a-z][a-z0-9-]*:[a-z][a-z0-9-]*\b", match.group(1).lower())
    return clean_token_list(tokens)


def _placeholder_step(value: str) -> bool:
    text = str(value).strip().lower()
    if text.startswith("/") or text.endswith("/"):
        return True
    return any(token in text for token in ["extract intro", "extract behavior", "schema slot", "concrete step"])


def _weak_implementation_step(value: str) -> bool:
    words = [word for word in re.split(r"[_\s-]+", str(value).strip()) if word]
    return len(words) < 3


def _weak_invariant(value: str) -> bool:
    words = [word for word in re.split(r"[_\s-]+", str(value).strip()) if word]
    return len(words) < 3


def _placeholder_test(value: str) -> bool:
    text = slug(value)
    return bool(
        re.match(r"^(?:(?:unit|integration)-)?test-?\d+$", text)
        or re.match(r"^test-(?:case|scenario)-?\d+$", text)
    )


def _contract_tests(inputs: List[str], outputs: List[str], idempotency_key: str) -> List[str]:
    return unique(
        [
            *[f"{value}-input-transition" for value in inputs],
            *[f"{value}-output-contract" for value in outputs],
            f"{slug(idempotency_key)}-duplicate-replay",
            "snapshot-round-trip-all-owned-state",
            "reset-restores-configured-defaults",
            "contract-shape-check",
        ]
    )


def _placeholder_contract_token(value: str) -> bool:
    token = str(value).split(":", 1)[-1]
    return bool(
        token == "intake-slot"
        or re.search(r"(?:field|slot|input|output)-?\d+$", token)
        or re.search(r"-\d{2,}$", token)
    )


def _valid_reset_behavior(value: str, owned_state: Optional[List[str]] = None) -> bool:
    normalized = re.sub(r"[_-]+", " ", str(value).lower())
    reset_terms = {"clear", "clears", "delete", "empty", "remove", "restore", "restores", "reset", "resets", "zero"}
    words = set(normalized.split())
    named_state = all(
        re.sub(r"[_-]+", " ", slug(state)) in normalized
        for state in (owned_state or [])
        if slug(state)
    )
    return bool(owned_state) and len(words) >= 3 and bool(words & reset_terms) and named_state


def correct_semantic_terms(values: List[str], description: str) -> Tuple[List[str], bool]:
    description_slug = slug(description)
    output = []
    corrected = False
    for value in values:
        normalized = slug(value)
        for invalid, replacement in SEMANTIC_CORRECTIONS.items():
            if invalid in normalized.split("-") and replacement in description_slug.split("-"):
                normalized = "-".join(
                    replacement if part == invalid else part
                    for part in normalized.split("-")
                )
                corrected = True
        output.append(normalized)
    return unique(output), corrected


def correct_semantic_sentence(value: str, description: str) -> Tuple[str, bool]:
    output = str(value)
    description_terms = set(slug(description).split("-"))
    corrected = False
    for invalid, replacement in SEMANTIC_CORRECTIONS.items():
        if replacement not in description_terms:
            continue
        updated = re.sub(
            rf"(?<![a-z]){re.escape(invalid)}(?![a-z])",
            replacement,
            output,
            flags=re.IGNORECASE,
        )
        corrected = corrected or updated != output
        output = updated
    return output, corrected


def correct_semantic_sentences(values: List[str], description: str) -> Tuple[List[str], bool]:
    output = []
    corrected = False
    for value in values:
        updated, item_corrected = correct_semantic_sentence(value, description)
        output.append(updated)
        corrected = corrected or item_corrected
    return unique(output), corrected


def _semantic_role_key(value: str) -> str:
    parts = slug(value).split("-")
    singular = [part[:-1] if part.endswith("s") and len(part) > 3 else part for part in parts]
    return "-".join(singular)


def clean_list(value: Any) -> List[str]:
    if isinstance(value, str):
        values: Iterable[Any] = re.split(r",|\n", value)
    elif isinstance(value, list):
        values = value
    else:
        values = []
    return unique(slug(item) for item in values if str(item).strip())


def clean_token_list(value: Any) -> List[str]:
    values = clean_list_preserve_colon(value)
    output = []
    for item in values:
        if ":" not in item:
            output.append(slug(item))
            continue
        namespace, token = item.split(":", 1)
        output.append(f"{slug(namespace)}:{slug(token)}")
    return unique(output)


def namespace_tokens(values: List[str], namespace: str) -> List[str]:
    normalized_namespace = slug(namespace)
    if not normalized_namespace:
        return values
    output = []
    for item in values:
        if ":" in item:
            output.append(item)
            continue
        token = slug(item)
        output.append(f"{normalized_namespace}:{token}" if token else item)
    return unique(output)


def clean_list_preserve_colon(value: Any) -> List[str]:
    if isinstance(value, str):
        values: Iterable[Any] = re.split(r",|\n", value)
    elif isinstance(value, list):
        values = value
    else:
        values = []
    return unique(str(item).strip().lower().strip(" ./") for item in values if str(item).strip())


def clean_sentences(value: Any) -> List[str]:
    if isinstance(value, str):
        values: Iterable[Any] = re.split(r"\n|;", value)
    elif isinstance(value, list):
        values = value
    else:
        values = []
    return unique(clean_sentence(item) for item in values if clean_sentence(item))


def clean_sentence(value: Any) -> str:
    return " ".join(str(value or "").strip(" .;:\n\t").split())[:320]


def unique(values: Iterable[str]) -> List[str]:
    seen = set()
    output = []
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value).lower()).strip("-")


def camel(value: Any) -> str:
    parts = [part for part in slug(value).split("-") if part]
    return parts[0] + "".join(part.capitalize() for part in parts[1:]) if parts else "kit"


def pascal(value: Any) -> str:
    return "".join(part.capitalize() for part in slug(value).split("-") if part)
