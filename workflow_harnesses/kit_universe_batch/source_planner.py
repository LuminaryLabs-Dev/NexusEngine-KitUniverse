from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional


OPERATIONS = [
    "register", "select", "reserve", "schedule", "dispatch", "apply",
    "confirm", "release", "recover", "audit", "route", "resolve",
]
BOUNDARIES = [
    "request", "state", "event", "policy", "queue", "result",
    "checkpoint", "transition", "eligibility", "allocation", "history", "signal",
]
LIFECYCLES = ["intake", "pending", "active", "completion", "failure", "reset", "replay", "handoff"]
GENERIC_WORDS = {"a", "an", "and", "for", "game", "idea", "of", "the", "with"}
OPERATION_RULES = {
    "register": "accept a new {focus} registration only when its semantic key is absent",
    "select": "choose the eligible {focus} candidate with highest priority and stable arrival-order ties",
    "reserve": "reserve {focus} capacity only when no active reservation owns the same target",
    "schedule": "order {focus} work by due time and stable request order",
    "dispatch": "dispatch {focus} work to the first compatible destination with available capacity",
    "apply": "apply the {focus} change only when its expected version matches current state",
    "confirm": "confirm {focus} completion only after every required result is present",
    "release": "release {focus} ownership exactly once and make the capacity available again",
    "recover": "restore {focus} work from the latest valid checkpoint without replaying applied events",
    "audit": "emit a {focus} finding when observed state violates a declared invariant",
    "route": "route {focus} work to the highest-priority compatible destination",
    "resolve": "resolve competing {focus} outcomes by priority and stable event order",
}
BOUNDARY_RULES = {
    "request": "reject requests missing an actor, target, or request id",
    "state": "reject stale state versions and preserve the current snapshot",
    "event": "emit the result only after the owned state transition succeeds",
    "policy": "record the policy id and decision reason used for every outcome",
    "queue": "preserve FIFO order among entries with equal priority",
    "result": "store success or failure with a stable reason code",
    "checkpoint": "bind every checkpoint to a source version and snapshot id",
    "transition": "allow only declared from-state to-state edges",
    "eligibility": "record every criterion and the final eligibility decision",
    "allocation": "never allocate more than the configured capacity",
    "history": "append immutable history entries without rewriting prior events",
    "signal": "deliver one signal per subscriber and event id",
}
LIFECYCLE_RULES = {
    "intake": "validate source identity before creating pending state",
    "pending": "retain pending work until completion, cancellation, or expiry",
    "active": "permit one active owner for each semantic target",
    "completion": "finalize state before publishing completion",
    "failure": "preserve failure reason and retry eligibility",
    "reset": "clear runtime work while restoring configured defaults",
    "replay": "ignore event ids already present in applied-event-ids",
    "handoff": "transfer ownership only after the receiving boundary acknowledges it",
}


def slug(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return text or "source"


def load_sources(source: Optional[str], source_file: Optional[Path]) -> List[Dict[str, Any]]:
    if bool(source) == bool(source_file):
        raise ValueError("provide exactly one of --source or --source-file")
    if source is not None:
        return [_normalize_source({"description": source})]
    assert source_file is not None
    text = source_file.read_text(encoding="utf-8")
    suffix = source_file.suffix.lower()
    if suffix == ".jsonl":
        values = [json.loads(line) for line in text.splitlines() if line.strip()]
    elif suffix == ".json":
        parsed = json.loads(text)
        values = parsed if isinstance(parsed, list) else [parsed]
    else:
        values = [{"name": source_file.stem, "description": text.strip()}]
    if not values:
        raise ValueError("source file did not contain any source records")
    return [_normalize_source(value) for value in values]


def _normalize_source(value: Any) -> Dict[str, Any]:
    if isinstance(value, str):
        value = {"description": value}
    if not isinstance(value, dict):
        raise ValueError("source records must be strings or JSON objects")
    description = str(value.get("description") or "").strip()
    if not description:
        raise ValueError("every source record requires a non-empty description")
    words = [word for word in slug(description).split("-") if word not in GENERIC_WORDS]
    inferred_name = " ".join(words[:4]) or "Kit Source"
    name = str(value.get("name") or inferred_name).strip()
    source_id = slug(value.get("source_id") or name)
    source_context = value.get("source_context") or value.get("provenance") or {}
    return {
        "source_id": source_id,
        "name": name,
        "description": description,
        "constraints": [str(item) for item in value.get("constraints", [])],
        "seed_domains": [slug(item) for item in value.get("seed_domains", [])],
        "focus_terms": list(dict.fromkeys(words))[:24] or [source_id],
        "source_hash": hashlib.sha256(description.encode("utf-8")).hexdigest(),
        "source_context": source_context if isinstance(source_context, dict) else {},
    }


def build_matrix(sources: List[Dict[str, Any]], size: int) -> List[Dict[str, Any]]:
    matrix: List[Dict[str, Any]] = []
    previous_provides: Dict[str, str] = {}
    for index in range(size):
        source = sources[index % len(sources)]
        source_id = source["source_id"]
        local_index = index // len(sources)
        operation = OPERATIONS[local_index % len(OPERATIONS)]
        boundary = BOUNDARIES[(local_index // len(OPERATIONS)) % len(BOUNDARIES)]
        lifecycle = LIFECYCLES[
            (local_index // (len(OPERATIONS) * len(BOUNDARIES))) % len(LIFECYCLES)
        ]
        layer = local_index // (len(OPERATIONS) * len(BOUNDARIES) * len(LIFECYCLES))
        subject = slug(source["name"])
        focus = source["focus_terms"][local_index % len(source["focus_terms"])]
        domain = slug(f"{subject}-{focus}-{operation}")
        token_suffix = slug(f"{focus}-{lifecycle}-{operation}-{boundary}-{layer}")
        provides = f"{subject}:{token_suffix}"
        requires = previous_provides.get(source_id, f"{subject}:source-ready")
        previous_provides[source_id] = provides
        owned_state = [
            slug(f"{focus}-{operation}-{boundary}-records"),
            slug(f"{focus}-{lifecycle}-state-{layer}"),
            "applied-event-ids",
        ]
        inputs = [slug(f"{requires.split(':', 1)[-1]}-events"), slug(f"{lifecycle}-control-events")]
        output = provides.split(":", 1)[-1]
        behavior_rules = [
            OPERATION_RULES[operation].format(focus=focus),
            BOUNDARY_RULES[boundary],
            LIFECYCLE_RULES[lifecycle],
        ]
        title = " ".join(
            word.capitalize()
            for word in f"{source['name']} {focus} {lifecycle} {operation} {boundary} {layer}".split()
        )
        description = (
            f"A {source['name']} {lifecycle} {operation} {boundary} kit that owns "
            f"{', '.join(owned_state[:-1])}, and applied event ids. It accepts "
            f"{inputs[0]} and {inputs[1]}. It emits {output} events. Requires {requires}. "
            f"Provides {provides}. Duplicate event ids are ignored. Reset clears "
            f"{', '.join(owned_state[:-1])} and snapshot preserves all owned state. "
            f"Behavior rules: {'; '.join(behavior_rules)}."
        )
        matrix.append(
            {
                "matrix_id": f"matrix-{index:06d}",
                "index": index,
                "source_id": source_id,
                "source_hash": source["source_hash"],
                "operation": operation,
                "boundary": boundary,
                "lifecycle": lifecycle,
                "focus": focus,
                "layer": layer,
                "case": {
                    "idea_id": slug(title),
                    "title": title,
                    "description": description,
                    "domain_hint": domain,
                    "requires_hint": [requires],
                    "provides_hint": [provides],
                    "slot_hints": {
                        "owned_state": owned_state,
                        "inputs": inputs,
                        "outputs": [output],
                        "idempotency_key": "event-id",
                    },
                    "build_plan": [],
                    "behavior_rules": behavior_rules,
                    "expected": {},
                    "source_context": source.get("source_context", {}),
                },
            }
        )
    return matrix
