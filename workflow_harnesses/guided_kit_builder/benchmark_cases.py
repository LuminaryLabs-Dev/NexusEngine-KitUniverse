from __future__ import annotations

from typing import Any, Dict, List


BENCHMARK_CASES: List[Dict[str, Any]] = [
    {
        "idea_id": "round-timer",
        "title": "Round Timer",
        "domain_hint": "simulation-timing",
        "requires_hint": ["clock:delta"],
        "provides_hint": ["time:tick", "time:expired"],
        "description": (
            "Own remaining round time and running state. Accept start, pause, resume, and tick inputs. "
            "Emit timer updates and expiration. Reset restores configured duration and stopped state. "
            "Snapshot remaining time and running state. Duplicate tick event ids must not apply twice."
        ),
        "build_plan": [
            "define remaining-time and running state",
            "reduce start pause resume and tick inputs",
            "emit tick and expired events",
            "prove duplicate tick ids are ignored",
        ],
        "expected": {
            "owned_state": ["remaining-time", "running"],
            "inputs": ["start", "pause", "resume", "tick"],
            "idempotency_terms": ["tick", "id"],
        },
    },
    {
        "idea_id": "cooldown-gate",
        "title": "Cooldown Gate",
        "domain_hint": "simulation-cooldown",
        "requires_hint": ["time:tick"],
        "provides_hint": ["cooldown:ready", "cooldown:changed"],
        "description": (
            "Own cooldown expiry by semantic action id. Accept arm, cancel, and tick inputs. Emit ready and "
            "changed outputs. Reset clears every armed cooldown. Snapshot action ids and remaining durations. "
            "Repeated arm request ids must not extend a cooldown twice."
        ),
        "build_plan": [
            "store cooldown entries by action id",
            "reduce arm cancel and tick inputs",
            "emit readiness transitions",
            "replay request ids to prove idempotency",
        ],
        "expected": {
            "owned_state": ["cooldown-entries"],
            "inputs": ["arm", "cancel", "tick"],
            "idempotency_terms": ["request", "id"],
        },
    },
    {
        "idea_id": "score-ledger",
        "title": "Score Ledger",
        "domain_hint": "progression-score",
        "requires_hint": ["round:result"],
        "provides_hint": ["score:changed", "score:state"],
        "description": (
            "Own score totals per participant. Accept award, deduct, and reset-participant inputs. Emit score "
            "changes and queryable score state. Reset clears the ledger. Snapshot participant totals and applied "
            "award ids. The same award id must only change score once."
        ),
        "build_plan": [
            "store participant score totals and applied award ids",
            "reduce award deduct and reset inputs",
            "emit score changes",
            "replay award ids against snapshots",
        ],
        "expected": {
            "owned_state": ["participant-totals", "applied-award-ids"],
            "inputs": ["award", "deduct", "reset-participant"],
            "idempotency_terms": ["award", "id"],
        },
    },
    {
        "idea_id": "objective-tracker",
        "title": "Objective Tracker",
        "domain_hint": "progression-objective",
        "requires_hint": ["score:changed", "inventory:changed"],
        "provides_hint": ["objective:completed", "objective:state"],
        "description": (
            "Own objective progress and completion state. Accept progress facts, reset-objective, and activate "
            "inputs. Emit objective state and completion. Reset restores configured objectives. Snapshot active "
            "objectives, progress, and completion. Duplicate fact ids must not advance progress twice."
        ),
        "build_plan": [
            "store objective definitions progress and completion",
            "reduce progress facts by objective id",
            "emit completion transitions",
            "prove duplicate facts do not advance progress",
        ],
        "expected": {
            "owned_state": ["objective-progress", "completion-state"],
            "inputs": ["progress-fact", "reset-objective", "activate"],
            "idempotency_terms": ["fact", "id"],
        },
    },
    {
        "idea_id": "inventory-transfer",
        "title": "Inventory Transfer",
        "domain_hint": "economy-inventory",
        "requires_hint": ["entity:identity"],
        "provides_hint": ["inventory:changed", "inventory:state"],
        "description": (
            "Own item quantities for inventory containers. Accept add, remove, and transfer commands. Emit "
            "inventory changes and queryable state. Reset restores the initial inventory manifest. Snapshot item "
            "quantities and applied command ids. A repeated transfer command id must not move items twice."
        ),
        "build_plan": [
            "store item quantities per inventory id",
            "validate and reduce add remove and transfer commands",
            "emit one change event per accepted command",
            "replay transfer command ids against snapshots",
        ],
        "expected": {
            "owned_state": ["item-quantities", "applied-command-ids"],
            "inputs": ["add", "remove", "transfer"],
            "idempotency_terms": ["command", "id"],
        },
    },
    {
        "idea_id": "checkpoint-snapshot",
        "title": "Checkpoint Snapshot",
        "domain_hint": "data-checkpoint",
        "requires_hint": ["inventory:state", "score:state"],
        "provides_hint": ["checkpoint:saved", "checkpoint:restored"],
        "description": (
            "Own named checkpoint envelopes and their source version. Accept save, restore, and delete commands. "
            "Emit saved and restored results. Reset removes runtime checkpoints and restores configured defaults. "
            "Snapshot checkpoint metadata. Repeating a save command id must replace rather than duplicate a checkpoint."
        ),
        "build_plan": [
            "store versioned checkpoint envelopes by name",
            "capture required state providers on save",
            "restore envelopes through provider adapters",
            "prove save command replay replaces the same checkpoint",
        ],
        "expected": {
            "owned_state": ["checkpoint-envelopes", "source-version"],
            "inputs": ["save", "restore", "delete"],
            "idempotency_terms": ["save", "command", "id"],
        },
    },
    {
        "idea_id": "interaction-prompt",
        "title": "Interaction Prompt",
        "domain_hint": "interaction-prompt",
        "requires_hint": ["interaction:focus"],
        "provides_hint": ["interaction:selected", "interaction:prompt"],
        "description": (
            "Own the currently available interaction choices for a focused target. Accept focus, clear, and "
            "select inputs. Emit prompt descriptors and selected actions. Reset clears focus and choices. Snapshot "
            "the focused target and choice ids. Duplicate selection ids must emit only one selected action."
        ),
        "build_plan": [
            "store focused target and available choice descriptors",
            "reduce focus clear and select inputs",
            "emit renderer-agnostic prompt descriptors",
            "replay selection ids to prove one selected action",
        ],
        "expected": {
            "owned_state": ["focused-target", "choice-ids"],
            "inputs": ["focus", "clear", "select"],
            "idempotency_terms": ["selection", "id"],
        },
    },
    {
        "idea_id": "dialogue-choice",
        "title": "Dialogue Choice",
        "domain_hint": "narrative-dialogue",
        "requires_hint": ["interaction:selected"],
        "provides_hint": ["dialogue:selected", "dialogue:state"],
        "description": (
            "Own the active dialogue node and selected choice history. Accept begin, choose, and end inputs. Emit "
            "dialogue state and selected-choice events. Reset returns to no active dialogue. Snapshot the node and "
            "choice history. Repeating a choice event id must not advance dialogue twice."
        ),
        "build_plan": [
            "store active node and selected choice history",
            "reduce begin choose and end inputs",
            "emit dialogue transitions",
            "replay choice event ids against the dialogue state",
        ],
        "expected": {
            "owned_state": ["active-dialogue-node", "choice-history"],
            "inputs": ["begin", "choose", "end"],
            "idempotency_terms": ["choice", "event", "id"],
        },
    },
    {
        "idea_id": "spawn-budget",
        "title": "Spawn Budget",
        "domain_hint": "ecology-spawn",
        "requires_hint": ["time:tick"],
        "provides_hint": ["spawn:allowed", "spawn:budget"],
        "description": (
            "Own available spawn capacity by zone. Accept configure, consume, release, and tick inputs. Emit allowed "
            "decisions and budget changes. Reset restores configured zone capacities. Snapshot capacity and pending "
            "reservations. A repeated consume request id must not spend capacity twice."
        ),
        "build_plan": [
            "store capacity and reservations per zone",
            "reduce configure consume release and tick inputs",
            "emit budget changes and allowed decisions",
            "replay consume ids to prove capacity is spent once",
        ],
        "expected": {
            "owned_state": ["zone-capacity", "pending-reservations"],
            "inputs": ["configure", "consume", "release", "tick"],
            "idempotency_terms": ["consume", "request", "id"],
        },
    },
    {
        "idea_id": "camera-descriptor",
        "title": "Camera Descriptor",
        "domain_hint": "camera-framing",
        "requires_hint": ["entity:transform"],
        "provides_hint": ["camera:descriptor", "camera:changed"],
        "description": (
            "Own active camera framing intent without owning a renderer or camera object. Accept follow, frame, "
            "shake, and clear inputs. Emit camera descriptors and change events. Reset clears active intent. Snapshot "
            "the target, framing mode, and shake state. Repeating a camera command id must not stack effects twice."
        ),
        "build_plan": [
            "store renderer-agnostic framing intent",
            "reduce follow frame shake and clear commands",
            "emit immutable camera descriptors",
            "prove camera command replay does not stack effects",
        ],
        "expected": {
            "owned_state": ["camera-framing-intent", "shake-state"],
            "inputs": ["follow", "frame", "shake", "clear"],
            "idempotency_terms": ["camera", "command", "id"],
        },
    },
]
