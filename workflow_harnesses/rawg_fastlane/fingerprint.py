from __future__ import annotations

from typing import Any, Dict, Iterable, List, Set

from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash
from workflow_harnesses.rawg_matrix_optimizer.workflow_rawg_matrix_optimizer import (
    MATRIX_ACTION_MARKERS,
    MATRIX_MECHANIC_TAG_MARKERS,
    _stem_token,
)


DOMAIN_RULES = {
    "combat": {"attack", "battle", "block", "combat", "damage", "dodge", "enemy", "fight", "stun", "weapon"},
    "movement": {"climb", "dash", "fly", "jump", "move", "parkour", "roll", "swim", "travel"},
    "progression": {"experience", "level", "progress", "reward", "skill", "upgrade", "unlock"},
    "inventory": {"collect", "equip", "inventory", "item", "loot", "resource", "store"},
    "crafting": {"build", "craft", "recipe", "repair"},
    "choice": {"choose", "choice", "consequence", "dialogue", "ending", "morality", "relationship"},
    "multiplayer": {"co-op", "lobby", "matchmaking", "multiplayer", "online", "party", "pvp", "session"},
    "simulation": {"automation", "economy", "manage", "physics", "simulate", "trade"},
    "world-interaction": {"destroy", "explore", "hack", "interact", "puzzle", "solve", "stealth", "survival"},
}
OBJECT_TERMS = {
    token for values in DOMAIN_RULES.values() for token in values
} | {"ability", "base", "card", "character", "checkpoint", "currency", "deck", "damage", "ending", "gear", "health", "path", "quest", "save", "score", "stamina", "surface", "timer"}


def fingerprint_record(record: Dict[str, Any], profile: str) -> Dict[str, Any]:
    tokens = _unit_tokens(record.get("evidence_units") or [])
    actions = sorted(tokens & MATRIX_ACTION_MARKERS)
    mechanics = sorted(token for token in tokens if any(marker == token or marker in token for marker in MATRIX_MECHANIC_TAG_MARKERS))
    objects = sorted(tokens & OBJECT_TERMS)
    domains = sorted(name for name, terms in DOMAIN_RULES.items() if tokens & terms)
    if profile in {"coarse", "stratified"}:
        signature = domains
    elif profile == "balanced":
        signature = [*domains, *actions[:8], *mechanics[:8]]
    elif profile == "fine":
        signature = [*domains, *actions[:12], *mechanics[:12], *objects[:16]]
    else:
        raise ValueError(f"unknown fingerprint profile: {profile}")
    signature = list(dict.fromkeys(signature))
    status = "expandable" if signature else "insufficient-evidence"
    fingerprint = stable_hash(["rawg.fast-fingerprint.v1", profile, signature, status])
    secondary_signature = list(dict.fromkeys([*domains, *actions[:8], *mechanics[:8], *objects[:12]]))
    return {
        "schema_version": "rawg.fast-fingerprint.v1",
        "fingerprint_profile": profile,
        "fingerprint": fingerprint,
        "secondary_fingerprint": stable_hash(["rawg.fast-secondary.v1", secondary_signature, status]),
        "secondary_signature": secondary_signature,
        "signature": signature,
        "domains": domains,
        "actions": actions,
        "mechanics": mechanics,
        "status": status,
    }


def _unit_tokens(units: Iterable[Dict[str, Any]]) -> Set[str]:
    output = set()
    for unit in units:
        for token in slug(unit.get("text")).split("-"):
            if token:
                output.add(_stem_token(token))
    return output
