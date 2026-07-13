from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Sequence

from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash

from .contracts import (
    GAME_EVIDENCE_SCHEMA,
    GAME_MAP_SCHEMA,
    INTERACTION_SCHEMA,
    KIT_OBSERVATION_SCHEMA,
    interaction_identity,
    semantic_key,
)


DOMAIN_TERMS = {
    "movement": {"move", "movement", "walk", "run", "jump", "dash", "climb", "fly", "swim", "traversal"},
    "combat": {"attack", "battle", "combat", "fight", "weapon", "damage", "block", "dodge", "parry"},
    "effects": {"effect", "status", "buff", "debuff", "poison", "burn", "freeze", "stun", "shock", "aura", "curse"},
    "abilities": {"ability", "skill", "spell", "cast", "power", "cooldown", "mana", "stamina"},
    "health": {"health", "heal", "healing", "death", "defeat", "revive", "respawn", "life"},
    "inventory": {"inventory", "item", "equipment", "equip", "gear", "slot", "stack", "collect"},
    "crafting": {"craft", "crafting", "recipe", "build", "construct", "repair", "upgrade"},
    "economy": {"economy", "currency", "buy", "sell", "trade", "cost", "shop", "market", "resource"},
    "progression": {"progress", "progression", "level", "experience", "unlock", "rank", "reward"},
    "objectives": {"quest", "objective", "mission", "goal", "challenge", "achievement"},
    "dialogue": {"dialogue", "conversation", "choice", "response", "branch", "narrative", "story"},
    "social": {"relationship", "faction", "party", "reputation", "companion", "cooperative", "multiplayer"},
    "world": {"world", "zone", "region", "terrain", "environment", "weather", "day", "night", "time"},
    "simulation": {"simulate", "simulation", "physics", "system", "automation", "production", "population"},
    "agents": {"enemy", "npc", "ai", "behavior", "stealth", "detect", "threat", "boss"},
    "puzzles": {"puzzle", "solve", "logic", "riddle", "match", "maze"},
    "cards": {"card", "deck", "hand", "draw", "discard"},
    "vehicles": {"vehicle", "car", "ship", "drive", "race", "flight"},
    "network": {"network", "online", "server", "client", "matchmaking", "synchronize"},
    "presentation": {"camera", "render", "graphic", "animation", "audio", "sound", "music", "interface", "hud"},
    "persistence": {"save", "load", "snapshot", "replay", "checkpoint", "reset"},
}

ACTION_WORDS = {
    "activate", "adapt", "add", "aim", "allocate", "apply", "assign", "attach", "attack", "avoid", "award",
    "bind", "block", "boost", "bounce", "break", "breed", "build", "buy", "cancel", "capture", "cast", "charge",
    "choose", "clear", "climb", "close", "collect", "combine", "consume", "cool", "copy", "counter", "craft",
    "create", "damage", "decay", "defeat", "deplete", "destroy", "detect", "disable", "discard", "discover",
    "dodge", "draw", "drive", "drop", "earn", "equip", "exchange", "explore", "feed", "fight", "form", "freeze",
    "gain", "gather", "generate", "grant", "harvest", "heal", "hide", "hold", "ignite", "increase", "interact",
    "join", "jump", "launch", "learn", "load", "lock", "lose", "manage", "merge", "mine", "modify", "move",
    "open", "parry", "place", "possess", "prevent", "produce", "progress", "protect", "push", "queue", "regenerate",
    "remove", "repair", "reset", "resist", "resolve", "restore", "reveal", "revive", "reward", "rotate", "route",
    "save", "scale", "sell", "share", "shoot", "slow", "solve", "spawn", "spend", "stack", "steal", "stun",
    "summon", "survive", "switch", "teleport", "trade", "transform", "transport", "trap", "travel", "trigger",
    "unlock", "upgrade", "use", "validate", "wear", "win",
}

EFFECT_WORDS = {
    "buff", "burn", "cancel", "curse", "damage", "debuff", "defeat", "destroy", "freeze", "heal", "lock",
    "poison", "reward", "shock", "spawn", "stun", "unlock",
}

STOP_TARGETS = {"a", "an", "and", "as", "at", "by", "for", "from", "in", "into", "of", "on", "or", "the", "to", "with"}

TARGET_ALIASES = {
    "abilities": "ability", "ability": "ability", "attacks": "attack", "attack": "attack",
    "boss": "boss", "bosses": "boss", "cards": "card", "card": "card", "characters": "character", "character": "character",
    "choices": "choice", "choice": "choice", "classes": "class", "class": "class", "coins": "currency", "coin": "currency",
    "currency": "currency", "deck": "deck", "decks": "deck", "dialogue": "dialogue", "dungeons": "dungeon", "dungeon": "dungeon",
    "enemies": "enemy", "enemy": "enemy", "equipment": "equipment", "experience": "experience", "food": "food",
    "gear": "gear", "items": "item", "item": "item", "levels": "level", "level": "level", "loot": "loot",
    "missions": "mission", "mission": "mission", "moons": "moon", "moon": "moon", "objects": "object", "object": "object",
    "origins": "origin", "origin": "origin", "players": "player", "player": "player", "platforms": "platform", "platform": "platform",
    "puzzles": "puzzle", "puzzle": "puzzle", "quests": "quest", "quest": "quest", "races": "race", "race": "race",
    "recipes": "recipe", "recipe": "recipe", "resources": "resource", "resource": "resource", "rewards": "reward", "reward": "reward",
    "skills": "skill", "skill": "skill", "stars": "star", "star": "star", "stories": "story", "story": "story",
    "surfaces": "surface", "surface": "surface", "traps": "trap", "trap": "trap", "walls": "surface", "wall": "surface",
    "weapons": "weapon", "weapon": "weapon", "world": "world", "worlds": "world", "zones": "zone", "zone": "zone",
    "city": "city", "cities": "city", "galaxy": "galaxy", "galaxies": "galaxy", "artifacts": "artifact", "artifact": "artifact",
    "attacks": "attack", "damage": "damage", "health": "health", "relationships": "relationship", "relationship": "relationship",
    "seal": "seal", "seals": "seal", "spells": "spell", "spell": "spell", "state": "state", "states": "state",
    "mountain": "mountain", "mountains": "mountain", "path": "path", "paths": "path", "upgrades": "upgrade", "upgrade": "upgrade",
    "party": "party", "parties": "party", "platforms": "platform", "platform": "platform", "seal": "seal", "seals": "seal",
    "environment": "environment", "environments": "environment", "kingdom": "world", "kingdoms": "world",
    "ammo": "ammunition", "ammunition": "ammunition", "armor": "armor", "armour": "armor", "bases": "base", "base": "base",
    "buildings": "building", "building": "building", "companions": "companion", "companion": "companion", "doors": "door", "door": "door",
    "effects": "status-effect", "effect": "status-effect", "energy": "energy", "factions": "faction", "faction": "faction",
    "gates": "gate", "gate": "gate", "habitats": "habitat", "habitat": "habitat", "keys": "key", "key": "key",
    "mana": "mana", "npcs": "npc", "npc": "npc", "positions": "position", "position": "position", "projectiles": "projectile", "projectile": "projectile",
    "reputation": "reputation", "ships": "vehicle", "ship": "vehicle", "stamina": "stamina", "statuses": "status-effect", "status": "status-effect",
    "teams": "team", "team": "team", "terrain": "terrain", "time": "time", "timers": "timer", "timer": "timer",
    "turns": "turn", "turn": "turn", "units": "unit", "unit": "unit", "vehicles": "vehicle", "vehicle": "vehicle",
    "weather": "weather", "waves": "wave", "wave": "wave", "zones": "zone", "zone": "zone",
}

IRREGULAR_ACTIONS = {
    "built": "build", "bought": "buy", "chose": "choose", "chosen": "choose", "fought": "fight",
    "fed": "feed", "froze": "freeze", "frozen": "freeze", "gained": "gain", "held": "hold", "lost": "lose",
    "shot": "shoot", "spent": "spend", "stole": "steal", "stolen": "steal", "wore": "wear", "won": "win",
}

ACTION_TARGETS = {
    "attack": {"enemy", "boss", "player", "object"}, "block": {"attack", "damage"}, "break": {"object", "surface", "seal"},
    "build": {"item", "weapon", "world", "city", "deck", "relationship", "story"}, "buy": {"item", "weapon", "card", "upgrade"},
    "cast": {"ability", "skill", "spell"}, "choose": {"character", "class", "race", "origin", "choice", "dialogue", "item", "card", "weapon"},
    "climb": {"surface", "wall", "mountain", "path"}, "collect": {"item", "resource", "currency", "loot", "card", "star", "moon", "artifact"},
    "craft": {"item", "weapon", "equipment", "gear", "recipe", "card", "deck", "story"}, "damage": {"enemy", "boss", "player", "object"},
    "defeat": {"enemy", "boss", "player"}, "destroy": {"enemy", "boss", "object", "weapon", "world"}, "dodge": {"attack", "enemy", "trap"},
    "draw": {"card", "weapon"}, "equip": {"item", "weapon", "equipment", "gear", "ability"},
    "explore": {"world", "zone", "city", "galaxy", "dungeon", "level"}, "fight": {"enemy", "boss", "player"},
    "freeze": {"enemy", "boss", "player", "object"}, "heal": {"player", "character", "health"}, "interact": {"object", "world", "character", "dialogue"},
    "repair": {"item", "weapon", "equipment", "gear", "object"}, "save": {"state", "player", "character", "world"},
    "sell": {"item", "weapon", "card", "resource"}, "solve": {"puzzle", "quest"}, "stun": {"enemy", "boss", "player"},
    "trade": {"item", "weapon", "card", "resource", "currency"}, "unlock": {"ability", "skill", "item", "weapon", "card", "level", "world"},
    "upgrade": {"ability", "skill", "item", "weapon", "card", "equipment", "gear"},
    "create": {"platform", "level", "world", "story", "item", "character"}, "gather": {"party", "resource", "item"},
    "interact": {"object", "world", "character", "dialogue"}, "restore": {"health", "state", "item"},
    "travel": {"world", "zone", "city", "level"}, "use": {"weapon", "ability", "skill", "item", "object", "environment"},
    "save": {"state", "player", "character", "world"},
}


def _words(text: str) -> List[str]:
    return re.findall(r"[a-z0-9][a-z0-9'-]*", text.lower())


def canonical_action(word: str) -> str:
    value = slug(word)
    if value in ACTION_WORDS:
        return value
    if value in IRREGULAR_ACTIONS:
        return IRREGULAR_ACTIONS[value]
    candidates = []
    if value.endswith("ing") and len(value) > 5:
        candidates.extend([value[:-3], value[:-3] + "e"])
    if value.endswith("ied") and len(value) > 5:
        candidates.append(value[:-3] + "y")
    if value.endswith("ed") and len(value) > 4:
        candidates.extend([value[:-2], value[:-1]])
    for candidate in candidates:
        if len(candidate) > 2 and candidate[-1:] == candidate[-2:-1]:
            candidate = candidate[:-1]
        if candidate in ACTION_WORDS:
            return candidate
    return ""


def _sentences(text: str) -> Iterable[str]:
    for sentence in re.split(r"(?<=[.!?])\s+|[\r\n]+", text):
        sentence = " ".join(sentence.split()).strip()
        if sentence:
            yield sentence


def evidence_units(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    units: List[Dict[str, Any]] = []
    for index, sentence in enumerate(_sentences(str(source.get("description") or ""))):
        units.append(_evidence_unit(source, "description", sentence, index))
    for field in ("genres", "tags"):
        for index, value in enumerate(source.get(field) or []):
            text = str(value).strip()
            if text:
                units.append(_evidence_unit(source, field, text, index))
    return units


def _evidence_unit(source: Dict[str, Any], field: str, text: str, index: int) -> Dict[str, Any]:
    tokens = set(_words(text))
    domains = sorted(domain for domain, terms in DOMAIN_TERMS.items() if tokens & terms)
    actions = sorted({canonical_action(token) for token in tokens if canonical_action(token)})
    effects = sorted(tokens & EFFECT_WORDS)
    evidence_id = stable_hash([source.get("source_hash"), field, index, text])
    return {
        "evidence_id": evidence_id,
        "field": field,
        "index": index,
        "text": text,
        "domains": domains,
        "actions": actions,
        "effects": effects,
        "mechanical": bool(domains or actions or effects),
    }


def build_game_evidence_map(source: Dict[str, Any]) -> Dict[str, Any]:
    units = evidence_units(source)
    domain_coverage = sorted({domain for unit in units for domain in unit["domains"]})
    return {
        "schema_version": GAME_EVIDENCE_SCHEMA,
        "map_id": stable_hash([source.get("source_hash"), GAME_EVIDENCE_SCHEMA]),
        "dataset": source.get("dataset"),
        "source_id": source.get("source_id"),
        "source_hash": source.get("source_hash"),
        "source_file": source.get("source_file"),
        "source_line": source.get("source_line"),
        "source_url": source.get("source_url"),
        "name": source.get("name"),
        "description": source.get("description"),
        "genres": source.get("genres") or [],
        "tags": source.get("tags") or [],
        "platforms": source.get("platforms") or [],
        "evidence_units": units,
        "domain_coverage": domain_coverage,
        "mechanical_unit_count": sum(bool(unit["mechanical"]) for unit in units),
        "coverage_status": "mechanical-evidence" if any(unit["mechanical"] for unit in units) else "insufficient-evidence",
    }


def deterministic_interactions(game_map: Dict[str, Any]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    seen = set()
    for unit in game_map.get("evidence_units") or []:
        words = _words(unit["text"])
        for index, raw_action in enumerate(words):
            action = canonical_action(raw_action)
            if not action:
                continue
            if action == "cast" and index + 1 < len(words) and words[index + 1] == "of":
                continue
            if action in {"attack", "close", "place", "reward", "upgrade"} and index > 0 and words[index - 1] in {"a", "an", "awesome", "drive", "new", "prominent", "the"}:
                continue
            target = _nearest_target(words, index, action)
            if not target:
                continue
            if action == "create" and target in {"level", "platform", "story", "world"} and not set(words) & {"editor", "generate", "generated", "mode", "user-generated-content"}:
                continue
            relation = {
                "subject": "player-or-system",
                "trigger": "",
                "condition": "",
                "action": action,
                "target": target,
                "effect": action if action in EFFECT_WORDS else "",
                "duration": "",
                "stacking": "",
                "cancellation": "",
                "resulting_state": f"{target}-{action}" if target else f"{action}-resolved",
            }
            key = (unit["evidence_id"], semantic_key(relation))
            if key in seen:
                continue
            seen.add(key)
            output.append(_interaction(game_map, unit, relation, "deterministic"))
    return output


def _nearest_target(words: Sequence[str], action_index: int, action: str) -> str:
    allowed = ACTION_TARGETS.get(action) or set(TARGET_ALIASES.values())
    for index in range(action_index + 1, min(len(words), action_index + 6)):
        if words[index] in {"dungeon", "dungeons"} and index + 1 < len(words) and words[index + 1] in {"dragon", "dragons"}:
            continue
        canonical = TARGET_ALIASES.get(words[index])
        if canonical and canonical in allowed:
            return canonical
    return ""


def model_interaction(game_map: Dict[str, Any], unit: Dict[str, Any], relation: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    evidence_words = _words(unit["text"])
    evidence_tokens = set(evidence_words)
    action = canonical_action(str(relation.get("action") or ""))
    evidence_actions = [canonical_action(word) for word in evidence_words]
    if not action or action not in evidence_actions:
        return None
    action_index = evidence_actions.index(action)
    target = _nearest_target(evidence_words, action_index, action)
    if not target or target not in (ACTION_TARGETS.get(action) or set(TARGET_ALIASES.values())):
        return None
    normalized = {key: slug(relation.get(key)) for key in (
        "subject", "trigger", "condition", "action", "target", "effect", "duration", "stacking", "cancellation", "resulting_state"
    )}
    normalized["subject"] = "player-or-system"
    normalized["action"] = action
    normalized["target"] = target
    for key in ("trigger", "condition", "effect", "duration", "stacking", "cancellation"):
        value = normalized[key]
        if value and not set(value.split("-")) <= evidence_tokens:
            normalized[key] = ""
    normalized["resulting_state"] = f"{target}-{action}"
    return _interaction(game_map, unit, normalized, "lfm-350m")


def _interaction(game_map: Dict[str, Any], unit: Dict[str, Any], relation: Dict[str, Any], origin: str) -> Dict[str, Any]:
    return {
        "schema_version": INTERACTION_SCHEMA,
        "interaction_id": interaction_identity(game_map["source_hash"], unit["evidence_id"], relation),
        "semantic_key": semantic_key(relation),
        "source_id": game_map["source_id"],
        "source_hash": game_map["source_hash"],
        "source_file": game_map["source_file"],
        "source_line": game_map["source_line"],
        "origin": origin,
        "relation": relation,
        "domains": unit.get("domains") or [],
        "evidence": {"evidence_id": unit["evidence_id"], "field": unit["field"], "text": unit["text"]},
    }


def kit_observation(interaction: Dict[str, Any]) -> Dict[str, Any]:
    relation = interaction["relation"]
    action = slug(relation.get("action") or relation.get("effect") or "apply")
    target = slug(relation.get("target") or relation.get("resulting_state") or "state")
    domains = interaction.get("domains") or ["mechanics"]
    domain = _relation_domain(action, target, domains)
    subdomain = f"{target}-{action}"
    observation_id = stable_hash([interaction["interaction_id"], KIT_OBSERVATION_SCHEMA])
    return {
        "schema_version": KIT_OBSERVATION_SCHEMA,
        "observation_id": observation_id,
        "semantic_key": interaction["semantic_key"],
        "merge_key": f"{domain}:{subdomain}",
        "kit_name": f"{subdomain}-kit",
        "kind": "atomic",
        "domain": domain,
        "subdomain": subdomain,
        "owns": f"Validate and apply {action} to {target} under the evidenced condition.",
        "does_not_own": "Rendering, host input capture, unrelated progression, or game-branded content.",
        "core_reuse": ["core-data-kit", "core-policy-kit", "core-diagnostics-kit"],
        "inputs": [item for item in [relation.get("subject"), relation.get("trigger"), relation.get("condition"), target] if item],
        "outputs": [item for item in [relation.get("effect"), relation.get("resulting_state"), f"{action}-applied-or-rejected"] if item],
        "idempotency_rule": f"Replay of the same {action} key does not apply the transition twice.",
        "reset_or_snapshot": f"Snapshot and restore owned {target} transition state and applied keys.",
        "first_proof": f"Replay the cited {interaction['evidence']['field']} evidence through one accepted and one rejected {action} transition.",
        "duplicate_or_merge_risk": "Compare semantic relation and owned transition against the master inventory.",
        "promotion_potential": "candidate",
        "temporal_behavior": {"before": f"{target}-before", "event": action, "after": relation.get("resulting_state") or f"{target}-after"},
        "dsk_boundary": {"layer": "domain-service", "reason": f"Owns reusable {domain} state transition policy; host and renderer remain adapters."},
        "source_context": {
            "interaction_id": interaction["interaction_id"],
            "source_id": interaction["source_id"],
            "source_hash": interaction["source_hash"],
            "evidence_id": interaction["evidence"]["evidence_id"],
            "evidence_text": interaction["evidence"]["text"],
            "evidence_field": interaction["evidence"]["field"],
            "relation": relation,
            "origin": interaction["origin"],
        },
    }


def _relation_domain(action: str, target: str, evidence_domains: Sequence[str]) -> str:
    if action in {"attack", "block", "damage", "defeat", "destroy", "dodge", "fight", "freeze", "stun"} or target in {"attack", "boss", "damage", "enemy", "trap"}:
        return "combat-effects" if action in EFFECT_WORDS else "combat"
    if action in {"collect", "draw", "equip"} or target in {"card", "currency", "equipment", "gear", "item", "loot", "resource", "star", "moon"}:
        return "inventory"
    if action in {"build", "craft", "repair"}:
        return "crafting"
    if action in {"buy", "sell", "trade"}:
        return "economy"
    if action in {"choose"}:
        return "choice"
    if action in {"climb", "explore", "move", "travel"}:
        return "movement" if action in {"climb", "move", "travel"} else "world"
    if action == "interact":
        return "interaction"
    if action == "create" and target in {"level", "story", "world"}:
        return "content-authoring"
    if action == "create":
        return "world-state"
    if action == "gather" and target == "party":
        return "social"
    if action == "restore" and target == "health":
        return "health"
    if action == "use" and target == "weapon":
        return "combat"
    if action == "use" and target == "environment":
        return "combat-interaction"
    if action == "save":
        return "persistence" if target == "state" else "objectives"
    if action in {"solve"}:
        return "puzzles"
    if action in {"unlock", "upgrade"}:
        return "progression"
    return "mechanics"


def game_domain_kit_map(game_map: Dict[str, Any], observations: List[Dict[str, Any]]) -> Dict[str, Any]:
    domains: Dict[str, List[str]] = {}
    for item in observations:
        domains.setdefault(item["domain"], []).append(item["observation_id"])
    return {
        "schema_version": GAME_MAP_SCHEMA,
        "game_map_id": stable_hash([game_map["map_id"], GAME_MAP_SCHEMA]),
        "source_id": game_map["source_id"],
        "source_hash": game_map["source_hash"],
        "game_summary": {"name": game_map.get("name"), "evidence_status": game_map["coverage_status"]},
        "layers": {
            "atomic_kit_map": [item["observation_id"] for item in observations],
            "domain_map": [{"domain": domain, "purpose": f"Own reusable {domain} mechanics.", "kit_observation_ids": ids} for domain, ids in sorted(domains.items())],
            "dsk_map": [{"domain": domain, "layer": "domain-service", "runtime": "state-transition", "adapter": "host-owned", "proof": "evidence-replay"} for domain in sorted(domains)],
            "temporal_ensemble": [item["temporal_behavior"] for item in observations],
            "duplicate_and_merge_ledger": [{"observation_id": item["observation_id"], "semantic_key": item["semantic_key"], "status": "pending-master-merge"} for item in observations],
            "proof_hooks": [item["first_proof"] for item in observations],
            "promotion_candidates": [item["observation_id"] for item in observations],
        },
    }
