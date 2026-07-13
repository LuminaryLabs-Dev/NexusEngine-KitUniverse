from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence

from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash

from .contracts import KIT_OBSERVATION_SCHEMA, MASTER_KIT_SCHEMA
from .evidence import DOMAIN_TERMS, kit_observation as base_kit_observation


POINTER_SCHEMA = "rawg.game-kit-pointer-map.v4"
FACET_REGISTRY_SCHEMA = "kituniverse.kit-facet-registry.v1"


_FACET_NAMES = (
    "request-intake", "schema-validation", "authorization", "eligibility", "condition-evaluation",
    "target-resolution", "cost-quotation", "cost-commit", "cooldown-query", "cooldown-commit",
    "charge-management", "scheduling", "priority", "conflict-resolution", "effect-application",
    "modifier-evaluation", "resistance", "immunity", "stacking", "refresh", "duration",
    "periodic-tick", "cancellation", "expiration", "state-transition", "event-emission", "state-query",
    "snapshot", "restore", "reset", "idempotency", "replay", "serialization", "schema-migration",
    "diagnostics", "audit", "metrics", "ai-query", "ui-descriptor", "tutorial-descriptor",
    "accessibility-descriptor", "network-command", "authority", "prediction", "rollback",
    "reconciliation", "persistence", "test-fixture",
)

_DOMAIN_FACETS = (
    "request-intake", "schema-validation", "authorization", "eligibility", "condition-evaluation",
    "target-resolution", "scheduling", "priority", "conflict-resolution", "state-transition",
    "event-emission", "state-query", "snapshot", "restore", "reset", "idempotency", "replay",
    "serialization", "schema-migration", "diagnostics", "audit", "metrics", "ai-query",
    "ui-descriptor", "tutorial-descriptor", "accessibility-descriptor", "network-command", "authority",
    "rollback", "reconciliation", "persistence", "test-fixture",
)

_PLATFORM_FACETS = (
    "request-intake", "schema-validation", "authorization", "state-query", "snapshot", "restore",
    "reset", "serialization", "schema-migration", "diagnostics", "metrics", "ui-descriptor",
    "accessibility-descriptor", "network-command", "authority", "test-fixture",
)


PROFILE_FACETS = {
    "direct-mechanic-v1": _FACET_NAMES,
    "domain-signal-v1": _DOMAIN_FACETS,
    "category-signal-v1": _DOMAIN_FACETS,
    "platform-constraint-v1": _PLATFORM_FACETS,
    "evidence-only-v1": (),
}


CATEGORY_DOMAIN_MAP = {
    "action": ("combat", "movement"),
    "adventure": ("world", "objectives", "puzzles"),
    "arcade": ("progression", "objectives"),
    "card": ("cards", "inventory"),
    "casual": ("objectives", "progression"),
    "co-op": ("network", "social"),
    "cooperative": ("network", "social"),
    "fighting": ("combat", "agents"),
    "multiplayer": ("network", "social"),
    "online": ("network",),
    "open-world": ("world", "movement"),
    "physics": ("simulation",),
    "platformer": ("movement", "world"),
    "puzzle": ("puzzles",),
    "racing": ("vehicles", "progression"),
    "rpg": ("progression", "inventory", "dialogue"),
    "role-playing-games-rpg": ("progression", "inventory", "dialogue"),
    "shooter": ("combat", "inventory"),
    "simulation": ("simulation",),
    "singleplayer": ("persistence", "simulation"),
    "sports": ("simulation", "progression"),
    "stealth": ("agents", "movement"),
    "strategy": ("agents", "simulation", "economy"),
    "survival": ("health", "crafting", "inventory"),
    "turn-based": ("simulation", "combat"),
}


def facet_registry() -> Dict[str, Any]:
    return {
        "schema_version": FACET_REGISTRY_SCHEMA,
        "materialization_rule": "For each seed, materialize one atomic master candidate for every facet id in its named profile.",
        "facets": [
            {
                "facet_id": name,
                "kind": _facet_kind(name),
                "owns": f"Own the reusable {name.replace('-', ' ')} boundary for one capability without owning its host or renderer.",
            }
            for name in _FACET_NAMES
        ],
        "profiles": {name: list(facets) for name, facets in PROFILE_FACETS.items()},
    }


def build_pointer_map(game_map: Dict[str, Any], interactions: Sequence[Dict[str, Any]], pipeline_epoch: str) -> Dict[str, Any]:
    seeds: List[Dict[str, Any]] = []
    seed_positions: Dict[tuple, int] = {}
    evidence_locators = {
        unit.get("evidence_id"): [unit.get("field"), int(unit.get("index") or 0)]
        for unit in game_map.get("evidence_units") or [] if unit.get("evidence_id")
    }

    def add(
        kind: str, profile: str, domain: str, capability: str, evidence: Dict[str, Any],
        relation: Optional[Dict[str, Any]] = None, master_capability: Optional[str] = None,
    ) -> None:
        capability = slug(capability)
        domain = slug(domain) or "mechanics"
        key = (kind, domain, capability)
        if not capability:
            return
        if key in seed_positions:
            existing = seeds[seed_positions[key]]
            evidence_ids = {item.get("evidence_id") for item in existing["evidence_refs"]}
            if evidence.get("evidence_id") not in evidence_ids:
                existing["evidence_refs"].append(evidence)
            return
        seed_positions[key] = len(seeds)
        seeds.append({
            "seed_id": stable_hash([game_map["source_hash"], kind, domain, capability, evidence.get("evidence_id")]),
            "kind": kind,
            "facet_profile": profile,
            "domain": domain,
            "capability": capability,
            "master_capability": slug(master_capability or capability),
            "evidence": evidence,
            "evidence_refs": [evidence],
            "relation": relation or {},
        })

    for interaction in interactions:
        relation = interaction.get("relation") or {}
        action = slug(relation.get("action") or relation.get("effect") or "apply")
        target = slug(relation.get("target") or relation.get("resulting_state") or "state")
        base_observation = base_kit_observation(interaction)
        domain = base_observation["domain"]
        add(
            "direct-mechanic", "direct-mechanic-v1", domain, interaction.get("semantic_key") or f"{target}-{action}",
            {
                **(interaction.get("evidence") or {}),
                "interaction_id": interaction.get("interaction_id"),
                "locator": evidence_locators.get((interaction.get("evidence") or {}).get("evidence_id")),
            }, relation,
            base_observation["subdomain"],
        )

    for unit in game_map.get("evidence_units") or []:
        evidence = {"evidence_id": unit["evidence_id"], "field": unit["field"], "text": unit["text"], "locator": [unit["field"], unit["index"]]}
        for domain in unit.get("domains") or []:
            add("domain-signal", "domain-signal-v1", domain, domain, evidence)

    field_units = {
        (str(unit.get("field")), slug(unit.get("text"))): unit
        for unit in game_map.get("evidence_units") or []
    }
    for field in ("genres", "tags"):
        for value in game_map.get(field) or []:
            normalized = slug(value)
            unit = field_units.get((field, normalized)) or {}
            evidence = {
                "evidence_id": unit.get("evidence_id") or stable_hash([game_map["source_hash"], field, normalized]),
                "field": field,
                "text": str(value),
                "locator": [field, int(unit.get("index") or 0)],
            }
            domains = _category_domains(normalized)
            if domains:
                for domain in domains:
                    add("category-signal", "domain-signal-v1", domain, domain, evidence)
            else:
                add("evidence-signal", "evidence-only-v1", "evidence-index", f"{field[:-1]}-{normalized}", evidence)

    for platform_index, value in enumerate(game_map.get("platforms") or []):
        normalized = slug(value)
        evidence = {
            "evidence_id": stable_hash([game_map["source_hash"], "platforms", normalized]),
            "field": "platforms",
            "text": str(value),
            "locator": ["platforms", platform_index],
        }
        add("platform-constraint", "platform-constraint-v1", "runtime-adapter", f"platform-{normalized}", evidence)

    expanded = sum(len(PROFILE_FACETS[seed["facet_profile"]]) for seed in seeds)
    return {
        "schema_version": POINTER_SCHEMA,
        "pointer_map_id": stable_hash([game_map["source_hash"], POINTER_SCHEMA]),
        "pipeline_epoch": pipeline_epoch,
        "source_id": game_map["source_id"],
        "source_hash": game_map["source_hash"],
        "source_file": game_map.get("source_file"),
        "source_line": game_map.get("source_line"),
        "seeds": seeds,
        "seed_count": len(seeds),
        "expanded_node_count": expanded,
        "coverage_status": "hundreds-decomposed" if expanded >= 100 else ("evidence-limited" if seeds else "insufficient-evidence"),
        "materialization": {"registry_schema": FACET_REGISTRY_SCHEMA, "profile_field": "facet_profile"},
    }


def compact_pointer_map(pointer: Dict[str, Any]) -> Dict[str, Any]:
    seed_columns = ["kind", "facet_profile", "domain", "master_capability", "evidence_locators"]
    seed_table = []
    for seed in pointer.get("seeds") or []:
        evidence_refs = seed.get("evidence_refs") or [seed.get("evidence") or {}]
        seed_table.append([
            seed.get("kind"), seed.get("facet_profile"), seed.get("domain"), seed.get("master_capability"),
            [item.get("locator") for item in evidence_refs if item.get("locator")],
        ])
    return {
        key: pointer.get(key) for key in (
            "schema_version", "pointer_map_id", "pipeline_epoch", "source_id", "source_hash", "source_file",
            "source_line", "seed_count", "expanded_node_count", "coverage_status", "materialization",
        )
    } | {"seed_columns": seed_columns, "seed_table": seed_table}


def aggregate_pointer_masters(
    pointer_maps: Iterable[Dict[str, Any]], inventory_status: Any, pipeline_epoch: str
) -> Iterable[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for pointer in pointer_maps:
        source_seen = set()
        for seed in pointer.get("seeds") or []:
            facets = PROFILE_FACETS.get(seed.get("facet_profile"), ())
            if not facets:
                continue
            master_capability = seed.get("master_capability") or seed["capability"]
            key = f"{seed['domain']}:{master_capability}"
            item = grouped.setdefault(key, {
                "seed": seed, "support": 0, "sources": [], "facets": set(), "variants": set(),
            })
            item["facets"].update(facets)
            item["variants"].add(seed["capability"])
            if key in source_seen:
                continue
            source_seen.add(key)
            item["support"] += 1
            if len(item["sources"]) < 64:
                item["sources"].append(pointer["source_id"])

    for key, item in sorted(grouped.items()):
        seed = item["seed"]
        facets = sorted(item["facets"])
        observation = _pointer_observation(key, seed, facets, item["support"])
        status = inventory_status(key)
        yield {
            "schema_version": MASTER_KIT_SCHEMA,
            "pipeline_epoch": pipeline_epoch,
            "master_kit_id": stable_hash([key, MASTER_KIT_SCHEMA, "pointer-family-v2"]),
            "semantic_key": key,
            "canonical_observation": observation,
            "domains": [seed["domain"]],
            "support_count": item["support"],
            "source_count": item["support"],
            "source_ids_sample": item["sources"],
            "interaction_keys_sample": sorted(item["variants"])[:128],
            "variant_count": len(item["variants"]),
            "required_facets": facets,
            "inventory_status": status,
            "lifecycle_status": "already-supported" if status["status"] == "already-supported" else "observed",
            "provenance_query": {
                "pointer_schema": POINTER_SCHEMA,
                "seed_kind": seed["kind"],
                "domain": seed["domain"],
                "master_capability": seed.get("master_capability") or seed["capability"],
                "facet_profile": seed["facet_profile"],
            },
        }


def _pointer_observation(key: str, seed: Dict[str, Any], facets: Sequence[str], support: int) -> Dict[str, Any]:
    capability = seed.get("master_capability") or seed["capability"]
    subdomain = capability
    evidence = seed.get("evidence") or {}
    observation_id = stable_hash([key, KIT_OBSERVATION_SCHEMA, "pointer-family-v2"])
    facet_basis = (
        "capability-root" if seed["kind"] == "direct-mechanic" else
        "adapter-root" if seed["kind"] == "platform-constraint" else
        "domain-root"
    )
    return {
        "schema_version": KIT_OBSERVATION_SCHEMA,
        "observation_id": observation_id,
        "semantic_key": key,
        "merge_key": key,
        "kit_name": f"{subdomain}-kit",
        "kind": "atomic-pointer-materialization",
        "domain": seed["domain"],
        "subdomain": subdomain,
        "owns": f"Own {subdomain} as one atomic reusable behavior and satisfy its declared lifecycle facets.",
        "does_not_own": "Game-branded content, rendering, host input capture, or unrelated behaviors.",
        "core_reuse": ["core-data-kit", "core-policy-kit", "core-diagnostics-kit"],
        "inputs": [f"{capability}-request", f"{capability}-context"],
        "outputs": [f"{capability}-accepted", f"{capability}-rejected"],
        "idempotency_rule": f"The same {capability} request key is applied at most once.",
        "reset_or_snapshot": f"Snapshot, restore, and reset all owned {capability} state.",
        "first_proof": f"Replay one accepted, rejected, duplicate, snapshot, restore, and reset {capability} transition.",
        "duplicate_or_merge_risk": "Compare atomic behavior, ownership, tokens, and evidence variants against the master inventory.",
        "promotion_potential": "candidate",
        "evidence_strength": "direct" if seed["kind"] == "direct-mechanic" else "requirement-signal",
        "facet_basis": facet_basis,
        "required_facets": list(facets),
        "support_count": support,
        "temporal_behavior": {"before": f"{capability}-before", "event": capability, "after": f"{capability}-after"},
        "dsk_boundary": {"layer": "domain-service", "reason": "Owns one behavior; lifecycle facets remain inside its contract."},
        "source_context": {
            "interaction_id": evidence.get("interaction_id") or seed["seed_id"],
            "source_id": "pointer-query",
            "source_hash": "pointer-query",
            "evidence_id": evidence.get("evidence_id"),
            "evidence_text": evidence.get("text"),
            "evidence_field": evidence.get("field"),
            "origin": seed["kind"],
            "relation": seed.get("relation") or {},
            "proposed_capability": capability,
            "proposed_facet": "capability-root",
            "facet_basis": facet_basis,
            "required_facets": list(facets),
            "provenance_query": {"seed_id": seed["seed_id"], "master_capability": capability},
        },
    }


def _category_domains(value: str) -> Sequence[str]:
    if value in CATEGORY_DOMAIN_MAP:
        return CATEGORY_DOMAIN_MAP[value]
    tokens = set(value.split("-"))
    for domain, terms in DOMAIN_TERMS.items():
        if tokens & terms:
            return (domain,)
    return ()


def _facet_kind(name: str) -> str:
    if name in {"ui-descriptor", "tutorial-descriptor", "accessibility-descriptor"}:
        return "presentation-descriptor"
    if name in {"network-command", "authority", "prediction", "rollback", "reconciliation"}:
        return "distributed-runtime"
    if name in {"snapshot", "restore", "reset", "replay", "serialization", "schema-migration", "persistence"}:
        return "state-lifecycle"
    if name in {"diagnostics", "audit", "metrics", "test-fixture"}:
        return "proof-and-observability"
    return "mechanic-policy"


def _facet_basis(seed_kind: str, facet_id: str) -> str:
    if seed_kind == "direct-mechanic":
        if facet_id in {"request-intake", "target-resolution", "effect-application", "state-transition"}:
            return "mechanic-entailed"
        if facet_id in {
            "schema-validation", "state-query", "snapshot", "restore", "reset", "idempotency", "replay",
            "serialization", "diagnostics", "audit", "metrics", "test-fixture",
        }:
            return "kit-quality-required"
        return "explicit-evidence-required"
    if seed_kind == "platform-constraint":
        return "adapter-required"
    if facet_id in {"request-intake", "schema-validation", "state-query", "diagnostics", "metrics", "test-fixture"}:
        return "domain-architecture-required"
    return "explicit-evidence-required"
