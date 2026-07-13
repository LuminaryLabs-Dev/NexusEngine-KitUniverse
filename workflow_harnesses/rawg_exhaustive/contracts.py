from __future__ import annotations

from typing import Any, Dict, List

from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash


WORKFLOW_AST_SCHEMA = "kituniverse.exhaustive-workflow-ast.v1"
GAME_EVIDENCE_SCHEMA = "rawg.game-evidence-map.v1"
INTERACTION_SCHEMA = "mechanic.interaction.v1"
KIT_OBSERVATION_SCHEMA = "atomic.kit-observation.v1"
GAME_MAP_SCHEMA = "game.domain-kit-map.v1"
MASTER_KIT_SCHEMA = "kituniverse.master-kit.v1"
REFINED_KIT_SCHEMA = "kituniverse.refined-kit.v1"
BUILD_REQUEST_SCHEMA = "kit.build-request.v2"


INTERACTION_FIELDS = (
    "subject",
    "trigger",
    "condition",
    "action",
    "target",
    "effect",
    "duration",
    "stacking",
    "cancellation",
    "resulting_state",
)


def semantic_key(interaction: Dict[str, Any]) -> str:
    values = [slug(interaction.get(key)) for key in INTERACTION_FIELDS]
    meaningful = [value for value in values if value and value not in {"none", "unknown", "unspecified"}]
    return "--".join(meaningful) or "unclassified-mechanic"


def interaction_identity(source_hash: str, evidence_id: str, interaction: Dict[str, Any]) -> str:
    return stable_hash([source_hash, evidence_id, semantic_key(interaction), INTERACTION_SCHEMA])


def validate_interaction(value: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if value.get("schema_version") != INTERACTION_SCHEMA:
        errors.append("invalid-interaction-schema")
    if not value.get("interaction_id"):
        errors.append("missing-interaction-id")
    if not value.get("source_id") or not value.get("source_hash"):
        errors.append("missing-source-provenance")
    evidence = value.get("evidence") or {}
    if not evidence.get("evidence_id") or not str(evidence.get("text") or "").strip():
        errors.append("missing-direct-evidence")
    relation = value.get("relation") or {}
    if not relation.get("action") and not relation.get("effect"):
        errors.append("missing-action-or-effect")
    if value.get("semantic_key") != semantic_key(relation):
        errors.append("semantic-key-mismatch")
    return errors


def validate_kit_observation(value: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if value.get("schema_version") != KIT_OBSERVATION_SCHEMA:
        errors.append("invalid-kit-observation-schema")
    for key in ("observation_id", "semantic_key", "merge_key", "kit_name", "domain", "subdomain", "owns", "first_proof"):
        if not value.get(key):
            errors.append(f"missing-{key.replace('_', '-')}")
    if not value.get("inputs") or not value.get("outputs"):
        errors.append("missing-input-output-contract")
    if not value.get("source_context", {}).get("interaction_id"):
        errors.append("missing-interaction-lineage")
    return errors


def validate_game_map(value: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if value.get("schema_version") != GAME_MAP_SCHEMA:
        errors.append("invalid-game-map-schema")
    if not value.get("source_id") or not value.get("source_hash"):
        errors.append("missing-game-source")
    layers = value.get("layers") or {}
    for key in ("atomic_kit_map", "domain_map", "dsk_map", "temporal_ensemble", "proof_hooks"):
        if key not in layers:
            errors.append(f"missing-{key.replace('_', '-')}")
    return errors
