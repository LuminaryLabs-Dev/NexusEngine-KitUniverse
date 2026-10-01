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
KIT_DESCRIPTOR_SCHEMA_V1 = "kituniverse.kit-descriptor.v1"
KIT_DESCRIPTOR_SCHEMA = "kituniverse.kit-descriptor.v2"
SUPPORTED_KIT_DESCRIPTOR_SCHEMAS = {KIT_DESCRIPTOR_SCHEMA_V1, KIT_DESCRIPTOR_SCHEMA}
KIT_ROLES = {"atomic", "policy", "adapter", "assembly", "app"}
PROMOTION_TIERS = {"core", "protokit-candidate", "application"}
KIT_VISIBILITIES = {"public", "internal", "editor-safe"}
DOMAIN_DESCRIPTOR_SCHEMA = "kituniverse.domain-descriptor.v1"
SUBDOMAIN_DESCRIPTOR_SCHEMA = "kituniverse.subdomain-descriptor.v1"
DOMAIN_MEMBERSHIP_SCHEMA = "kituniverse.domain-membership.v1"
DOMAIN_ARCHITECTURE_REPORT_SCHEMA = "kituniverse.domain-architecture-report.v1"
KIT_AUTHORING_SCHEMA = "kituniverse.codex-kit-authoring.v1"


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


def validate_kit_descriptor(value: Dict[str, Any], allow_legacy: bool = True) -> List[str]:
    errors: List[str] = []
    schema = value.get("schema_version")
    if schema == KIT_DESCRIPTOR_SCHEMA_V1 and allow_legacy:
        return errors
    if schema != KIT_DESCRIPTOR_SCHEMA:
        return ["invalid-kit-descriptor-schema"]
    for key in ("kit_id", "domain", "subdomain", "domain_path", "promotion_tier", "kit_role", "visibility"):
        if not isinstance(value.get(key), str) or not value[key].strip():
            errors.append(f"missing-{key.replace('_', '-')}")
    if value.get("kit_role") not in KIT_ROLES:
        errors.append("invalid-kit-role")
    if value.get("promotion_tier") not in PROMOTION_TIERS:
        errors.append("invalid-promotion-tier")
    if value.get("visibility") not in KIT_VISIBILITIES:
        errors.append("invalid-kit-visibility")
    for key in ("child_kit_ids", "core_kits_reused", "requires", "provides"):
        items = value.get(key)
        if not isinstance(items, list) or any(not isinstance(item, str) or not item.strip() for item in items):
            errors.append(f"invalid-{key.replace('_', '-')}")
        elif len(items) != len(set(items)):
            errors.append(f"duplicate-{key.replace('_', '-')}")
    parent = value.get("parent_kit_id")
    if parent is not None and (not isinstance(parent, str) or not parent.strip()):
        errors.append("invalid-parent-kit-id")
    children = value.get("child_kit_ids") if isinstance(value.get("child_kit_ids"), list) else []
    if value.get("kit_role") == "assembly":
        proof = value.get("child_idempotency_proof")
        if not children:
            errors.append("assembly-without-child-kits")
        if not isinstance(proof, dict) or proof.get("status") != "passing":
            errors.append("assembly-child-idempotency-unproven")
    elif children:
        errors.append("non-assembly-with-child-kits")
    if value.get("kit_role") == "app" and value.get("promotion_tier") != "application":
        errors.append("app-kit-outside-application-tier")
    return errors


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
