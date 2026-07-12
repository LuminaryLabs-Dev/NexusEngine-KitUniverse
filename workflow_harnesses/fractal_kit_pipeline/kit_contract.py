from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, Iterable, List, Tuple


DOMAIN_FAMILIES = [
    ("data", ["snapshot", "reset", "migration", "selector", "ledger", "schema"]),
    ("composition", ["manifest", "dependency-graph", "install-plan", "promotion", "health"]),
    ("input", ["intent", "binding", "action-map", "context", "device-adapter"]),
    ("spatial", ["transform", "bounds", "zone", "query", "coordinate-space"]),
    ("scene", ["lifecycle", "transition", "object-graph", "tag", "host-binding"]),
    ("physics", ["contact", "friction", "constraint", "fall", "carry-mass"]),
    ("motion", ["locomotion", "vehicle", "flight", "swim", "climb"]),
    ("simulation", ["timer", "objective", "resource-meter", "checkpoint", "hazard"]),
    ("interaction", ["affordance", "target", "prompt", "activation", "result"]),
    ("graphics", ["descriptor", "layer", "material", "visibility", "batch"]),
    ("camera", ["follow", "occlusion", "shake", "volume", "framing"]),
    ("animation", ["pose", "blend", "state", "event", "retarget"]),
    ("audio", ["cue", "mix", "feedback", "spatial-audio", "ducking"]),
    ("ui", ["hud", "menu", "widget", "notification", "accessibility"]),
    ("network", ["replication", "authority", "session", "reconciliation", "transport"]),
    ("diagnostics", ["telemetry", "replay", "determinism", "performance", "health"]),
    ("policy", ["budget", "permission", "safety", "degradation", "approval"]),
    ("agent", ["observation", "decision", "proposal", "execution-ledger", "adapter"]),
    ("terrain", ["chunk", "sampler", "lod", "biome", "route-marker"]),
    ("navigation", ["navmesh", "waypoint", "path-request", "portal", "clearance"]),
    ("economy", ["currency", "inventory", "shop", "reward", "cost"]),
    ("ecology", ["spawn", "flock", "weather", "vegetation", "habitat"]),
    ("rpg", ["dialogue", "quest", "relationship", "status-effect", "schedule"]),
    ("xr", ["ray", "hand", "anchor", "reticle", "stereo-descriptor"]),
]

NEEDS = [
    "state ownership",
    "idempotent updates",
    "deterministic tick behavior",
    "queryable runtime surface",
    "event emission",
    "descriptor output",
    "adapter boundary",
    "validation proof",
    "snapshot reset",
    "dependency declaration",
    "promotion metadata",
    "safe host composition",
]

DEPENDENCIES = [
    "n:data",
    "n:composition",
    "n:input",
    "n:spatial",
    "n:scene",
    "n:simulation",
    "n:diagnostics",
    "n:policy",
    "render:descriptors",
    "interaction:request",
    "scene:graph",
    "domain:capability-graph",
]

OPERATIONS = [
    "register",
    "apply",
    "resolve",
    "route",
    "validate",
    "snapshot",
    "reset",
    "compose",
    "schedule",
    "emit",
    "inspect",
    "reconcile",
    "promote",
    "budget",
    "gate",
    "index",
    "sample",
    "project",
]

SURFACES = [
    "engine-api",
    "resource-store",
    "event-stream",
    "query-surface",
    "descriptor-feed",
    "manifest",
    "state-ledger",
    "snapshot-envelope",
    "validation-report",
    "composition-plan",
]

PROOF_TYPES = [
    "headless-smoke",
    "snapshot-reset-smoke",
    "deterministic-loop",
    "dependency-gap-check",
    "idempotency-replay",
    "manifest-validation",
]

GENERIC_FILLER = {"one", "rough", "roughly", "name", "blue", "item", "node", "thing"}


def make_kit_record(
    index: int,
    family: str,
    subdomain: str,
    need: str,
    dependency: str,
    operation: str,
    surface: str,
    proof: str,
    reveal: str,
) -> Dict[str, Any]:
    base = key(f"{family}-{subdomain}-{operation}-{surface}")
    suffix = short_hash(f"{need}|{dependency}|{proof}|{reveal}|{index}")
    name = f"{base}-{suffix}-kit"
    api_name = camel(f"{family}-{subdomain}-{operation}")
    record_id = f"kit-{index:06d}-{suffix}"
    return {
        "record_id": record_id,
        "source": "FractalKitPipeline",
        "payload": {
            "name": name,
            "domain": f"{family}-{subdomain}",
            "domain_path": f"n:{family}-{subdomain}",
            "parent_domain": family,
            "category": family,
            "type": "atomic-domain-service-kit",
            "status": "generated-candidate",
            "purpose": f"Owns {operation} behavior for {subdomain} so {need} stays reusable.",
            "need": need,
            "primary_dependency": dependency,
            "requires": [dependency],
            "provides": [f"n:{family}-{subdomain}", f"{family}:{operation}", f"{surface}:{subdomain}"],
            "resources": [f"{camel(family)}{pascal(subdomain)}State"],
            "events": [f"{camel(family)}.{key(operation).replace('-', '')}.changed"],
            "systems": [f"{camel(name.replace('-kit', ''))}System"],
            "public_api": [f"engine.n.{api_name}.getState", f"engine.n.{api_name}.{camel(operation)}"],
            "descriptors": [f"{family}.{subdomain}.{surface}"] if "descriptor" in surface or "render" in dependency else [],
            "inputs": [f"{operation} request", f"{surface} context"],
            "outputs": [f"{surface} update", f"{proof} result"],
            "state_rules": [
                "updates are idempotent by semantic id",
                "state is serializable and resettable",
                "runtime truth is not owned by renderer or host UI",
            ],
            "tests": [proof, "snapshot-reset-smoke", "manifest-validation"],
            "snapshot": {"supportsSnapshot": True, "supportsReset": True, "supportsLoadSnapshot": "snapshot" in need},
            "renderer_boundary": {
                "outputsDescriptors": "descriptor" in surface or "render" in dependency,
                "ownsDom": False,
                "ownsCanvas": False,
                "ownsThreeObjects": False,
            },
            "promotion": {
                "level": "incubating",
                "criteria": ["manifest complete", "headless smoke", "snapshot/reset if stateful"],
            },
            "source_evidence": {"revealed_signal": reveal, "operation": operation, "surface": surface, "proof": proof},
            "aliases": [key(f"{subdomain}-{operation}"), key(f"{operation}-{surface}-{family}")],
            "atomic": True,
            "idempotent": True,
            "merge_key": key(f"{family}|{subdomain}|{need}|{dependency}"),
            "semantic_key": key(f"{family}|{subdomain}|{need}|{dependency}|{operation}|{surface}|{proof}|{reveal}"),
        },
    }


def base_signal_triplets() -> List[Tuple[str, str, str]]:
    flat = [f"{family}-{subdomain}" for family, subdomains in DOMAIN_FAMILIES for subdomain in subdomains]
    return [(flat[i], flat[(i * 3 + 1) % len(flat)], flat[(i * 7 + 2) % len(flat)]) for i in range(len(flat))]


def cycle_domain_families() -> Iterable[Tuple[str, List[str]]]:
    while True:
        yield from DOMAIN_FAMILIES


def key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value).lower()).strip("-")


def camel(value: str) -> str:
    parts = [part for part in key(value).split("-") if part]
    return parts[0] + "".join(part.capitalize() for part in parts[1:]) if parts else "kit"


def pascal(value: str) -> str:
    return "".join(part.capitalize() for part in key(value).split("-") if part)


def short_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
