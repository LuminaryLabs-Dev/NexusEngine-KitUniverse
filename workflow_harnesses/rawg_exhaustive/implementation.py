from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash

from .contracts import KIT_DESCRIPTOR_SCHEMA, KIT_ROLES, KIT_VISIBILITIES, PROMOTION_TIERS


def runtime_kit_shape(request: Dict[str, Any]) -> Dict[str, str]:
    contract = request["contract"]
    feature = slug(request.get("kit_identity_override")) or slug(contract.get("name")) or slug(contract.get("subdomain")) or "capability-kit"
    if feature.startswith("n-"):
        feature = feature[2:]
    if feature.endswith("-kit"):
        feature = feature[:-4]
    feature = feature or "capability"
    feature_parts = [part for part in feature.split("-") if part]
    action = feature_parts[-1] if feature_parts else "apply"
    target = "-".join(feature_parts[:-1]) if len(feature_parts) > 1 else "state"
    action = action or "apply"
    target = target or "state"
    domain = feature
    kit_id = f"n-{feature}-kit"
    result_state = feature
    return {"action": action, "target": target, "runtime_domain": domain, "kit_id": kit_id, "result_state": result_state}


def descriptor_architecture(request: Dict[str, Any]) -> Dict[str, Any]:
    contract = request["contract"]
    shape = runtime_kit_shape(request)
    def values(key: str) -> list[Any]:
        candidate = request.get(key)
        if candidate is None:
            candidate = contract.get(key)
        if candidate is None:
            return []
        return candidate if isinstance(candidate, list) else [candidate]

    role = str(request.get("kit_role") or contract.get("kit_role") or "atomic").strip()
    if role not in KIT_ROLES:
        role = "atomic"
    tier = str(request.get("promotion_tier") or contract.get("promotion_tier") or "protokit-candidate").strip()
    if tier not in PROMOTION_TIERS:
        tier = "protokit-candidate"
    if role == "app":
        tier = "application"
    visibility = str(request.get("visibility") or contract.get("visibility") or "public").strip()
    if visibility not in KIT_VISIBILITIES:
        visibility = "public"
    children = sorted({
        str(item).strip()
        for item in values("child_kit_ids")
        if str(item).strip()
    })
    if role != "assembly":
        children = []
    domain = slug(contract["domain"])
    subdomain = slug(contract["subdomain"])
    subdomain_path = f"n:{domain}:{subdomain}"
    if shape["runtime_domain"] == subdomain:
        domain_path = subdomain_path
        parent_domain_path = f"n:{domain}"
    else:
        domain_path = f"{subdomain_path}:{shape['runtime_domain']}"
        parent_domain_path = subdomain_path
    return {
        "kit_role": role,
        "promotion_tier": tier,
        "visibility": visibility,
        "domain_path": domain_path,
        "parent_domain_path": parent_domain_path,
        "parent_kit_id": request.get("parent_kit_id", contract.get("parent_kit_id")),
        "child_kit_ids": children,
        "core_kits_reused": sorted({
            str(item).strip()
            for item in values("core_kits_reused")
            if str(item).strip()
        }),
        "requires": sorted({
            str(item).strip()
            for item in values("requires")
            if str(item).strip()
        }),
        "provides": sorted({
            f"n:{shape['runtime_domain']}",
            f"n:{domain}:{subdomain}",
            *(
                str(item).strip()
                for item in values("provides")
                if str(item).strip()
            ),
        }),
        "child_idempotency_proof": (
            request.get("child_idempotency_proof", contract.get("child_idempotency_proof"))
            if role == "assembly"
            else None
        ),
    }


def build_runtime_package(request: Dict[str, Any], package_root: Path, engine_root: Path) -> Dict[str, Any]:
    contract = request["contract"]
    context = request["source_context"]
    shape = runtime_kit_shape(request)
    action = shape["action"]
    target = shape["target"]
    domain = shape["runtime_domain"]
    kit_id = shape["kit_id"]
    result_state = shape["result_state"]
    architecture = descriptor_architecture(request)
    package_root.mkdir(parents=True, exist_ok=True)
    (package_root / "package.json").write_text(
        json.dumps({"name": kit_id, "private": True, "type": "module"}, indent=2) + "\n",
        encoding="utf-8",
    )
    module = f'''import {{ defineDomainServiceKit, defineEvent, defineResource }} from "../engine/src/index.js";

const DOMAIN = {json.dumps(domain)};
const ACTION = {json.dumps(action)};
const TARGET = {json.dumps(target)};
const RESULTING_STATE = {json.dumps(result_state)};
const DOMAIN_PATH = {json.dumps(architecture["domain_path"])};
const PARENT_DOMAIN_PATH = {json.dumps(architecture["parent_domain_path"])};
const State = defineResource(`${{DOMAIN}}.state`);
const Command = defineEvent(`${{DOMAIN}}.command`);
const Applied = defineEvent(`${{DOMAIN}}.applied`);
const clone = (value) => value == null ? value : JSON.parse(JSON.stringify(value));
const initialState = () => ({{ revision: 0, applied: {{}}, lastResult: null }});

export function createKit() {{
  return defineDomainServiceKit({{
    domain: DOMAIN,
    domainPath: DOMAIN_PATH,
    parentDomainPath: PARENT_DOMAIN_PATH,
    stability: "experimental",
    version: "0.1.0",
    metadata: {{ resetPolicy: "explicit-api-reset", snapshotPolicy: "serializable-resource-state" }},
    services: ["apply", "snapshot"],
    resources: {{ State }},
    events: {{ Command, Applied }},
    inputs: [`${{DOMAIN}}.command`],
    outputs: [`${{DOMAIN}}.applied`],
    initWorld({{ world }}) {{ world.setResource(State, initialState()); }},
    createApi({{ world }}) {{
      const get = () => world.getResource(State) ?? initialState();
      const set = (state) => (world.setResource(State, clone(state)), clone(state));
      return {{
        apply(input = {{}}) {{
          const id = String(input.id ?? input.commandId ?? "").trim();
          if (!id) return {{ status: "rejected", reason: "missing-id", action: ACTION, target: TARGET }};
          const state = get();
          if (state.applied[id]) return {{ ...clone(state.applied[id]), duplicateIgnored: true }};
          const result = {{ status: "applied", id, action: ACTION, target: TARGET, resultingState: RESULTING_STATE }};
          state.applied[id] = result;
          state.lastResult = result;
          state.revision += 1;
          set(state);
          world.emit(Applied, clone(result));
          return clone(result);
        }},
        snapshot() {{ return clone(get()); }},
        loadSnapshot(snapshot) {{ return set(snapshot); }},
        reset() {{ return set(initialState()); }}
      }};
    }}
  }});
}}

export function createProofAdapter({{ engine, kit }}) {{
  const api = engine.n[kit.metadata.apiName];
  return {{
    handle(input) {{ return api.apply(input); }},
    snapshot() {{ return api.snapshot(); }},
    loadSnapshot(snapshot) {{ return api.loadSnapshot(snapshot); }},
    reset() {{ return api.reset(); }}
  }};
}}
'''
    (package_root / "index.js").write_text(module, encoding="utf-8")
    manifest = {
        "schemaVersion": "kit.runtime-proof.v1",
        "packageRoot": ".",
        "engineRoot": str(engine_root.resolve()),
        "engineModule": "src/index.js",
        "module": "index.js",
        "publicImport": "index.js",
        "exportName": "createKit",
        "proofAdapterExport": "createProofAdapter",
        "kitId": kit_id,
        "domainPath": architecture["domain_path"],
        "kitRole": architecture["kit_role"],
        "promotionTier": architecture["promotion_tier"],
        "childKitIds": architecture["child_kit_ids"],
        "requires": architecture["requires"],
        "provides": architecture["provides"],
        "inputs": [{"id": "proof-1", "action": action, "target": target}],
        "expectedOutputs": ["applied", action, target, result_state],
        "forbiddenImports": ["document.", "window.", "canvas", "three", "browser-host-lifecycle"],
        "testCommands": [["node", "--check", "index.js"]],
        "sourceContext": context,
    }
    manifest_path = package_root / "runtime-proof-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    descriptor = {
        "schema_version": KIT_DESCRIPTOR_SCHEMA,
        "kit_id": kit_id,
        "master_kit_id": context["master_kit_id"],
        "source_id": request["source_id"],
        "contract_hash": stable_hash(contract),
        "domain": slug(contract["domain"]),
        "subdomain": slug(contract["subdomain"]),
        **architecture,
        "contract": contract,
        "source_context": context,
        "runtime_proof": "runtime-proof.json",
        "module": "index.js",
    }
    descriptor_path = package_root / "kit.json"
    descriptor_path.write_text(json.dumps(descriptor, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "kit_id": kit_id,
        "action": action,
        "target": target,
        "manifest_path": manifest_path,
        "descriptor_path": descriptor_path,
    }
