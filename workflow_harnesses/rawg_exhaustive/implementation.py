from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from workflow_harnesses.rawg_capability_pipeline.contracts import slug


def build_runtime_package(request: Dict[str, Any], package_root: Path, engine_root: Path) -> Dict[str, Any]:
    contract = request["contract"]
    context = request["source_context"]
    subdomain_parts = [part for part in slug(contract["subdomain"]).split("-") if part]
    action = subdomain_parts[-1] if subdomain_parts else "apply"
    target = "-".join(subdomain_parts[:-1]) if len(subdomain_parts) > 1 else "state"
    action = action or "apply"
    target = target or "state"
    domain = slug(f"{target}-{action}")
    kit_id = f"n-{domain}-kit"
    result_state = f"{target}-{action}"
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
const State = defineResource(`${{DOMAIN}}.state`);
const Command = defineEvent(`${{DOMAIN}}.command`);
const Applied = defineEvent(`${{DOMAIN}}.applied`);
const clone = (value) => value == null ? value : JSON.parse(JSON.stringify(value));
const initialState = () => ({{ revision: 0, applied: {{}}, lastResult: null }});

export function createKit() {{
  return defineDomainServiceKit({{
    domain: DOMAIN,
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
        "inputs": [{"id": "proof-1", "action": action, "target": target}],
        "expectedOutputs": ["applied", action, target, result_state],
        "forbiddenImports": ["document.", "window.", "canvas", "three", "browser-host-lifecycle"],
        "testCommands": [["node", "--check", "index.js"]],
        "sourceContext": context,
    }
    manifest_path = package_root / "runtime-proof-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"kit_id": kit_id, "action": action, "target": target, "manifest_path": manifest_path}
