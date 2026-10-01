from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional

from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash
from workflow_harnesses.rawg_matrix_optimizer.workflow_rawg_matrix_optimizer import ShardedJsonlWriter

from .codex_runner import DEFAULT_CODEX_MODEL, DEFAULT_REASONING_EFFORT, peak_overlap, run_codex_lane
from .contracts import KIT_AUTHORING_SCHEMA, KIT_DESCRIPTOR_SCHEMA
from .implementation import descriptor_architecture, runtime_kit_shape


AUTHOR_PROMPT_VERSION = "codex-contract-implementation-v13-idempotency-assertion-alternatives"
AUTHOR_NORMALIZER_VERSION = "public-engine-imports-window-members-v4"
AUTHOR_SYNTAX_TIMEOUT_SECONDS = 60

_PUBLIC_ENGINE_IMPORT = "../engine/src/index.js"
_PUBLIC_ENGINE_REEXPORTS = {
    "../engine/src/domain-api.js",
    "../engine/src/domain-path.js",
    "../engine/src/domain-service-kit.js",
    "../engine/src/ecs.js",
    "../engine/src/engine.js",
    "../engine/src/runtime-kit.js",
}


def author_runtime_kits(
    node: Dict[str, Any], controls: Dict[str, Any], workspace: Path, max_authors_override: Optional[int]
) -> Dict[str, Any]:
    config = node.get("config") or {}
    limit = max_authors_override if max_authors_override is not None else int(config.get("max_authors") or 0)
    retries = max(0, min(2, int(config.get("retries") or 1)))
    timeout_seconds = int(config.get("timeout_seconds") or 1200)
    model = str(config.get("model") or DEFAULT_CODEX_MODEL)
    reasoning_effort = str(config.get("reasoning_effort") or DEFAULT_REASONING_EFFORT)
    max_concurrency = max(1, min(16, int(config.get("max_concurrency") or 1)))
    shards = workspace / "shards"
    completed = {
        str(item["author_identity"]): item
        for item in _read_jsonl(shards.glob("codex-kit-author-ledger-*.jsonl"))
        if item.get("status") == "authored" and item.get("author_identity")
    }
    latest_attempts: Dict[str, Dict[str, Any]] = {}
    latest_attempts_by_source: Dict[str, Dict[str, Any]] = {}
    for item in _read_jsonl(shards.glob("codex-kit-author-ledger-*.jsonl")):
        if item.get("author_identity"):
            latest_attempts[str(item["author_identity"])] = item
        if item.get("source_id"):
            latest_attempts_by_source[str(item["source_id"])] = item
    runtime_proven_contracts = _runtime_proven_contracts(shards)
    runtime_failures = _latest_runtime_failures(shards)
    identity_revisions = _latest_kit_identity_revisions(shards)
    pending = []
    for stored_request in _read_jsonl(shards.glob("exhaustive-build-requests-*.jsonl")):
        request = dict(stored_request)
        revision = identity_revisions.get(str(request.get("source_id")))
        if revision:
            request["kit_identity_override"] = revision["kit_identity_override"]
        if request.get("build_status") != "queued":
            continue
        if (request["source_id"], stable_hash(request["contract"])) in runtime_proven_contracts:
            continue
        repair_signature = _runtime_failure_signature(runtime_failures.get(str(request["source_id"])))
        identity = author_identity(request, model, reasoning_effort, repair_signature)
        authored_root = _author_root(
            workspace, request["source_context"]["master_kit_id"], identity
        )
        if identity in completed and authored_root.is_dir():
            continue
        pending.append(request)
    if limit:
        pending = pending[:limit]
    ledger = ShardedJsonlWriter(shards, "codex-kit-author-ledger", int(controls["shard_max_bytes"]))
    authored = failed = 0
    failures = []
    wave_reports = []
    held = False
    author_root = workspace / "codex-kit-author"
    for wave_index, wave in enumerate(_chunks(pending, max_concurrency)):
        if _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
            break

        def author_request(request: Dict[str, Any]) -> Dict[str, Any]:
            runtime_failure = runtime_failures.get(str(request["source_id"]))
            repair_signature = _runtime_failure_signature(runtime_failure)
            identity = author_identity(request, model, reasoning_effort, repair_signature)
            prior_attempt = (
                latest_attempts.get(identity)
                or latest_attempts_by_source.get(str(request["source_id"]))
                or {}
            )
            raw_output = Path(str(prior_attempt.get("raw_output") or ""))
            if raw_output.is_file() and runtime_failure is None:
                try:
                    value = _parse_object(raw_output.read_text(encoding="utf-8"))
                    value, normalizations = _normalize_author_value(value)
                    _validate_author_output(value, runtime_kit_shape(request)["kit_id"], descriptor_architecture(request))
                    _run_contract_preflight(value, Path(controls["engine_root"]))
                    recovered_root = _author_root(workspace, request["source_context"]["master_kit_id"], identity)
                    if not recovered_root.exists():
                        _write_authored_package(
                            recovered_root,
                            request,
                            Path(controls["engine_root"]),
                            identity,
                            value,
                            model,
                            reasoning_effort,
                        )
                    return {
                        "ok": True,
                        "identity": identity,
                        "request": request,
                        "author_root": str(recovered_root),
                        "kit_id": value["kit_id"],
                        "raw_output": str(raw_output),
                        "execution_report": prior_attempt.get("execution_report"),
                        "scratch_cleaned": True,
                        "recovered_without_model_call": True,
                        "normalizations": normalizations,
                        "attempts": 0,
                    }
                except (OSError, ValueError, json.JSONDecodeError):
                    pass
            result: Dict[str, Any] = {}
            prior_error = str(prior_attempt.get("error") or "")
            for attempt in range(retries + 1):
                result = _author_one(
                    request,
                    Path(controls["engine_root"]),
                    workspace,
                    identity,
                    attempt,
                    prior_error,
                    timeout_seconds,
                    model,
                    reasoning_effort,
                    str(controls.get("pipeline_epoch") or "epoch"),
                    runtime_failure,
                )
                if result.get("ok"):
                    break
                prior_error = str(result.get("error") or "authoring validation failed")
            result["identity"] = identity
            result["request"] = request
            result["attempts"] = attempt + 1
            return result

        with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            futures = {executor.submit(author_request, request): request for request in wave}
            results = []
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:  # keep the wave durable and retry next run
                    request = futures[future]
                    results.append({
                        "ok": False,
                        "error": str(exc),
                        "attempts": 0,
                        "identity": author_identity(
                            request,
                            model,
                            reasoning_effort,
                            _runtime_failure_signature(runtime_failures.get(str(request["source_id"]))),
                        ),
                        "request": request,
                    })
        for result in sorted(results, key=lambda item: str(item.get("identity") or "")):
            request = result.get("request")
            if not isinstance(request, dict):
                failed += 1
                failures.append({"master_kit_id": None, "error": result.get("error")})
                continue
            identity = str(result["identity"])
            status = "authored" if result.get("ok") else "failed"
            ledger.append({
                "schema_version": KIT_AUTHORING_SCHEMA,
                "pipeline_epoch": controls.get("pipeline_epoch"),
                "author_identity": identity,
                "source_id": request["source_id"],
                "master_kit_id": request["source_context"]["master_kit_id"],
                "status": status,
                "author_root": result.get("author_root"),
                "kit_id": result.get("kit_id"),
                "contract_hash": stable_hash(request["contract"]),
                "prompt_version": AUTHOR_PROMPT_VERSION,
                "model": model,
                "reasoning_effort": reasoning_effort,
                "attempts": result.get("attempts"),
                "error": result.get("error"),
                "raw_output": result.get("raw_output"),
                "execution_report": result.get("execution_report"),
                "recovered_without_model_call": result.get("recovered_without_model_call") is True,
                "normalizer_version": AUTHOR_NORMALIZER_VERSION,
                "normalizations": result.get("normalizations") or [],
            })
            if result.get("ok"):
                authored += 1
                completed[identity] = result
            else:
                failed += 1
                failures.append({"master_kit_id": request["source_context"]["master_kit_id"], "error": result.get("error")})
        wave_report = {
            "schema_version": "kituniverse.codex-author-wave.v1",
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "wave_index": wave_index,
            "model": model,
            "reasoning_effort": reasoning_effort,
            "configured_concurrency": max_concurrency,
            "peak_active_lanes": peak_overlap(results),
            "requested": len(wave),
            "authored": sum(item.get("ok") is True for item in results),
            "failed": sum(item.get("ok") is not True for item in results),
            "scratch_cleaned": all(item.get("scratch_cleaned") is True for item in results),
            "results": [
                {key: item.get(key) for key in ("identity", "ok", "kit_id", "attempts", "elapsed_seconds", "error", "raw_output", "execution_report", "normalizations")}
                for item in results
            ],
        }
        _write_json(author_root / "waves" / f"{controls.get('pipeline_epoch')}-{wave_index:06d}.json", wave_report)
        wave_reports.append(wave_report)
        if _free_gib(workspace) < float(controls.get("min_free_gib", 10)):
            held = True
            break
    return {
        "ok": not held,
        "status": "hold" if held else ("complete-with-retries-needed" if failed else ("limit-complete" if limit else "complete")),
        "reason": "low-disk-space" if held else None,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "max_concurrency": max_concurrency,
        "peak_active_lanes": max((int(item.get("peak_active_lanes") or 0) for item in wave_reports), default=0),
        "new_authored": authored,
        "failed": failed,
        "total_authored": len(completed),
        "pending": max(0, len(pending) - authored - failed),
        "failures": failures[:20],
    }


def author_identity(
    request: Dict[str, Any],
    model: str = DEFAULT_CODEX_MODEL,
    reasoning_effort: str = DEFAULT_REASONING_EFFORT,
    repair_signature: str = "",
) -> str:
    return stable_hash([
        request["source_id"],
        request["contract"],
        descriptor_architecture(request),
        AUTHOR_PROMPT_VERSION,
        AUTHOR_NORMALIZER_VERSION,
        model,
        reasoning_effort,
        repair_signature,
    ])


def _author_one(
    request: Dict[str, Any],
    engine_root: Path,
    workspace: Path,
    identity: str,
    attempt: int,
    prior_error: str,
    timeout_seconds: int,
    model: str,
    reasoning_effort: str,
    pipeline_epoch: str,
    runtime_failure: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    master_id = request["source_context"]["master_kit_id"]
    author_root = _author_root(workspace, master_id, identity)
    packet_root = workspace / "codex-kit-author"
    packet_path = packet_root / "packets" / f"{identity}.json"
    packet_path.parent.mkdir(parents=True, exist_ok=True)
    packet_path.write_text(json.dumps(request, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    shape = runtime_kit_shape(request)
    architecture = descriptor_architecture(request)
    repair = f"\nThe prior attempt failed author validation: {prior_error}. Repair that exact defect." if prior_error else ""
    runtime_repair = ""
    if runtime_failure:
        proof_path = runtime_failure.get("proof_path")
        runtime_repair = (
            f"\nThe latest NexusSimulator attempt failed. Read {proof_path} and repair every failed check and its exact detail. "
            "Do not weaken the contract or tests. If declaredOutputs failed because the first call was rejected, changing only proof_inputs is insufficient: "
            "createProofAdapter.handle must read prerequisite fields from that same input, apply them through the installed engine.n API, and then invoke the primary behavior. "
            "For example, register a supplied building definition before moving it, or set a supplied starting balance before exchanging it."
        )
    prompt = f"""
Act as the implementation author for one accepted Nexus Engine Domain Service Kit.
Read {packet_path}, {engine_root / 'src/domain-service-kit.js'}, {engine_root / 'src/engine.js'}, and only the nearby Nexus Engine files needed to use defineDomainServiceKit correctly.
The accepted contract and source provenance are authoritative. Implement the actual owned transition/query rules; do not emit the generic apply-template behavior and do not broaden beyond the contract.
Required stable kit id: {shape['kit_id']}.
Required domain path: {architecture['domain_path']} with parent path {architecture['parent_domain_path']}.
Required kit role: {architecture['kit_role']}; promotion tier: {architecture['promotion_tier']}; visibility: {architecture['visibility']}.
The module runs in disposable SimSpace. Engine imports must be exactly ../engine/src/index.js or ../engine/src/domain-service-kit.js; never use a repository-relative path containing NexusEngine or climb more directory levels.
Return only one JSON object with exactly these keys:
{{"ok":true,"kit_id":"{shape['kit_id']}","index_js":"complete ESM module exporting createKit and createProofAdapter","test_js":"complete node:test contract suite","proof_inputs":[{{"id":"proof-1"}}],"expected_outputs":["contract-specific observable value"]}}
The quoted index_js, test_js, proof_inputs, and expected_outputs values above describe the required value types; they are not valid literal answers. Replace index_js and test_js with complete executable JavaScript source, replace proof_inputs with complete contract-specific objects, and replace expected_outputs with exact observable tokens. Never return the literal phrases "complete ESM module exporting createKit and createProofAdapter", "complete node:test contract suite", or "contract-specific observable value"; that is an incomplete-authored-module failure.
The module must use defineDomainServiceKit, namespaced events/resources, deterministic serializable state, idempotent duplicate handling, snapshot, loadSnapshot, and reset. Set truthy metadata.resetPolicy and metadata.snapshotPolicy. Expose the installed API only through engine.n.<apiName>.
Import defineResource and defineEvent from ../engine/src/index.js when the kit declares resources or events. Every value supplied in the kit's resources collection must be the exact object returned by defineResource("namespaced.id"), and every value supplied in events must be returned by defineEvent("namespaced.id"); never put state values, strings, or plain descriptor objects in those collections. engine.world.setResource requires that same resource-definition object as its first argument, never a string or state object. Use one module-level definition constant consistently in the declaration, init/reset logic, and handlers.
Emit declared events with engine.world.emit(eventDefinition, payload) or the install callback's world.emit(eventDefinition, payload). Never call engine.events, engine.events.publish, or a publish method that is not part of the public engine contract.
The simulator creates and installs createKit() before it calls createProofAdapter({{ engine, kit, manifest }}). Therefore createProofAdapter must never create another engine or call installKit. It must return handle(input), snapshot(), loadSnapshot(snapshot), and reset(); handle must exercise the real primary behavior through the already-installed engine.n API. A duplicate handle call must return the exact same observable result as the first call or an object with duplicateIgnored:true; never return a different duplicate-only string. reset() must restore the exact snapshot visible immediately after adapter creation and before the first handle() call, including clearing any configuration introduced by handle; it must not reset to a later configured baseline.
createProofAdapter.snapshot() must return a defined JSON-serializable object immediately after kit installation and before any handle call. Initialize owned state during kit installation and return an empty serializable state object when no transitions exist; never return undefined, functions, symbols, or non-serializable values from snapshot().
proof_inputs must contain complete capability-specific input fields, not only an id. The first proof input must be self-contained and produce the declared successful output in a freshly installed kit: include every eligibility flag, entity/building definition, source balance, inventory item, or other prerequisite, and make handle seed/configure that prerequisite from the input before calling the primary behavior. Never rely on setup performed only inside the node:test suite. Every expected_outputs string must be a short literal status/event token that handle returns for the first proof input; do not describe multiple possible events in one sentence.
If the role is assembly, compose only the declared child kits through their public contracts, keep every child independently installable and idempotent, and do not duplicate child-owned state or behavior.
The node:test suite must import createEngine from ../engine/src/index.js, install createKit once, use the engine.n API, and prove the contract's positive behavior, rejection/eligibility rule when applicable, duplicate replay, query/state, snapshot-load equivalence, and reset. Capture the reset baseline with api.snapshot() immediately after install and before setup or the first behavior call; after api.reset(), compare api.snapshot() directly with that captured baseline. Never hand-build a reset expectation from later lookups, retain post-setup entities, or add properties whose value is undefined. Use assert.deepStrictEqual for separately returned objects with equal structure; use assert.strictEqual only for primitives or intentionally identical object references. The direct API test and createProofAdapter.handle are independent entry paths: before either invokes the primary behavior, each must explicitly apply every required prerequisite through the installed API, or the primary behavior itself must consume those prerequisite fields. Test createProofAdapter on a fresh engine with a fresh createKit installation, or reset the existing API to its immediate post-install baseline before constructing the adapter. Capture adapterBaseline = adapter.snapshot() immediately after adapter creation and compare adapter.snapshot() to that exact adapterBaseline after adapter.reset(); never construct an adapter after loading a later snapshot and compare its reset result to an earlier unrelated API baseline. Never pass setup fields to a primary method that ignores them. For example, call api.seed(input) before api.share(input), set the source balance before an exchange, or register the supplied entity before moving it. Prefer one shared installed-API setup method used by both the test and adapter, then trace the first positive test line by line and ensure it cannot be rejected from fresh state. Duplicate replay has two valid forms: it may return exactly the same primitive or structurally equal business result as the first call, or it may return the stable business fields plus duplicateIgnored:true. The test must assert the form the implementation actually returns and must not require duplicateIgnored when replay already returns the same stable result. When duplicate replay does add duplicateIgnored:true, compare the stable business fields and assert duplicateIgnored separately; do not require the duplicate object to be deeply identical to the first object. It must not use browser, renderer, network, timers, randomness, filesystem, eval, or external packages.
Do not edit files. Do not return markdown or commentary.{runtime_repair}{repair}
""".strip()
    execution = run_codex_lane(
        repo_root=workspace.parent.parent.parent,
        artifact_root=packet_root,
        scratch_root=workspace / ".codex-scratch" / "kit-author",
        job_id=f"{pipeline_epoch[:12]}-{identity}",
        prompt=prompt,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
        attempt=attempt,
    )
    raw_path = Path(str(execution.get("response") or packet_root / "responses" / f"missing-{identity}.txt"))
    if not execution.get("ok") or not execution.get("response"):
        return {
            "ok": False,
            "error": str(execution.get("error") or f"Codex author failed:{execution.get('returncode')}"),
            "attempt": attempt,
            "raw_output": str(raw_path),
            "execution_report": execution.get("execution_report"),
            "scratch_cleaned": execution.get("scratch_cleaned"),
            "elapsed_seconds": execution.get("elapsed_seconds"),
            "_started_monotonic": execution.get("_started_monotonic"),
            "_finished_monotonic": execution.get("_finished_monotonic"),
        }
    try:
        value = _parse_object(raw_path.read_text(encoding="utf-8"))
        value, normalizations = _normalize_author_value(value)
        _validate_author_output(value, shape["kit_id"], architecture)
        _run_contract_preflight(value, engine_root)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "error": str(exc),
            "attempt": attempt,
            "raw_output": str(raw_path),
            "execution_report": execution.get("execution_report"),
            "scratch_cleaned": execution.get("scratch_cleaned"),
            "elapsed_seconds": execution.get("elapsed_seconds"),
            "_started_monotonic": execution.get("_started_monotonic"),
            "_finished_monotonic": execution.get("_finished_monotonic"),
        }
    if author_root.exists():
        descriptor_path = author_root / "kit.json"
        try:
            descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return {"ok": False, "error": f"authored-root-conflict:{exc}", "attempt": attempt, "raw_output": str(raw_path), **_execution_fields(execution)}
        if descriptor.get("author_identity") != identity:
            return {"ok": False, "error": "authored-root-identity-conflict", "attempt": attempt, "raw_output": str(raw_path), **_execution_fields(execution)}
        return {
            "ok": True,
            "attempt": attempt,
            "author_root": str(author_root),
            "kit_id": shape["kit_id"],
            "raw_output": str(raw_path),
            "normalizations": normalizations,
            **_execution_fields(execution),
        }
    _write_authored_package(author_root, request, engine_root, identity, value, model, reasoning_effort)
    return {
        "ok": True,
        "attempt": attempt,
        "author_root": str(author_root),
        "kit_id": shape["kit_id"],
        "raw_output": str(raw_path),
        "normalizations": normalizations,
        **_execution_fields(execution),
    }


def _write_authored_package(
    root: Path,
    request: Dict[str, Any],
    engine_root: Path,
    identity: str,
    value: Dict[str, Any],
    model: str,
    reasoning_effort: str,
) -> None:
    temporary_root = root.with_name(f".{root.name}.{os.getpid()}.tmp")
    if temporary_root.exists():
        shutil.rmtree(temporary_root)
    temporary_root.mkdir(parents=True, exist_ok=False)
    shape = runtime_kit_shape(request)
    contract = request["contract"]
    architecture = descriptor_architecture(request)
    (temporary_root / "package.json").write_text(
        json.dumps({"name": shape["kit_id"], "private": True, "type": "module"}, indent=2) + "\n", encoding="utf-8"
    )
    (temporary_root / "index.js").write_text(value["index_js"].rstrip() + "\n", encoding="utf-8")
    (temporary_root / "contract.test.js").write_text(value["test_js"].rstrip() + "\n", encoding="utf-8")
    descriptor = {
        "schema_version": KIT_DESCRIPTOR_SCHEMA,
        "kit_id": shape["kit_id"],
        "master_kit_id": request["source_context"]["master_kit_id"],
        "source_id": request["source_id"],
        "contract_hash": stable_hash(contract),
        "kit_identity_override": request.get("kit_identity_override"),
        "author_identity": identity,
        "author_model": model,
        "author_reasoning_effort": reasoning_effort,
        "author_prompt_version": AUTHOR_PROMPT_VERSION,
        "domain": slug(contract["domain"]),
        "subdomain": slug(contract["subdomain"]),
        **architecture,
        "contract": contract,
        "source_context": request["source_context"],
        "runtime_proof": "runtime-proof.json",
        "module": "index.js",
        "contract_test": "contract.test.js",
    }
    (temporary_root / "kit.json").write_text(json.dumps(descriptor, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest = {
        "schemaVersion": "kit.runtime-proof.v1",
        "packageRoot": ".",
        "engineRoot": str(engine_root.resolve()),
        "engineModule": "src/index.js",
        "module": "index.js",
        "publicImport": "index.js",
        "exportName": "createKit",
        "proofAdapterExport": "createProofAdapter",
        "kitId": shape["kit_id"],
        "domainPath": architecture["domain_path"],
        "kitRole": architecture["kit_role"],
        "promotionTier": architecture["promotion_tier"],
        "childKitIds": architecture["child_kit_ids"],
        "requires": architecture["requires"],
        "provides": architecture["provides"],
        "inputs": value["proof_inputs"],
        "expectedOutputs": value["expected_outputs"],
        "forbiddenImports": ["document.", "window.", "canvas", "three", "browser-host-lifecycle"],
        "testCommands": [["node", "--check", "index.js"], ["node", "--test", "contract.test.js"]],
        "sourceContext": request["source_context"],
    }
    (temporary_root / "runtime-proof-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary_root, root)


def _author_root(workspace: Path, master_id: str, identity: str) -> Path:
    return workspace / "authored-kits" / master_id / identity


def _runtime_proven_contracts(shards: Path) -> set[tuple[str, str]]:
    result: set[tuple[str, str]] = set()
    for item in _read_jsonl(shards.glob("runtime-build-ledger-*.jsonl")):
        if item.get("status") != "runtime-proven" or not item.get("source_id"):
            continue
        descriptor_path = Path(str(item.get("descriptor_path") or ""))
        try:
            descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        contract_hash = descriptor.get("contract_hash")
        if contract_hash:
            result.add((str(item["source_id"]), str(contract_hash)))
    return result


def _latest_runtime_failures(shards: Path) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for item in _read_jsonl(shards.glob("runtime-build-ledger-*.jsonl")):
        if item.get("source_id"):
            latest[str(item["source_id"])] = item
    return {
        source_id: item
        for source_id, item in latest.items()
        if item.get("status") == "runtime-failed" and item.get("proof_path")
    }


def _runtime_failure_signature(item: Optional[Dict[str, Any]]) -> str:
    if not item:
        return ""
    return stable_hash([
        item.get("pipeline_epoch"),
        item.get("proof_path"),
        item.get("proof_errors") or [],
    ])


def _latest_kit_identity_revisions(shards: Path) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for item in _read_jsonl(shards.glob("kit-identity-revisions-*.jsonl")):
        if item.get("source_id") and item.get("kit_identity_override"):
            latest[str(item["source_id"])] = item
    return latest


def _validate_author_output(
    value: Dict[str, Any], expected_kit_id: str, architecture: Dict[str, Any]
) -> None:
    if value.get("ok") is not True or value.get("kit_id") != expected_kit_id:
        raise ValueError("invalid-author-identity")
    module = value.get("index_js")
    test = value.get("test_js")
    if not isinstance(module, str) or not all(token in module for token in ("defineDomainServiceKit", "createKit", "createProofAdapter")):
        raise ValueError("incomplete-authored-module")
    if not all(token in module for token in ("resetPolicy", "snapshotPolicy", "handle")) or not re.search(r"engine\??\.n", module):
        raise ValueError("missing-simulator-lifecycle-contract")
    adapter_source = module[module.find("createProofAdapter") :]
    if ".installKit(" in adapter_source or "createEngine(" in adapter_source:
        raise ValueError("proof-adapter-reinstalls-runtime")
    module_imports = re.findall(r"\bfrom\s+[\"']([^\"']+)[\"']", module)
    allowed_engine_imports = {"../engine/src/index.js", "../engine/src/domain-service-kit.js"}
    if any(spec.startswith(".") and spec not in allowed_engine_imports for spec in module_imports):
        raise ValueError("nonportable-module-import")
    if not all(
        token in module
        for token in (
            expected_kit_id,
            "domainPath",
            architecture["domain_path"],
            "parentDomainPath",
            architecture["parent_domain_path"],
        )
    ):
        raise ValueError("missing-authored-domain-architecture")
    if architecture["kit_role"] == "assembly" and any(
        child_id not in module for child_id in architecture["child_kit_ids"]
    ):
        raise ValueError("missing-authored-child-kit-composition")
    if not isinstance(test, str) or "node:assert" not in test or "node:test" not in test:
        raise ValueError("missing-contract-test")
    if "engine.n" not in test and "createProofAdapter" not in test:
        raise ValueError("test-bypasses-domain-api")
    test_imports = re.findall(r"\bfrom\s+[\"']([^\"']+)[\"']", test)
    allowed_test_imports = allowed_engine_imports | {"./index.js"}
    if any(spec.startswith(".") and spec not in allowed_test_imports for spec in test_imports):
        raise ValueError("nonportable-test-import")
    forbidden_patterns = (
        r"\bdocument\s*\.",
        r"\bwindow\s*\.(?:document|location|navigator|addEventListener|removeEventListener|requestAnimationFrame|cancelAnimationFrame|localStorage|sessionStorage|innerWidth|innerHeight|devicePixelRatio)\b",
        r"\b(?:HTMLCanvasElement|CanvasRenderingContext2D|OffscreenCanvas|WebGLRenderingContext|WebGL2RenderingContext)\b",
        r"\bTHREE\s*\.",
        r"\bchild_process\b",
        r"\beval\s*\(",
        r"\bMath\s*\.\s*random\b",
        r"\bsetTimeout\s*\(",
    )
    if any(re.search(pattern, module) or re.search(pattern, test) for pattern in forbidden_patterns):
        raise ValueError("forbidden-authored-capability")
    for label, source in (("module", module), ("test", test)):
        result = subprocess.run(
            ["node", "--input-type=module", "--check", "-"],
            input=source,
            text=True,
            capture_output=True,
            timeout=AUTHOR_SYNTAX_TIMEOUT_SECONDS,
            check=False,
        )
        if result.returncode != 0:
            detail = str(result.stderr or result.stdout).strip().replace("\n", " ")[-500:]
            raise ValueError(f"{label}-syntax-error:{detail}")
    if not isinstance(value.get("proof_inputs"), list) or not value["proof_inputs"] or not all(
        isinstance(item, dict) for item in value["proof_inputs"]
    ):
        raise ValueError("missing-proof-inputs")
    if any(set(item) <= {"id", "requestId", "request_id"} for item in value["proof_inputs"]):
        raise ValueError("generic-proof-input")
    if not isinstance(value.get("expected_outputs"), list) or not value["expected_outputs"] or not all(
        isinstance(item, str) and item for item in value["expected_outputs"]
    ):
        raise ValueError("missing-expected-outputs")
    if any(item not in module for item in value["expected_outputs"]):
        raise ValueError("expected-output-not-literal-in-adapter")


def _run_contract_preflight(value: Dict[str, Any], engine_root: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="kit-author-preflight-") as temporary:
        root = Path(temporary)
        kit_root = root / "kit"
        kit_root.mkdir()
        (kit_root / "package.json").write_text('{"private":true,"type":"module"}\n', encoding="utf-8")
        (kit_root / "index.js").write_text(str(value["index_js"]).rstrip() + "\n", encoding="utf-8")
        (kit_root / "contract.test.js").write_text(str(value["test_js"]).rstrip() + "\n", encoding="utf-8")
        os.symlink(engine_root.resolve(), root / "engine", target_is_directory=True)
        result = subprocess.run(
            ["node", "--test", "contract.test.js"],
            cwd=kit_root,
            text=True,
            capture_output=True,
            timeout=120,
            check=False,
        )
        if result.returncode != 0:
            detail = str(result.stdout or result.stderr).strip().replace("\n", " ")[-1200:]
            raise ValueError(f"contract-test-preflight:{detail}")


def _parse_object(raw: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else raw[raw.find("{") :]
    value, end = json.JSONDecoder().raw_decode(candidate)
    trailing = candidate[end:].strip()
    if trailing:
        if trailing.startswith(",") and candidate[:end].rstrip().endswith("}"):
            repaired = candidate[: candidate[:end].rfind("}")] + trailing
            value = json.loads(repaired)
        else:
            raise json.JSONDecodeError("unexpected trailing author output", candidate, end)
    if not isinstance(value, dict):
        raise ValueError("author-output-not-object")
    return value


def _normalize_author_value(value: Dict[str, Any]) -> tuple[Dict[str, Any], list[Dict[str, str]]]:
    normalized, changes = _normalize_public_engine_imports(value)
    normalized, member_changes = _normalize_window_member_access(normalized)
    return normalized, [*changes, *member_changes]


def _normalize_public_engine_imports(value: Dict[str, Any]) -> tuple[Dict[str, Any], list[Dict[str, str]]]:
    """Route known Nexus Engine public re-exports through the SimSpace entrypoint.

    Raw model output remains immutable in the response artifact. Only exact engine
    modules that Nexus Engine's public index re-exports are normalized, and the
    resulting package still has to pass static validation and its real Node test.
    """
    normalized = dict(value)
    changes: list[Dict[str, str]] = []
    import_pattern = re.compile(r"(\bfrom\s+[\"'])([^\"']+)([\"'])")
    for field in ("index_js", "test_js"):
        source = normalized.get(field)
        if not isinstance(source, str):
            continue

        def replace(match: re.Match[str]) -> str:
            specifier = match.group(2)
            if specifier not in _PUBLIC_ENGINE_REEXPORTS:
                return match.group(0)
            changes.append({
                "field": field,
                "from": specifier,
                "to": _PUBLIC_ENGINE_IMPORT,
            })
            return f"{match.group(1)}{_PUBLIC_ENGINE_IMPORT}{match.group(3)}"

        normalized[field] = import_pattern.sub(replace, source)
    return normalized, changes


def _normalize_window_member_access(value: Dict[str, Any]) -> tuple[Dict[str, Any], list[Dict[str, str]]]:
    """Avoid NexusSimulator's legacy case-insensitive `window.` substring false positive.

    Local identifiers ending in Window are changed. An exact `window` owner is
    changed only for a narrow allowlist of mechanical timing/state members, so
    browser globals such as window.document, window.location, and window.open
    remain visible to the forbidden-capability gate.
    """
    normalized = dict(value)
    changes: list[Dict[str, str]] = []
    member_pattern = re.compile(r"\b((?:[A-Za-z_$][A-Za-z0-9_$]*)?[Ww]indow)\.([A-Za-z_$][A-Za-z0-9_$]*)")
    safe_exact_window_members = {
        "active",
        "attackId",
        "closedAt",
        "defenderId",
        "duration",
        "end",
        "max",
        "min",
        "openedAt",
        "requestKey",
        "start",
    }
    for field in ("index_js", "test_js"):
        source = normalized.get(field)
        if not isinstance(source, str):
            continue

        def replace(match: re.Match[str]) -> str:
            owner, member = match.groups()
            if owner.lower() == "window" and member not in safe_exact_window_members:
                return match.group(0)
            replacement = f'{owner}["{member}"]'
            changes.append({
                "field": field,
                "from": match.group(0),
                "to": replacement,
                "reason": "nexussimulator-window-substring-compat",
            })
            return replacement

        normalized[field] = member_pattern.sub(replace, source)
    return normalized, changes


def _execution_fields(execution: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "execution_report": execution.get("execution_report"),
        "scratch_cleaned": execution.get("scratch_cleaned"),
        "elapsed_seconds": execution.get("elapsed_seconds"),
        "_started_monotonic": execution.get("_started_monotonic"),
        "_finished_monotonic": execution.get("_finished_monotonic"),
    }


def _chunks(items: list[Dict[str, Any]], size: int) -> Iterator[list[Dict[str, Any]]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _read_jsonl(paths: Iterable[Path]) -> Iterator[Dict[str, Any]]:
    for path in sorted(paths):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def _free_gib(path: Path) -> float:
    return shutil.disk_usage(path).free / (1024 ** 3)


def _write_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)
