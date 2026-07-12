from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


JSONL_ARTIFACTS = [
    "expansion-points.jsonl",
    "revealed-reduced.jsonl",
    "idea-matrix.jsonl",
    "filtered-candidates.jsonl",
    "domain-canonicalized.jsonl",
    "kit-canonicalized.jsonl",
    "selected-final-records.jsonl",
    "final-kits.jsonl",
    "build-work-orders.jsonl",
]

JSON_ARTIFACTS = [
    "manifest.json",
    "research-pack.json",
    "expansion-report.json",
    "first-stage-breadth-audit.json",
    "revealed-reduced-audit.json",
    "idea-matrix-audit.json",
    "filter-report.json",
    "atomic-filter-audit.json",
    "domain-merge-report.json",
    "domain-merge-input-audit.json",
    "domain-canonicalization-report.json",
    "domain-canonicalization-audit.json",
    "merge-review-report.json",
    "kit-canonicalization-report.json",
    "final-quality-report.json",
    "source-shape-audit.json",
    "diversity-audit.json",
    "dependency-graph-audit.json",
    "build-batches.json",
    "build-work-orders-report.json",
    "build-inputs/index.json",
    "build-batch-packets-report.json",
    "batch-results/index.json",
    "build-batch-dry-run-report.json",
    "build-promotion-index.json",
    "promoted-batches/index.json",
    "promoted-batches-report.json",
    "downstream-build-chain-integrity.json",
    "final-bucket-reconciliation.json",
    "feed-forward-artifact-integrity.json",
    "stage-contract-integrity.json",
    "final-lineage-integrity.json",
    "merge-review-coverage.json",
    "prompt-control-indirection.json",
    "slot-decision-trace-integrity.json",
    "stage-resume-plan.json",
    "lfm-slot-decisions.json",
    "simulator-slot-smoke.json",
]

REQUIRED_JSONL_FIELDS = {
    "expansion-points.jsonl": ["stage", "value", "key"],
    "revealed-reduced.jsonl": ["expansion_point", "revealed", "reduced"],
    "idea-matrix.jsonl": ["record_id", "payload"],
    "filtered-candidates.jsonl": ["record_id", "payload"],
    "domain-canonicalized.jsonl": ["record_id", "payload"],
    "kit-canonicalized.jsonl": ["record_id", "payload"],
    "selected-final-records.jsonl": ["record_id", "payload"],
    "final-kits.jsonl": ["record_id", "payload"],
    "build-work-orders.jsonl": ["work_order_id", "batch_id", "record_ids"],
}

REQUIRED_PAYLOAD_FIELDS = {
    "idea-matrix.jsonl": ["name", "domain_path", "requires", "provides", "idempotent", "atomic"],
    "filtered-candidates.jsonl": ["name", "domain_path", "requires", "provides", "idempotent", "atomic"],
    "domain-canonicalized.jsonl": ["name", "domain_path", "canonical_domain_path"],
    "kit-canonicalized.jsonl": ["name", "canonical_kit_record_id", "requires", "provides"],
    "selected-final-records.jsonl": ["name", "domain_path", "requires", "provides"],
    "final-kits.jsonl": ["name", "domain_path", "requires", "provides", "public_api", "tests"],
}

REQUIRED_JSON_FIELDS = {
    "manifest.json": ["run_id", "target_count", "stage_graph", "max_context_tokens", "max_predictions"],
    "first-stage-breadth-audit.json": ["ok", "checks", "accepted_count", "candidate_count"],
    "revealed-reduced-audit.json": ["ok", "checks", "expansion_points", "reveal_records"],
    "idea-matrix-audit.json": ["ok", "checks", "record_count", "unique_semantic_keys"],
    "atomic-filter-audit.json": ["ok", "checks", "matrix_records", "filtered_records"],
    "domain-merge-input-audit.json": ["ok", "checks", "filtered_records", "unique_domains"],
    "domain-canonicalization-audit.json": ["ok", "checks", "canonicalized_records", "canonicalized_count"],
    "build-batches.json": ["ok", "batch_count", "batches"],
    "build-inputs/index.json": ["packets"],
    "batch-results/index.json": ["batches"],
    "build-promotion-index.json": ["ok", "ready_count", "ready"],
    "promoted-batches/index.json": ["promoted", "blocked"],
    "downstream-build-chain-integrity.json": ["ok", "counts", "checks"],
    "stage-resume-plan.json": ["ok", "resume_steps", "checks"],
}

SAMPLE_LIMIT = 64


def build_artifact_schema_index(run_dir: Path, target_count: int) -> Dict[str, Any]:
    jsonl_entries = [_jsonl_schema(run_dir, relative_path) for relative_path in JSONL_ARTIFACTS]
    json_entries = [_json_schema(run_dir, relative_path) for relative_path in JSON_ARTIFACTS]
    checks = [
        _check(
            "jsonl-artifacts-safe",
            "all indexed JSONL artifacts exist, parse, and expose required fields",
            all(entry["ok"] for entry in jsonl_entries),
            {"failed": _failed_entries(jsonl_entries)},
        ),
        _check(
            "json-artifacts-safe",
            "all indexed JSON artifacts exist, parse, and expose required fields",
            all(entry["ok"] for entry in json_entries),
            {"failed": _failed_entries(json_entries)},
        ),
        _check(
            "final-target-count",
            "final JSONL schema entry reports the configured target count",
            _entry_count(jsonl_entries, "final-kits.jsonl") == target_count,
            {
                "target_count": target_count,
                "final_count": _entry_count(jsonl_entries, "final-kits.jsonl"),
            },
        ),
        _check(
            "schema-index-breadth",
            "schema index covers feed-forward, merge, validation, downstream, handoff, and smoke artifacts",
            len(jsonl_entries) == len(JSONL_ARTIFACTS) and len(json_entries) == len(JSON_ARTIFACTS),
            {"jsonl_artifacts": len(jsonl_entries), "json_artifacts": len(json_entries)},
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "artifact-schema-index",
        "target_count": target_count,
        "sample_limit": SAMPLE_LIMIT,
        "jsonl_artifact_count": len(jsonl_entries),
        "json_artifact_count": len(json_entries),
        "jsonl_artifacts": jsonl_entries,
        "json_artifacts": json_entries,
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _jsonl_schema(run_dir: Path, relative_path: str) -> Dict[str, Any]:
    path = run_dir / relative_path
    if not path.exists():
        return _schema_entry(relative_path, "jsonl", False, ["missing-artifact"])
    top_fields: Set[str] = set()
    payload_fields: Set[str] = set()
    type_map: Dict[str, Set[str]] = {}
    malformed = 0
    count = 0
    samples = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            count += 1
            if samples >= SAMPLE_LIMIT:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            samples += 1
            if isinstance(record, dict):
                top_fields.update(record.keys())
                _merge_types(type_map, "", record)
                payload = record.get("payload")
                if isinstance(payload, dict):
                    payload_fields.update(payload.keys())
                    _merge_types(type_map, "payload.", payload)
    missing_fields = sorted(set(REQUIRED_JSONL_FIELDS.get(relative_path, [])) - top_fields)
    missing_payload = sorted(set(REQUIRED_PAYLOAD_FIELDS.get(relative_path, [])) - payload_fields)
    errors = []
    if malformed:
        errors.append("malformed-jsonl")
    if count == 0:
        errors.append("empty-jsonl")
    if missing_fields:
        errors.append("missing-fields")
    if missing_payload:
        errors.append("missing-payload-fields")
    return {
        **_schema_entry(relative_path, "jsonl", not errors, errors),
        "line_count": count,
        "sampled_records": samples,
        "malformed": malformed,
        "fields": sorted(top_fields),
        "payload_fields": sorted(payload_fields),
        "required_fields": REQUIRED_JSONL_FIELDS.get(relative_path, []),
        "missing_fields": missing_fields,
        "required_payload_fields": REQUIRED_PAYLOAD_FIELDS.get(relative_path, []),
        "missing_payload_fields": missing_payload,
        "types": _sorted_type_map(type_map),
    }


def _json_schema(run_dir: Path, relative_path: str) -> Dict[str, Any]:
    path = run_dir / relative_path
    if not path.exists():
        return _schema_entry(relative_path, "json", False, ["missing-artifact"])
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _schema_entry(relative_path, "json", False, ["malformed-json"])
    fields = set(data.keys()) if isinstance(data, dict) else set()
    type_map: Dict[str, Set[str]] = {}
    if isinstance(data, dict):
        _merge_types(type_map, "", data)
    missing_fields = sorted(set(REQUIRED_JSON_FIELDS.get(relative_path, [])) - fields)
    errors = []
    if not isinstance(data, dict):
        errors.append("not-object")
    if missing_fields:
        errors.append("missing-fields")
    return {
        **_schema_entry(relative_path, "json", not errors, errors),
        "fields": sorted(fields),
        "required_fields": REQUIRED_JSON_FIELDS.get(relative_path, []),
        "missing_fields": missing_fields,
        "types": _sorted_type_map(type_map),
    }


def _schema_entry(relative_path: str, artifact_type: str, ok: bool, errors: List[str]) -> Dict[str, Any]:
    return {
        "relative_path": relative_path,
        "artifact_type": artifact_type,
        "ok": bool(ok),
        "errors": errors,
    }


def _merge_types(type_map: Dict[str, Set[str]], prefix: str, value: Dict[str, Any]) -> None:
    for key, item in value.items():
        path = f"{prefix}{key}"
        type_map.setdefault(path, set()).add(_type_name(item))
        if isinstance(item, dict):
            for child_key, child_value in item.items():
                type_map.setdefault(f"{path}.{child_key}", set()).add(_type_name(child_value))


def _type_name(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "object"
    if value is None:
        return "null"
    return type(value).__name__


def _sorted_type_map(type_map: Dict[str, Set[str]]) -> Dict[str, List[str]]:
    return {key: sorted(values) for key, values in sorted(type_map.items())}


def _failed_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "relative_path": entry.get("relative_path"),
            "errors": entry.get("errors", []),
            "missing_fields": entry.get("missing_fields", []),
            "missing_payload_fields": entry.get("missing_payload_fields", []),
        }
        for entry in entries
        if not entry.get("ok")
    ][:50]


def _entry_count(entries: List[Dict[str, Any]], relative_path: str) -> int:
    for entry in entries:
        if entry.get("relative_path") == relative_path:
            return int(entry.get("line_count", 0))
    return 0


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
