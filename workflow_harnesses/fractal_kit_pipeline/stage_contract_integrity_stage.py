from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.fractal_kit_pipeline.stage_contracts import stage_contracts


REQUIRED_FIELDS = ["name", "input", "output", "purpose", "gate"]


def build_stage_contract_integrity(run_dir: Path) -> Dict[str, Any]:
    code_contracts = stage_contracts()
    code_names = [contract.get("name") for contract in code_contracts]
    manifest = _read_json(run_dir / "manifest.json")
    artifact = _read_json(run_dir / "stage-contracts.json")
    artifact_contracts = artifact.get("stages", [])
    artifact_names = [contract.get("name") for contract in artifact_contracts]
    manifest_names = manifest.get("stage_graph", [])

    duplicate_code_names = _duplicates(code_names)
    missing_fields = [
        {"stage": contract.get("name", ""), "field": field}
        for contract in code_contracts
        for field in REQUIRED_FIELDS
        if not str(contract.get(field, "")).strip()
    ]
    checks = [
        _check(
            "manifest-stage-graph",
            "manifest stage_graph matches the code-owned stage contract order",
            manifest_names == code_names,
            {
                "manifest_stage_count": len(manifest_names),
                "code_stage_count": len(code_names),
                "first_mismatch": _first_mismatch(manifest_names, code_names),
            },
        ),
        _check(
            "stage-contract-artifact",
            "stage-contracts.json matches the code-owned stage contract order",
            artifact_names == code_names,
            {
                "artifact_stage_count": len(artifact_names),
                "code_stage_count": len(code_names),
                "first_mismatch": _first_mismatch(artifact_names, code_names),
            },
        ),
        _check(
            "unique-stage-names",
            "stage contract names are unique",
            not duplicate_code_names,
            {"duplicate_stage_names": duplicate_code_names},
        ),
        _check(
            "required-contract-fields",
            "every stage contract has name, input, output, purpose, and gate",
            not missing_fields,
            {"missing_fields": missing_fields[:50]},
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "stage-contract-integrity",
        "run_dir": str(run_dir),
        "contract_count": len(code_contracts),
        "manifest_stage_count": len(manifest_names),
        "artifact_stage_count": len(artifact_names),
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _duplicates(values: List[Any]) -> List[Any]:
    seen = set()
    duplicates = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def _first_mismatch(left: List[Any], right: List[Any]) -> Dict[str, Any]:
    for index, (left_value, right_value) in enumerate(zip(left, right)):
        if left_value != right_value:
            return {"index": index, "left": left_value, "right": right_value}
    if len(left) != len(right):
        return {"index": min(len(left), len(right)), "left_count": len(left), "right_count": len(right)}
    return {}


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
