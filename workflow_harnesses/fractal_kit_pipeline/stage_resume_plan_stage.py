from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.fractal_kit_pipeline.stage_contracts import stage_contracts


ARTIFACTS_BY_STAGE: Dict[str, List[str]] = {
    "research-pack": ["research-pack.json"],
    "expansion-points": ["expansion-points.jsonl", "expansion-report.json"],
    "first-stage-breadth-audit": ["first-stage-breadth-audit.json"],
    "revealed-reduced": ["revealed-reduced.jsonl"],
    "revealed-reduced-audit": ["revealed-reduced-audit.json"],
    "idea-matrix": ["idea-matrix.jsonl"],
    "idea-matrix-audit": ["idea-matrix-audit.json"],
    "atomic-idempotent-filter": ["filtered-candidates.jsonl", "filter-report.json"],
    "atomic-filter-audit": ["atomic-filter-audit.json"],
    "recursive-domain-merge-review": ["domain-merge-report.json"],
    "domain-merge-input-audit": ["domain-merge-input-audit.json"],
    "domain-canonicalization": ["domain-canonicalized.jsonl", "domain-canonicalization-report.json"],
    "domain-canonicalization-audit": ["domain-canonicalization-audit.json"],
    "recursive-kit-merge-review": ["merge-review-report.json"],
    "kit-canonicalization": ["kit-canonicalized.jsonl", "kit-canonicalization-report.json"],
    "final-selection": ["selected-final-records.jsonl"],
    "final-quality-gate": ["final-quality-report.json"],
    "source-shape-audit": ["source-shape-audit.json"],
    "diversity-audit": ["diversity-audit.json"],
    "dependency-graph-audit": ["dependency-graph-audit.json"],
    "build-batch-manifest": ["build-batches.json"],
    "build-batch-replay-smoke": ["build-batch-replay-smoke.json"],
    "build-work-orders": ["build-work-orders.jsonl", "build-work-orders-report.json"],
    "build-batch-packets": ["build-inputs/index.json", "build-batch-packets-report.json"],
    "build-batch-dry-run": ["batch-results/index.json", "build-batch-dry-run-report.json"],
    "build-promotion-index": ["build-promotion-index.json"],
    "promoted-batches": ["promoted-batches/index.json", "promoted-batches-report.json"],
    "downstream-build-chain-integrity": ["downstream-build-chain-integrity.json"],
    "final-bucket-reconciliation": ["final-bucket-reconciliation.json"],
    "feed-forward-artifact-integrity": ["feed-forward-artifact-integrity.json"],
    "stage-contract-integrity": ["stage-contract-integrity.json"],
    "final-lineage-integrity": ["final-lineage-integrity.json"],
    "merge-review-coverage": ["merge-review-coverage.json"],
    "prompt-control-indirection": ["prompt-control-indirection.json"],
    "slot-decision-trace-integrity": ["slot-decision-trace-integrity.json"],
    "stage-resume-plan": ["stage-resume-plan.json"],
    "artifact-schema-index": ["artifact-schema-index.json"],
    "handoff-manifest": ["handoff-manifest.json"],
    "lfm-slot-decision-tree": ["lfm-slot-decisions.json"],
    "simulator-slot-smoke": ["simulator-slot-smoke.json"],
    "final-output": ["final-kits.jsonl", "stage-ledger.json", "objective-audit.json", "report.json", "report.md"],
}

DEFERRED_WHILE_BUILDING = {
    "artifact-schema-index",
    "stage-resume-plan",
    "handoff-manifest",
    "final-output",
}


def build_stage_resume_plan(run_dir: Path, target_count: int) -> Dict[str, Any]:
    manifest = _read_json(run_dir / "manifest.json")
    contracts = stage_contracts()
    contract_names = [contract["name"] for contract in contracts]
    manifest_names = list(manifest.get("stage_graph", []))
    steps = [_resume_step(run_dir, index, contract) for index, contract in enumerate(contracts)]
    missing_artifacts = [
        {
            "stage": step["name"],
            "missing": [artifact["relative_path"] for artifact in step["artifacts"] if not artifact["exists"]],
        }
        for step in steps
        if step["name"] not in DEFERRED_WHILE_BUILDING
        and any(not artifact["exists"] for artifact in step["artifacts"])
    ]
    checks = [
        _check(
            "contract-fields",
            "every stage contract has input, output, purpose, and gate text",
            all(_contract_has_required_fields(contract) for contract in contracts),
            {"contract_count": len(contracts)},
        ),
        _check(
            "manifest-stage-graph",
            "manifest stage graph matches code stage contracts",
            manifest_names == contract_names,
            {
                "manifest_stage_count": len(manifest_names),
                "contract_count": len(contract_names),
                "first_mismatch": _first_mismatch(manifest_names, contract_names),
            },
        ),
        _check(
            "artifact-map-complete",
            "every stage contract has a concrete artifact map entry",
            set(contract_names) == set(ARTIFACTS_BY_STAGE),
            {
                "missing_artifact_map": sorted(set(contract_names) - set(ARTIFACTS_BY_STAGE)),
                "extra_artifact_map": sorted(set(ARTIFACTS_BY_STAGE) - set(contract_names)),
            },
        ),
        _check(
            "completed-artifacts-present",
            "all completed non-deferred stage artifacts exist in the run directory",
            not missing_artifacts,
            {"missing_artifacts": missing_artifacts[:50]},
        ),
        _check(
            "resume-controls",
            "resume plan preserves target count, 100 context tokens, 128 predictions, and 128 workflow concurrency",
            manifest.get("target_count") == target_count
            and manifest.get("max_context_tokens", 999) <= 100
            and manifest.get("max_predictions") == 128
            and manifest.get("concurrency") == 128,
            {
                "target_count": manifest.get("target_count"),
                "max_context_tokens": manifest.get("max_context_tokens"),
                "max_predictions": manifest.get("max_predictions"),
                "concurrency": manifest.get("concurrency"),
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "stage-resume-plan",
        "target_count": target_count,
        "run_dir": str(run_dir),
        "stage_count": len(steps),
        "deferred_while_building": sorted(DEFERRED_WHILE_BUILDING),
        "resume_steps": steps,
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _resume_step(run_dir: Path, index: int, contract: Dict[str, Any]) -> Dict[str, Any]:
    name = contract["name"]
    artifacts = [_artifact(run_dir, artifact) for artifact in ARTIFACTS_BY_STAGE.get(name, [])]
    return {
        "index": index,
        "name": name,
        "input": contract["input"],
        "output": contract["output"],
        "purpose": contract["purpose"],
        "gate": contract["gate"],
        "artifacts": artifacts,
        "deferred": name in DEFERRED_WHILE_BUILDING,
        "resume_hint": _resume_hint(name),
    }


def _resume_hint(name: str) -> str:
    hints = {
        "expansion-points": "Resume from expansion-points.jsonl into revealed-reduced.jsonl.",
        "first-stage-breadth-audit": "Resume by checking expansion-report.json before reveal/reduce.",
        "revealed-reduced": "Resume from revealed-reduced.jsonl into idea-matrix.jsonl.",
        "revealed-reduced-audit": "Resume by checking revealed-reduced.jsonl before idea-matrix generation.",
        "idea-matrix": "Resume from idea-matrix.jsonl into filtered-candidates.jsonl.",
        "idea-matrix-audit": "Resume by checking idea-matrix.jsonl before atomic/idempotent filtering.",
        "atomic-idempotent-filter": "Resume from filtered-candidates.jsonl into recursive domain merge review.",
        "atomic-filter-audit": "Resume by checking filtered-candidates.jsonl and filter-report.json before merge review.",
        "recursive-domain-merge-review": "Resume from domain-merge-report.json into domain-canonicalized.jsonl.",
        "domain-merge-input-audit": "Resume by checking filtered-candidates.jsonl and domain-merge-report.json before canonicalization.",
        "domain-canonicalization-audit": "Resume by checking domain-canonicalized.jsonl before kit merge review.",
        "recursive-kit-merge-review": "Resume from merge-review-report.json into kit-canonicalized.jsonl.",
        "build-batch-packets": "Resume one batch at a time from build-inputs/<batch-id>/kit-records.jsonl.",
        "build-promotion-index": "Resume downstream batch work from build-promotion-index.json.",
        "promoted-batches": "Resume external builders from promoted-batches/index.json.",
        "final-output": "Resume final proof from final-kits.jsonl, final bucket shards, stage-ledger.json, and report.json.",
    }
    return hints.get(name, "Resume from the listed input artifacts and verify the listed gate.")


def _artifact(run_dir: Path, relative_path: str) -> Dict[str, Any]:
    path = run_dir / relative_path
    return {
        "relative_path": relative_path,
        "path": str(path),
        "exists": path.exists(),
        "bytes": path.stat().st_size if path.exists() else 0,
    }


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _contract_has_required_fields(contract: Dict[str, Any]) -> bool:
    return all(str(contract.get(field, "")).strip() for field in ["name", "input", "output", "purpose", "gate"])


def _first_mismatch(left: List[Any], right: List[Any]) -> Dict[str, Any]:
    for index, (left_item, right_item) in enumerate(zip(left, right)):
        if left_item != right_item:
            return {"index": index, "manifest": left_item, "contract": right_item}
    if len(left) != len(right):
        return {"index": min(len(left), len(right)), "manifest_len": len(left), "contract_len": len(right)}
    return {}


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
