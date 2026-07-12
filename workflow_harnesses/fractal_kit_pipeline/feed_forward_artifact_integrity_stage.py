from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.fractal_kit_pipeline.run_artifacts import count_jsonl


def build_feed_forward_artifact_integrity(
    run_dir: Path,
    target_count: int,
    expected_counts: Dict[str, int],
) -> Dict[str, Any]:
    artifact_specs = [
        ("expansion-points", "expansion-points.jsonl", expected_counts.get("expansion_points", 0), 1),
        ("revealed-reduced", "revealed-reduced.jsonl", expected_counts.get("reveal_records", 0), 1),
        ("idea-matrix", "idea-matrix.jsonl", expected_counts.get("idea_records", 0), target_count),
        ("filtered-candidates", "filtered-candidates.jsonl", expected_counts.get("filtered_records", 0), target_count),
        (
            "domain-canonicalized",
            "domain-canonicalized.jsonl",
            expected_counts.get("canonicalized_records", 0),
            target_count,
        ),
        (
            "kit-canonicalized",
            "kit-canonicalized.jsonl",
            expected_counts.get("kit_canonicalized_records", 0),
            target_count,
        ),
        ("selected-final-records", "selected-final-records.jsonl", expected_counts.get("final_records", 0), target_count),
        ("final-kits", "final-kits.jsonl", expected_counts.get("final_records", 0), target_count),
    ]
    artifacts: List[Dict[str, Any]] = []
    for name, relative_path, expected_count, minimum_count in artifact_specs:
        path = run_dir / relative_path
        exists = path.exists()
        line_count, malformed = count_jsonl(path) if exists else (0, 0)
        artifacts.append(
            {
                "name": name,
                "path": relative_path,
                "exists": exists,
                "line_count": line_count,
                "expected_count": expected_count,
                "minimum_count": minimum_count,
                "malformed": malformed,
                "ok": exists and malformed == 0 and line_count == expected_count and line_count >= minimum_count,
            }
        )
    checks = [
        _check(
            "all-artifacts-present",
            "all feed-forward JSONL artifacts exist",
            all(artifact["exists"] for artifact in artifacts),
            {"missing": [artifact["path"] for artifact in artifacts if not artifact["exists"]]},
        ),
        _check(
            "no-malformed-jsonl",
            "all feed-forward JSONL artifacts have zero malformed lines",
            all(artifact["malformed"] == 0 for artifact in artifacts),
            {
                "malformed": {
                    artifact["path"]: artifact["malformed"]
                    for artifact in artifacts
                    if artifact["malformed"]
                }
            },
        ),
        _check(
            "counts-match-report",
            "artifact line counts match the stage counts carried in the run report",
            all(artifact["line_count"] == artifact["expected_count"] for artifact in artifacts),
            {
                "mismatches": [
                    {
                        "path": artifact["path"],
                        "line_count": artifact["line_count"],
                        "expected_count": artifact["expected_count"],
                    }
                    for artifact in artifacts
                    if artifact["line_count"] != artifact["expected_count"]
                ]
            },
        ),
        _check(
            "large-stage-coverage",
            "matrix, filtered, canonicalized, selected, and final artifacts meet target coverage",
            all(artifact["line_count"] >= artifact["minimum_count"] for artifact in artifacts),
            {
                "below_minimum": [
                    {
                        "path": artifact["path"],
                        "line_count": artifact["line_count"],
                        "minimum_count": artifact["minimum_count"],
                    }
                    for artifact in artifacts
                    if artifact["line_count"] < artifact["minimum_count"]
                ]
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks) and all(artifact["ok"] for artifact in artifacts),
        "stage": "feed-forward-artifact-integrity",
        "target_count": target_count,
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
