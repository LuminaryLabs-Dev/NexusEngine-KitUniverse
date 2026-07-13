from __future__ import annotations

from typing import Any, Dict

from workflow_harnesses.rawg_capability_pipeline.contracts import stable_hash
from workflow_harnesses.rawg_matrix_optimizer.workflow_rawg_matrix_optimizer import _matrix_evidence_units


def normalize_for_fastlane(source: Dict[str, Any]) -> Dict[str, Any]:
    units = _matrix_evidence_units(source)
    return {
        "schema_version": "rawg.fast-normalized.v1",
        "identity": stable_hash([source.get("source_hash"), "rawg.fast-normalized.v1"]),
        "source_id": source.get("source_id"),
        "source_hash": source.get("source_hash"),
        "source_file": source.get("source_file"),
        "source_line": source.get("source_line"),
        "name": source.get("name"),
        "evidence_units": units,
        "mechanic_evidence": bool(units),
        "error": source.get("error"),
    }
