from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, List


DATASET_ID = "IVproger/rawg-games-dataset-updated"
RAWG_SOURCE_SCHEMA = "rawg.source.v1"
MECHANIC_EVIDENCE_SCHEMA = "mechanic.evidence.v1"
CAPABILITY_CLUSTER_SCHEMA = "capability.cluster.v1"
ENGINE_GAP_SCHEMA = "engine.gap.v1"
KIT_BUILD_REQUEST_SCHEMA = "kit.build-request.v1"


def slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def split_terms(value: Any) -> List[str]:
    if isinstance(value, list):
        raw: Iterable[Any] = value
    elif value is None:
        raw = []
    else:
        raw = [value]
    output: List[str] = []
    for item in raw:
        for part in re.split(r"[,|;]", str(item)):
            text = part.strip()
            if text and text not in output:
                output.append(text)
    return output


def source_identity(record: Dict[str, Any]) -> str:
    return ":".join(
        [
            str(record.get("dataset", DATASET_ID)),
            str(record.get("source_id", "unknown")),
            str(record.get("source_hash", "missing")),
            str(record.get("pipeline_epoch", "unknown")),
        ]
    )


def validate_build_request(value: Dict[str, Any]) -> List[str]:
    context = value.get("source_context") or {}
    errors = []
    if value.get("schema_version") != KIT_BUILD_REQUEST_SCHEMA:
        errors.append("invalid-build-request-schema")
    if not value.get("source_id") or not value.get("description"):
        errors.append("missing-build-request-source")
    if context.get("schema_version") != KIT_BUILD_REQUEST_SCHEMA:
        errors.append("missing-build-request-context")
    if not context.get("capability_cluster_id"):
        errors.append("missing-capability-cluster-id")
    if not context.get("rawg_source_ids"):
        errors.append("missing-rawg-source-ids")
    if not context.get("evidence_hash"):
        errors.append("missing-evidence-hash")
    return errors
