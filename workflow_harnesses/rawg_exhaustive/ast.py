from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from .contracts import WORKFLOW_AST_SCHEMA


ALLOWED_OPERATIONS = {
    "rawg.map-evidence",
    "lfm.extract-interactions",
    "kit.expand-pointer-decomposition",
    "master.merge-observations",
    "lfm.refine-master-kits",
    "codex.review-master-kits",
    "kit.enqueue-builds",
    "kit.build-runtime-prove",
    "workflow.report",
}


def load_ast(path: Path) -> Dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    validate_ast(value)
    return value


def validate_ast(value: Dict[str, Any]) -> None:
    if value.get("schema_version") != WORKFLOW_AST_SCHEMA:
        raise ValueError("invalid exhaustive workflow AST schema")
    controls = value.get("controls") or {}
    if not 1 <= int(controls.get("task_concurrency", 0)) <= 64:
        raise ValueError("task_concurrency must be between 1 and 64")
    if int(controls.get("max_context_tokens", 0)) > 2000:
        raise ValueError("max_context_tokens may not exceed 2000")
    if not 1 <= int((controls.get("model_prediction_limits") or {}).get("lfm2.5-350m", 0)) <= 64:
        raise ValueError("lfm2.5-350m prediction limit must be between 1 and 64")
    if not 1 <= int((controls.get("model_prediction_limits") or {}).get("lfm2.5-1.2b-instruct", 0)) <= 8:
        raise ValueError("lfm2.5-1.2b-instruct prediction limit must be between 1 and 8")
    if not 1 <= int(controls.get("shard_max_bytes", 0)) <= 90_000_000:
        raise ValueError("shard_max_bytes must be between 1 and 90000000")
    nodes = value.get("nodes") or []
    ids = [str(node.get("id") or "") for node in nodes]
    if not ids or any(not item for item in ids) or len(ids) != len(set(ids)):
        raise ValueError("workflow nodes need unique ids")
    if any(node.get("op") not in ALLOWED_OPERATIONS for node in nodes):
        raise ValueError("workflow contains unsupported operation")
    _ordered_ids(value)


def ordered_nodes(value: Dict[str, Any]) -> List[Dict[str, Any]]:
    by_id = {node["id"]: node for node in value["nodes"]}
    return [by_id[item] for item in _ordered_ids(value)]


def _ordered_ids(value: Dict[str, Any]) -> List[str]:
    ids = [node["id"] for node in value.get("nodes") or []]
    incoming = {item: 0 for item in ids}
    outgoing = {item: [] for item in ids}
    for edge in value.get("edges") or []:
        if edge.get("from") not in incoming or edge.get("to") not in incoming:
            raise ValueError("edge references unknown node")
        incoming[edge["to"]] += 1
        outgoing[edge["from"]].append(edge["to"])
    ready = [item for item in ids if incoming[item] == 0]
    ordered: List[str] = []
    while ready:
        item = ready.pop(0)
        ordered.append(item)
        for target in outgoing[item]:
            incoming[target] -= 1
            if incoming[target] == 0:
                ready.append(target)
    if len(ordered) != len(ids):
        raise ValueError("workflow AST contains a cycle")
    return ordered
