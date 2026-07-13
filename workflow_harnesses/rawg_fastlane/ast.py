from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


AST_SCHEMA = "kituniverse.workflow-ast.v1"
ALLOWED_OPERATIONS = {
    "rawg.scan-group",
    "group.expand-representatives",
    "lfm.swarm-ideas",
    "lfm.refine-swarm",
    "swarm.merge-clusters",
    "lfm.refine-clusters",
    "workflow.support-first-pipeline",
    "filter.revalidate-atomic",
    "lfm.expand-representatives",
    "codex.review-recall",
    "codex.propose-kits",
    "workflow.report",
}


def load_ast(path: Path) -> Dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    validate_ast(value)
    return value


def validate_ast(value: Dict[str, Any]) -> None:
    if value.get("schema_version") != AST_SCHEMA:
        raise ValueError("invalid workflow AST schema")
    controls = value.get("controls") or {}
    if not 1 <= int(controls.get("task_concurrency", 1)) <= 64:
        raise ValueError("task_concurrency must be between 1 and 64")
    if int(controls.get("max_context_tokens", 0)) > 2000:
        raise ValueError("max_context_tokens may not exceed 2000")
    if int(controls.get("shard_max_bytes", 0)) > 90_000_000:
        raise ValueError("shard_max_bytes may not exceed 90000000")
    for model, limit in (controls.get("model_prediction_limits") or {}).items():
        ceiling = 64 if model == "lfm2.5-350m" else 8
        if not model or not 1 <= int(limit) <= ceiling:
            raise ValueError(f"prediction limit for {model} must be between 1 and {ceiling}")
    nodes = value.get("nodes") or []
    ids = [str(node.get("id") or "") for node in nodes]
    if not ids or any(not item for item in ids) or len(ids) != len(set(ids)):
        raise ValueError("workflow nodes need unique ids")
    for node in nodes:
        if node.get("op") not in ALLOWED_OPERATIONS:
            raise ValueError(f"unsupported operation: {node.get('op')}")
        if node.get("op") == "codex.review-recall":
            loop = node.get("loop") or {}
            if not 1 <= int(loop.get("max_passes", 1)) <= 3:
                raise ValueError("Codex review loop max_passes must be between 1 and 3")
            permissions = node.get("permissions") or {}
            if permissions.get("repo_edits") or permissions.get("shell_mutation"):
                raise ValueError("fast-lane Codex nodes are review-only")
    known = set(ids)
    for edge in value.get("edges") or []:
        if edge.get("from") not in known or edge.get("to") not in known:
            raise ValueError("edge references an unknown node")
    _topological_node_ids(value)


def ordered_nodes(value: Dict[str, Any]) -> List[Dict[str, Any]]:
    node_map = {node["id"]: node for node in value["nodes"]}
    return [node_map[node_id] for node_id in _topological_node_ids(value)]


def _topological_node_ids(value: Dict[str, Any]) -> List[str]:
    ids = [node["id"] for node in value.get("nodes") or []]
    incoming = {node_id: 0 for node_id in ids}
    outgoing = {node_id: [] for node_id in ids}
    for edge in value.get("edges") or []:
        incoming[edge["to"]] += 1
        outgoing[edge["from"]].append(edge["to"])
    ready = [node_id for node_id in ids if incoming[node_id] == 0]
    output = []
    while ready:
        node_id = ready.pop(0)
        output.append(node_id)
        for target in outgoing[node_id]:
            incoming[target] -= 1
            if incoming[target] == 0:
                ready.append(target)
    if len(output) != len(ids):
        raise ValueError("workflow AST contains a cycle")
    return output
