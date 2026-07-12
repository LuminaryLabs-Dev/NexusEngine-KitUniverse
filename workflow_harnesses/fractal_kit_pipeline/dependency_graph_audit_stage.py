from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List


def build_dependency_graph_audit(final_records: List[Dict[str, Any]], target_count: int) -> Dict[str, Any]:
    required_tokens = Counter()
    provided_tokens = Counter()
    primary_dependencies = Counter()
    missing_requires = []
    missing_provides = []
    missing_primary_dependency = []
    missing_domain_provide = []
    direct_self_edges = []
    malformed_tokens = []

    for record in final_records:
        record_id = record.get("record_id")
        payload = record.get("payload", {})
        requires = payload.get("requires") or []
        provides = payload.get("provides") or []
        primary_dependency = payload.get("primary_dependency")
        domain_path = payload.get("domain_path")
        require_set = set(requires)
        provide_set = set(provides)

        if not requires:
            missing_requires.append(record_id)
        if not provides:
            missing_provides.append(record_id)
        if primary_dependency not in require_set:
            missing_primary_dependency.append(record_id)
        if domain_path not in provide_set:
            missing_domain_provide.append(record_id)
        if require_set & provide_set:
            direct_self_edges.append(record_id)
        for token in list(require_set) + list(provide_set):
            if not _namespaced(token):
                malformed_tokens.append({"record_id": record_id, "token": token})

        required_tokens.update(require_set)
        provided_tokens.update(provide_set)
        primary_dependencies[primary_dependency] += 1

    thresholds = _thresholds(target_count)
    checks = [
        _check(
            "requires-present",
            "every final kit declares at least one required dependency",
            not missing_requires,
            {"missing_count": len(missing_requires), "sample": missing_requires[:10]},
        ),
        _check(
            "provides-present",
            "every final kit declares at least one provided capability",
            not missing_provides,
            {"missing_count": len(missing_provides), "sample": missing_provides[:10]},
        ),
        _check(
            "primary-dependency-required",
            "primary dependency is always present in requires",
            not missing_primary_dependency,
            {"missing_count": len(missing_primary_dependency), "sample": missing_primary_dependency[:10]},
        ),
        _check(
            "domain-path-provided",
            "domain path is always present in provides",
            not missing_domain_provide,
            {"missing_count": len(missing_domain_provide), "sample": missing_domain_provide[:10]},
        ),
        _check(
            "no-direct-self-edge",
            "no kit requires the same token it provides",
            not direct_self_edges,
            {"self_edge_count": len(direct_self_edges), "sample": direct_self_edges[:10]},
        ),
        _check(
            "tokens-namespaced",
            "all dependency tokens are namespaced with ':'",
            not malformed_tokens,
            {"malformed_count": len(malformed_tokens), "sample": malformed_tokens[:10]},
        ),
        _check(
            "require-breadth",
            "required dependency root set is broad enough for the configured target",
            len(required_tokens) >= thresholds["min_required_tokens"],
            {"unique_required_tokens": len(required_tokens), "min_required_tokens": thresholds["min_required_tokens"]},
        ),
        _check(
            "provide-breadth",
            "provided capability set is broad enough for the configured target",
            len(provided_tokens) >= thresholds["min_provided_tokens"],
            {"unique_provided_tokens": len(provided_tokens), "min_provided_tokens": thresholds["min_provided_tokens"]},
        ),
        _check(
            "primary-dependency-breadth",
            "primary dependencies cover enough root areas",
            len(primary_dependencies) >= thresholds["min_primary_dependencies"],
            {
                "unique_primary_dependencies": len(primary_dependencies),
                "min_primary_dependencies": thresholds["min_primary_dependencies"],
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "dependency-graph-audit",
        "record_count": len(final_records),
        "target_count": target_count,
        "thresholds": thresholds,
        "unique_required_tokens": len(required_tokens),
        "unique_provided_tokens": len(provided_tokens),
        "unique_primary_dependencies": len(primary_dependencies),
        "missing_requires": len(missing_requires),
        "missing_provides": len(missing_provides),
        "missing_primary_dependency": len(missing_primary_dependency),
        "missing_domain_provide": len(missing_domain_provide),
        "direct_self_edges": len(direct_self_edges),
        "malformed_tokens": len(malformed_tokens),
        "top_required_tokens": _top_counts(required_tokens, 12),
        "top_provided_tokens": _top_counts(provided_tokens, 12),
        "top_primary_dependencies": _top_counts(primary_dependencies, 12),
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _thresholds(target_count: int) -> Dict[str, int]:
    return {
        "min_required_tokens": min(8, max(1, target_count // 1024)),
        "min_provided_tokens": min(512, max(8, target_count // 16)),
        "min_primary_dependencies": min(8, max(1, target_count // 1024)),
    }


def _namespaced(token: Any) -> bool:
    return isinstance(token, str) and ":" in token and bool(token.split(":", 1)[0]) and bool(token.split(":", 1)[1])


def _top_counts(counter: Counter, limit: int) -> List[Dict[str, Any]]:
    return [{"value": value, "count": count} for value, count in counter.most_common(limit)]


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
