from __future__ import annotations

from typing import Any, Dict, List, Set


EXPECTED_POLICY = "expand aggressively, reject only obvious waste"
EXPECTED_RULE = "Generate unusual but relevant new items."
EXPECTED_AVOID = "exact repeats only"
EXPECTED_ALLOW = "rough names, strange angles, partial concepts"
EXPECTED_RETURN = "comma-separated list"
EXPECTED_LATER_STAGES = ["filter", "merge", "reduce", "map"]

ALLOWED_REJECTION_REASONS = {
    "empty-output",
    "empty",
    "exact-duplicate",
    "numeric-only",
    "formatting-junk",
    "record-unsafe",
    "direct-target-leakage",
    "relevance-check-n",
}

REQUIRED_DO_NOT_REJECT_FOR = {
    "awkward wording",
    "near-duplicate meaning",
    "weak grammar",
    "rough labels",
    "odd but connected ideas",
}


def build_first_stage_breadth_audit(
    expansion_points: List[Dict[str, Any]],
    expansion_report: Dict[str, Any],
) -> Dict[str, Any]:
    rejection_reasons = _rejection_reasons(expansion_report.get("rejections", []))
    accepted_count = int(expansion_report.get("accepted_count", len(expansion_points)))
    candidate_count = int(expansion_report.get("candidate_count", 0))
    raw_generations = int(expansion_report.get("raw_generations", 0))
    acceptance_ratio = accepted_count / candidate_count if candidate_count else 0.0
    prompt_stance = expansion_report.get("prompt_stance", {})
    do_not_reject_for = set(expansion_report.get("do_not_reject_for", []))
    checks = [
        _check(
            "policy",
            "first stage uses aggressive expansion policy",
            expansion_report.get("policy") == EXPECTED_POLICY,
            {"policy": expansion_report.get("policy")},
        ),
        _check(
            "prompt-stance",
            "prompt asks for unusual relevant items, avoids exact repeats only, allows rough partial concepts",
            prompt_stance.get("rule") == EXPECTED_RULE
            and prompt_stance.get("avoid") == EXPECTED_AVOID
            and prompt_stance.get("allow") == EXPECTED_ALLOW
            and prompt_stance.get("return") == EXPECTED_RETURN,
            {"prompt_stance": prompt_stance},
        ),
        _check(
            "later-cleanup-boundary",
            "semantic cleanup remains deferred to filter, merge, reduce, and map",
            expansion_report.get("later_stages") == EXPECTED_LATER_STAGES,
            {"later_stages": expansion_report.get("later_stages")},
        ),
        _check(
            "do-not-reject-roughness",
            "rough wording, near duplicates, weak grammar, rough labels, and odd connected ideas are not first-stage rejection reasons",
            REQUIRED_DO_NOT_REJECT_FOR.issubset(do_not_reject_for),
            {
                "required": sorted(REQUIRED_DO_NOT_REJECT_FOR),
                "actual": sorted(do_not_reject_for),
                "missing": sorted(REQUIRED_DO_NOT_REJECT_FOR - do_not_reject_for),
            },
        ),
        _check(
            "rejection-reasons-bounded",
            "first stage rejection reasons are limited to obvious waste, exact repeats, record safety, target leakage, or loose relevance failure",
            rejection_reasons.issubset(ALLOWED_REJECTION_REASONS),
            {
                "allowed": sorted(ALLOWED_REJECTION_REASONS),
                "actual": sorted(rejection_reasons),
                "unexpected": sorted(rejection_reasons - ALLOWED_REJECTION_REASONS),
            },
        ),
        _check(
            "accepted-records-match-report",
            "accepted expansion point records match expansion report accepted count",
            len(expansion_points) == accepted_count and accepted_count > 0,
            {"records": len(expansion_points), "accepted_count": accepted_count},
        ),
        _check(
            "exact-unique-keys",
            "accepted expansion point keys are exact-unique",
            len(expansion_points) == len({record.get("key") for record in expansion_points}),
            {
                "records": len(expansion_points),
                "unique_keys": len({record.get("key") for record in expansion_points}),
            },
        ),
        _check(
            "not-over-filtered",
            "first stage keeps breadth unless candidates are obvious waste or fail loose relevance",
            candidate_count == 0 or acceptance_ratio >= 0.25 or accepted_count >= raw_generations,
            {
                "candidate_count": candidate_count,
                "accepted_count": accepted_count,
                "raw_generations": raw_generations,
                "acceptance_ratio": round(acceptance_ratio, 4),
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "first-stage-breadth-audit",
        "policy": expansion_report.get("policy"),
        "accepted_count": accepted_count,
        "candidate_count": candidate_count,
        "rejected_count": int(expansion_report.get("rejected_count", 0)),
        "acceptance_ratio": round(acceptance_ratio, 4),
        "rejection_reasons": sorted(rejection_reasons),
        "semantic_cleanup_deferred_to": expansion_report.get("later_stages", []),
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _rejection_reasons(rejections: List[Dict[str, Any]]) -> Set[str]:
    reasons: Set[str] = set()
    for rejection in rejections:
        reason = str(rejection.get("reason", "")).strip()
        if not reason:
            continue
        for part in reason.split(","):
            if part.strip():
                reasons.add(part.strip())
    return reasons


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
