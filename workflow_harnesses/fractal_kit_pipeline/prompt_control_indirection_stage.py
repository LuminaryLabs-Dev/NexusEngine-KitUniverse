from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


FORBIDDEN_GENERATION_TERMS = [
    "nexus",
    "protokit",
    "proto kit",
    "domain service kit",
    "domain-service-kit",
    "atomic-domain-service-kit",
]


def build_prompt_control_indirection(
    run_dir: Path,
    router_stats: Dict[str, Any],
) -> Dict[str, Any]:
    manifest = _read_json(run_dir / "manifest.json")
    expansion_prompts = _expansion_prompts(run_dir / "expansion-points.jsonl")
    expansion_report = _read_json(run_dir / "expansion-report.json")
    rejection_prompts = [
        str(item.get("prompt", ""))
        for item in expansion_report.get("rejections", [])
        if str(item.get("prompt", "")).strip()
    ]
    generation_prompts = expansion_prompts + rejection_prompts
    leaks = _forbidden_hits(generation_prompts)
    schedule = manifest.get("temperature_schedule", {})
    checks = [
        _check(
            "generic-list-goal",
            "LIST GOAL stays generic and does not name the final Nexus kit target",
            not _contains_forbidden(str(manifest.get("list_goal", ""))),
            {"list_goal": manifest.get("list_goal", ""), "forbidden_terms": FORBIDDEN_GENERATION_TERMS},
        ),
        _check(
            "generic-source",
            "source description stays generic and does not name the final Nexus kit target",
            not _contains_forbidden(str(manifest.get("source", ""))),
            {"source": manifest.get("source", ""), "forbidden_terms": FORBIDDEN_GENERATION_TERMS},
        ),
        _check(
            "generation-prompts-indirect",
            "persisted generation prompts avoid final Nexus kit target terms",
            not leaks,
            {"prompt_count": len(generation_prompts), "leaks": leaks[:20]},
        ),
        _check(
            "first-stage-stance",
            "first-stage prompt stance keeps expansion broad before later filtering",
            expansion_report.get("policy") == "expand aggressively, reject only obvious waste"
            and expansion_report.get("prompt_stance", {}).get("rule") == "Generate unusual but relevant new items."
            and expansion_report.get("later_stages") == ["filter", "merge", "reduce", "map"],
            {
                "policy": expansion_report.get("policy"),
                "prompt_stance": expansion_report.get("prompt_stance"),
                "later_stages": expansion_report.get("later_stages"),
            },
        ),
        _check(
            "router-controls",
            "router and manifest enforce 100 context tokens and 128 active predictions",
            manifest.get("max_context_tokens", 999) <= 100
            and manifest.get("max_predictions") == 128
            and router_stats.get("max_context_tokens", 999) <= 100
            and router_stats.get("max_predictions") == 128,
            {
                "manifest_max_context_tokens": manifest.get("max_context_tokens"),
                "manifest_max_predictions": manifest.get("max_predictions"),
                "router_max_context_tokens": router_stats.get("max_context_tokens"),
                "router_max_predictions": router_stats.get("max_predictions"),
            },
        ),
        _check(
            "workflow-concurrency",
            "workflow concurrency is fixed at 128",
            manifest.get("concurrency") == 128,
            {"concurrency": manifest.get("concurrency")},
        ),
        _check(
            "temperature-schedule",
            "temperature schedule scales from high expansion to low gates",
            _temperature_schedule_ok(schedule),
            {"temperature_schedule": schedule},
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "prompt-control-indirection",
        "prompt_count": len(generation_prompts),
        "forbidden_terms": FORBIDDEN_GENERATION_TERMS,
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _expansion_prompts(path: Path) -> List[str]:
    prompts: List[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            for key in ["prompt", "relevance_prompt"]:
                prompt = str(record.get(key, "")).strip()
                if prompt:
                    prompts.append(prompt)
    return prompts


def _contains_forbidden(value: str) -> bool:
    text = value.lower()
    return any(term in text for term in FORBIDDEN_GENERATION_TERMS)


def _forbidden_hits(prompts: List[str]) -> List[Dict[str, Any]]:
    hits = []
    for index, prompt in enumerate(prompts):
        text = prompt.lower()
        matched = [term for term in FORBIDDEN_GENERATION_TERMS if term in text]
        if matched:
            hits.append({"index": index, "terms": matched, "preview": prompt[:180]})
    return hits


def _temperature_schedule_ok(schedule: Dict[str, Any]) -> bool:
    return (
        schedule.get("expand", 0) > schedule.get("reveal", 999)
        and schedule.get("reveal", 0) > schedule.get("reduce", 999)
        and schedule.get("relevance_check", 999) <= schedule.get("reduce", 999)
        and schedule.get("domain_merge_review", 999) <= schedule.get("reduce", 999)
        and schedule.get("kit_merge_review", 999) <= schedule.get("reduce", 999)
        and schedule.get("slot_decision", 999) <= schedule.get("reduce", 999)
    )


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
