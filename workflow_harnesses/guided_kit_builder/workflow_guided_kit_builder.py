from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import sys
import time
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from kituniverse_harness.providers import LMStudioProvider
from kituniverse_harness.smart_router import SmartRoutingService
from workflow_harnesses.fractal_kit_pipeline.run_artifacts import write_json, write_jsonl
from workflow_harnesses.guided_kit_builder.benchmark_cases import BENCHMARK_CASES
from workflow_harnesses.guided_kit_builder.codex_cli_review import run_codex_cli_review
from workflow_harnesses.guided_kit_builder.contract import (
    RESPONSE_FORMAT,
    apply_slot_status,
    build_guided_kit,
    compare_and_inject,
    parse_json_object,
    score_benchmark_case,
    validate_guided_kit,
)
from workflow_harnesses.guided_kit_builder.post_codex_validation import run_post_codex_validation


DEFAULT_BASE_URL = "http://10.0.0.137:1234/v1"
DEFAULT_MODEL = "lfm2.5-350m-heretic-high-reasoning-i1"
DEFAULT_RUN_ROOT = Path("runs/workflow-harnesses/guided-kit-builder")
DEFAULT_SYSTEM = (
    "Fill semantic slots for one atomic reusable domain kit. "
    "Use only source facts and copy namespaced tokens exactly."
)


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--universe-turn", action="store_true")
    parser.add_argument("--universe-root", default="runs/kit-universe-1000")
    parser.add_argument("--universe-target", type=int, default=1000)
    parser.add_argument("--skip-codex-review", action="store_true")
    parser.add_argument("--idea", help="One rough idea to turn into one complete kit")
    parser.add_argument("--title")
    parser.add_argument("--description")
    parser.add_argument("--domain-hint", default="")
    parser.add_argument("--requires", default="")
    parser.add_argument("--provides", default="")
    parser.add_argument("--owned-state", default="")
    parser.add_argument("--inputs", default="")
    parser.add_argument("--outputs", default="")
    parser.add_argument("--idempotency-key", default="")
    parser.add_argument("--context-length", type=int, default=8192)
    parser.add_argument("--max-context-tokens", type=int, default=1024)
    parser.add_argument("--max-tokens", type=int, default=700)
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--provider-retries", type=int, default=1)
    parser.add_argument("--repair-passes", type=int, default=2)
    parser.add_argument("--repair-delay-seconds", type=float, default=0.5)
    parser.add_argument("--max-repair-delay-seconds", type=float, default=4.0)
    parser.add_argument("--task-concurrency", type=int, default=10)
    parser.add_argument("--max-predictions", type=int, default=1)
    parser.add_argument("--min-case-coverage", type=float, default=0.72)
    parser.add_argument("--min-average-coverage", type=float, default=0.82)
    parser.add_argument("--run-root", default=str(DEFAULT_RUN_ROOT))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-guided-kit-builder",
        description="Turn full kit descriptions into comparable slot-filled kit records.",
    )
    configure_parser(parser)
    return run_from_namespace(parser.parse_args(argv))


def run_from_namespace(args: argparse.Namespace) -> int:
    report = asyncio.run(
        run_guided_kit_builder(
            base_url=args.base_url,
            model=args.model,
            benchmark=bool(args.benchmark),
            universe_turn=bool(args.universe_turn),
            universe_root=Path(args.universe_root),
            universe_target=args.universe_target,
            codex_review=not args.skip_codex_review,
            idea=args.idea,
            title=args.title,
            description=args.description,
            domain_hint=args.domain_hint,
            requires_hint=_csv(args.requires),
            provides_hint=_csv(args.provides),
            owned_state_hint=_csv(args.owned_state),
            inputs_hint=_csv(args.inputs),
            outputs_hint=_csv(args.outputs),
            idempotency_hint=args.idempotency_key,
            context_length=args.context_length,
            max_context_tokens=args.max_context_tokens,
            max_tokens=args.max_tokens,
            temperature=args.temperature,
            timeout_seconds=args.timeout_seconds,
            provider_retries=args.provider_retries,
            repair_passes=args.repair_passes,
            repair_delay_seconds=args.repair_delay_seconds,
            max_repair_delay_seconds=args.max_repair_delay_seconds,
            task_concurrency=args.task_concurrency,
            max_predictions=args.max_predictions,
            min_case_coverage=args.min_case_coverage,
            min_average_coverage=args.min_average_coverage,
            run_root=Path(args.run_root),
        )
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


async def run_guided_kit_builder(
    base_url: str,
    model: str,
    benchmark: bool,
    universe_turn: bool,
    universe_root: Path,
    universe_target: int,
    codex_review: bool,
    idea: Optional[str],
    title: Optional[str],
    description: Optional[str],
    domain_hint: str,
    requires_hint: List[str],
    provides_hint: List[str],
    owned_state_hint: List[str],
    inputs_hint: List[str],
    outputs_hint: List[str],
    idempotency_hint: str,
    context_length: int,
    max_context_tokens: int,
    max_tokens: int,
    temperature: float,
    timeout_seconds: int,
    provider_retries: int,
    repair_passes: int,
    repair_delay_seconds: float,
    max_repair_delay_seconds: float,
    task_concurrency: int,
    max_predictions: int,
    min_case_coverage: float,
    min_average_coverage: float,
    run_root: Path,
) -> Dict[str, Any]:
    _validate_controls(
        benchmark=benchmark,
        universe_turn=universe_turn,
        idea=idea,
        title=title,
        description=description,
        context_length=context_length,
        max_context_tokens=max_context_tokens,
        max_tokens=max_tokens,
        repair_passes=repair_passes,
        repair_delay_seconds=repair_delay_seconds,
        max_repair_delay_seconds=max_repair_delay_seconds,
        task_concurrency=task_concurrency,
        max_predictions=max_predictions,
        universe_target=universe_target,
    )
    run_id = _run_id()
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    resolved_title = title or _title_from_idea(idea or "")
    resolved_description = description or idea
    if benchmark:
        cases = []
        for benchmark_case in BENCHMARK_CASES:
            case = deepcopy(benchmark_case)
            expected = case.get("expected") or {}
            case["slot_hints"] = {
                "owned_state": expected.get("owned_state", []),
                "inputs": expected.get("inputs", []),
                "idempotency_key": "-".join(expected.get("idempotency_terms", [])),
            }
            cases.append(case)
    else:
        cases = [
            {
                "idea_id": _slug(resolved_title),
                "title": resolved_title,
                "description": resolved_description,
                "domain_hint": domain_hint,
                "requires_hint": requires_hint,
                "provides_hint": provides_hint,
                "slot_hints": {
                    "owned_state": owned_state_hint,
                    "inputs": inputs_hint,
                    "outputs": outputs_hint,
                    "idempotency_key": idempotency_hint,
                },
                "build_plan": [],
                "expected": {},
            }
        ]
    provider = LMStudioProvider(base_url=base_url, model=model, timeout_seconds=timeout_seconds)
    load_result = provider.ensure_loaded(context_length=context_length)
    health = provider.health()
    manifest = {
        "run_id": run_id,
        "workflow_harness": "GuidedKitBuilder",
        "base_url": base_url,
        "model": model,
        "benchmark": benchmark,
        "universe_turn": universe_turn,
        "universe_root": str(universe_root) if universe_turn else None,
        "universe_target": universe_target if universe_turn else None,
        "codex_review": codex_review if universe_turn else False,
        "case_count": len(cases),
        "context_length": context_length,
        "max_context_tokens": max_context_tokens,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "provider_retries": provider_retries,
        "repair_passes": repair_passes,
        "repair_delay_seconds": repair_delay_seconds,
        "max_repair_delay_seconds": max_repair_delay_seconds,
        "task_concurrency": task_concurrency,
        "max_predictions": max_predictions,
        "min_case_coverage": min_case_coverage,
        "min_average_coverage": min_average_coverage,
        "stage_graph": [
            "description-intake",
            "structured-slot-draft",
            "deterministic-normalization",
            "slot-validation-repair-loop",
            "kit-comparison",
            "link-injection",
            "final-output",
        ],
    }
    write_json(run_dir / "manifest.json", manifest)
    write_json(run_dir / "model-load.json", load_result)
    write_json(run_dir / "provider-health.json", health)
    write_json(run_dir / "intake-cases.json", {"cases": cases})
    if not load_result.get("ok") or not health.get("ok"):
        report = {
            "ok": False,
            "workflow_harness": "GuidedKitBuilder",
            "run_id": run_id,
            "run_dir": str(run_dir),
            "error": "model ensure/load or health check failed",
            "model_load": load_result,
            "provider_health": health,
        }
        write_json(run_dir / "report.json", report)
        return report

    router = SmartRoutingService(
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
        max_predictions=max_predictions,
        max_context_tokens=max_context_tokens,
    )
    task_semaphore = asyncio.Semaphore(task_concurrency)
    started = time.time()

    async def run_case(case: Dict[str, Any]) -> Dict[str, Any]:
        async with task_semaphore:
            return await _run_case_chain(
                router=router,
                case=case,
                max_tokens=max_tokens,
                temperature=temperature,
                provider_retries=provider_retries,
                repair_passes=repair_passes,
                repair_delay_seconds=repair_delay_seconds,
                max_repair_delay_seconds=max_repair_delay_seconds,
                min_case_coverage=min_case_coverage,
            )

    try:
        results = await asyncio.gather(*(run_case(case) for case in cases))
        router_stats = router.stats()
    finally:
        router.shutdown()

    accepted_records = [result["record"] for result in results if result.get("accepted")]
    draft_records = [
        {
            "idea_id": result["idea_id"],
            "best_attempt": result.get("best_attempt"),
            "parsed_draft": result.get("parsed_draft"),
        }
        for result in results
    ]
    comparisons, injections = compare_and_inject(accepted_records)
    attempt_records = [
        {"idea_id": result["idea_id"], **attempt}
        for result in results
        for attempt in result.get("attempts", [])
    ]
    slot_source_counts: Counter[str] = Counter(
        slot.get("source", "unknown")
        for record in accepted_records
        for slot in record.get("payload", {}).get("slots", {}).values()
    )
    usage_records = [attempt.get("usage") or {} for attempt in attempt_records]
    usage_summary = {
        "prompt_tokens": sum(int(usage.get("prompt_tokens", 0)) for usage in usage_records),
        "completion_tokens": sum(int(usage.get("completion_tokens", 0)) for usage in usage_records),
        "total_tokens": sum(int(usage.get("total_tokens", 0)) for usage in usage_records),
        "max_prompt_tokens": max((int(usage.get("prompt_tokens", 0)) for usage in usage_records), default=0),
        "max_completion_tokens": max(
            (int(usage.get("completion_tokens", 0)) for usage in usage_records),
            default=0,
        ),
    }
    total_semantic_slots = sum(slot_source_counts.values())
    model_draft_slots = slot_source_counts.get("model-draft", 0)
    model_semantic_contribution = {
        "model_draft_slots": model_draft_slots,
        "total_slots": total_semantic_slots,
        "ratio": round(model_draft_slots / total_semantic_slots, 4)
        if total_semantic_slots
        else 0.0,
    }
    write_jsonl(run_dir / "chain-ledger.jsonl", attempt_records)
    write_jsonl(run_dir / "kit-drafts.jsonl", draft_records)
    write_jsonl(run_dir / "final-kits.jsonl", accepted_records)
    write_json(run_dir / "comparisons.json", {"comparisons": comparisons})
    write_jsonl(run_dir / "link-injections.jsonl", injections)
    write_json(
        run_dir / "outputs.json",
        {
            "record_ids": [record["record_id"] for record in accepted_records],
            "final_kits_jsonl": str(run_dir / "final-kits.jsonl"),
            "link_injections_jsonl": str(run_dir / "link-injections.jsonl"),
        },
    )

    coverages = [result["benchmark_score"]["coverage"] for result in results]
    average_coverage = sum(coverages) / len(coverages) if coverages else 0.0
    min_coverage = min(coverages) if coverages else 0.0
    validation_failures = [result["idea_id"] for result in results if not result["validation"].get("ok")]
    rejected_cases = [result["idea_id"] for result in results if not result.get("accepted")]
    final_lines, final_malformed = _count_jsonl(run_dir / "final-kits.jsonl")
    injection_lines, injection_malformed = _count_jsonl(run_dir / "link-injections.jsonl")
    benchmark_ok = (
        len(accepted_records) == len(cases)
        and not validation_failures
        and not rejected_cases
        and average_coverage >= min_average_coverage
        and min_coverage >= min_case_coverage
        and (not benchmark or len(injections) >= 7)
    )
    concurrency_ok = (
        router_stats.get("max_predictions") == 1
        and router_stats.get("peak_active_predictions") == 1
        and router_stats.get("active_predictions") == 0
    )
    report = {
        "ok": benchmark_ok and concurrency_ok and final_malformed == 0 and injection_malformed == 0,
        "workflow_harness": "GuidedKitBuilder",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "elapsed_seconds": round(time.time() - started, 3),
        "model": model,
        "model_load": load_result,
        "cases_requested": len(cases),
        "cases_accepted": len(accepted_records),
        "rejected_cases": rejected_cases,
        "validation_failures": validation_failures,
        "average_benchmark_coverage": round(average_coverage, 4),
        "minimum_benchmark_coverage": round(min_coverage, 4),
        "case_results": [
            {
                "idea_id": result["idea_id"],
                "accepted": result["accepted"],
                "attempt_count": len(result.get("attempts", [])),
                "coverage": result["benchmark_score"]["coverage"],
                "validation_ok": result["validation"]["ok"],
                "validation_errors": result["validation"]["errors"],
                "record_id": result.get("record", {}).get("record_id"),
            }
            for result in results
        ],
        "comparison_count": len(comparisons),
        "link_injection_count": len(injections),
        "model_calls": len(attempt_records),
        "repair_calls": max(0, len(attempt_records) - len(cases)),
        "repair_delay_seconds_total": round(
            sum(float(attempt.get("repair_delay_seconds", 0.0)) for attempt in attempt_records),
            3,
        ),
        "first_pass_accepted": sum(1 for result in results if len(result.get("attempts", [])) == 1),
        "slot_source_counts": dict(sorted(slot_source_counts.items())),
        "model_semantic_contribution": model_semantic_contribution,
        "usage": usage_summary,
        "router": router_stats,
        "concurrency_gate": concurrency_ok,
        "final_kits_jsonl": str(run_dir / "final-kits.jsonl"),
        "final_kits_lines": final_lines,
        "final_kits_malformed": final_malformed,
        "link_injections_jsonl": str(run_dir / "link-injections.jsonl"),
        "link_injections_lines": injection_lines,
        "link_injections_malformed": injection_malformed,
        "acceptance": {
            "all_cases_accepted": len(accepted_records) == len(cases),
            "all_slots_valid": not validation_failures,
            "average_coverage_required": min_average_coverage,
            "minimum_coverage_required": min_case_coverage,
            "seven_or_more_links_required_for_benchmark": benchmark,
            "one_active_prediction_required": True,
        },
    }
    if universe_turn:
        generation_ok = bool(report["ok"])
        report["generation_ok"] = generation_ok
        report["ok"] = False
        report["turn_status"] = "awaiting-codex-review"
        report["universe"] = {
            "ok": False,
            "committed": False,
            "reason": "awaiting-codex-review",
        }
        write_json(run_dir / "report.json", report)
        if codex_review:
            report["codex_cli_review"] = run_codex_cli_review(
                repo_root=REPO_ROOT,
                run_dir=run_dir,
            )
        if generation_ok and len(accepted_records) == 1:
            report["post_codex_validation"] = run_post_codex_validation(
                repo_root=REPO_ROOT,
                final_kits_path=run_dir / "final-kits.jsonl",
            )
            write_json(
                run_dir / "post-codex-validation.json",
                report["post_codex_validation"],
            )
        review_ok = (
            (not codex_review or bool(report.get("codex_cli_review", {}).get("ok")))
            and bool(report.get("post_codex_validation", {}).get("ok"))
        )
        if generation_ok and len(accepted_records) == 1:
            if review_ok:
                universe_ledger = importlib.import_module(
                    "workflow_harnesses.guided_kit_builder.universe_ledger"
                )
                universe_ledger = importlib.reload(universe_ledger)
                report["universe"] = universe_ledger.commit_universe_turn(
                    universe_root=universe_root,
                    record=accepted_records[0],
                    run_report=report,
                    target=universe_target,
                )
            else:
                report["universe"] = {
                    "ok": False,
                    "committed": False,
                    "reason": (
                        "post-codex-validation-failed"
                        if report.get("codex_cli_review", {}).get("ok")
                        else "codex-cli-review-failed"
                    ),
                }
            report["ok"] = bool(report["universe"].get("ok"))
        else:
            report["universe"] = {
                "ok": False,
                "committed": False,
                "reason": "turn-did-not-produce-exactly-one-accepted-kit",
            }
        report["turn_status"] = (
            "committed" if report["universe"].get("committed") else "rejected"
        )
    write_json(run_dir / "benchmark-report.json", report)
    write_json(run_dir / "report.json", report)
    _write_markdown_report(run_dir / "report.md", report)
    return report


async def _run_case_chain(
    router: SmartRoutingService,
    case: Dict[str, Any],
    max_tokens: int,
    temperature: float,
    provider_retries: int,
    repair_passes: int,
    repair_delay_seconds: float,
    max_repair_delay_seconds: float,
    min_case_coverage: float,
) -> Dict[str, Any]:
    attempts = []
    best: Optional[Dict[str, Any]] = None
    previous_raw = ""
    previous_errors: List[str] = []
    for attempt_index in range(1, repair_passes + 2):
        delay_seconds = 0.0
        if attempt_index > 1:
            delay_seconds = min(
                repair_delay_seconds * (2 ** (attempt_index - 2)),
                max_repair_delay_seconds,
            )
            await asyncio.sleep(delay_seconds)
        prompt = (
            _draft_prompt(case)
            if attempt_index == 1
            else _repair_prompt(case, previous_raw, previous_errors)
        )
        response, provider_attempts = await router.chat(
            [
                {"role": "system", "content": DEFAULT_SYSTEM},
                {"role": "user", "content": prompt},
            ],
            temperature=temperature if attempt_index == 1 else 0.0,
            max_tokens=max_tokens,
            retries=provider_retries,
            response_format=RESPONSE_FORMAT,
        )
        parsed, parse_error = parse_json_object(response.content)
        record = build_guided_kit(
            case=case,
            draft=parsed or {},
            raw_output=response.content,
            model_attempt=attempt_index,
        )
        validation = validate_guided_kit(record)
        score = score_benchmark_case(case, record)
        record = apply_slot_status(record, validation)
        attempt = {
            "attempt": attempt_index,
            "repair_delay_seconds": delay_seconds,
            "prompt": prompt,
            "raw_output": response.content,
            "provider_ok": response.ok,
            "provider_error": response.error,
            "provider_attempts": provider_attempts,
            "usage": response.usage,
            "parse_error": parse_error,
            "validation": validation,
            "benchmark_score": score,
            "record_id": record.get("record_id"),
        }
        attempts.append(attempt)
        candidate = {
            "attempt": attempt_index,
            "parsed": parsed or {},
            "record": record,
            "validation": validation,
            "score": score,
            "raw": response.content,
        }
        if best is None or _candidate_rank(candidate) > _candidate_rank(best):
            best = candidate
        if response.ok and parse_error is None and validation["ok"] and score["coverage"] >= min_case_coverage:
            break
        previous_raw = response.content
        previous_errors = [
            *(validation.get("errors") or []),
            *[
                f"benchmark-miss-{check['name']}"
                for check in score.get("checks", [])
                if check.get("score", 0.0) < 1.0
            ],
        ]
    if best is None:
        raise RuntimeError("guided kit chain completed without a candidate")
    accepted = bool(best["validation"]["ok"] and best["score"]["coverage"] >= min_case_coverage)
    return {
        "idea_id": case["idea_id"],
        "accepted": accepted,
        "attempts": attempts,
        "best_attempt": best["attempt"],
        "parsed_draft": best["parsed"],
        "record": best["record"],
        "validation": best["validation"],
        "benchmark_score": best["score"],
    }


def _candidate_rank(candidate: Dict[str, Any]) -> Tuple[int, float, int]:
    return (
        1 if candidate["validation"].get("ok") else 0,
        float(candidate["score"].get("coverage", 0.0)),
        -int(candidate["attempt"]),
    )


def _draft_prompt(case: Dict[str, Any]) -> str:
    guided_lines = [f"TITLE: {case.get('title', '')}"]
    optional_slots = [
        ("DOMAIN", case.get("domain_hint", "")),
        ("REQUIRES", ", ".join(case.get("requires_hint", []))),
        ("PROVIDES", ", ".join(case.get("provides_hint", []))),
        ("OWNED STATE", ", ".join((case.get("slot_hints") or {}).get("owned_state", []))),
        ("INPUTS", ", ".join((case.get("slot_hints") or {}).get("inputs", []))),
        ("OUTPUTS", ", ".join((case.get("slot_hints") or {}).get("outputs", []))),
        ("IDEMPOTENCY KEY", (case.get("slot_hints") or {}).get("idempotency_key", "")),
        ("BUILD GUIDANCE", "; ".join(case.get("build_plan", []))),
    ]
    guided_lines.extend(f"{label}: {value}" for label, value in optional_slots if value)
    guided = "\n".join(guided_lines)
    return (
        f"{guided}\n"
        f"DESCRIPTION: {case.get('description', '')}\n"
        "TASK: return one reusable behavior contract grounded only in TITLE and DESCRIPTION.\n"
        "DOMAIN RULE: choose one to three specific subject words from TITLE; omit generic words such as "
        "request gate kit service behavior capability domain and description.\n"
        "RULES: copy every explicit intake fact exactly; keep state inputs outputs and snapshot fields "
        "specific and never use generic labels such as data inputs outputs state or snapshots; reset_behavior "
        "must say exactly which owned state is cleared or restored; purpose must state the concrete decision "
        "or state transition from DESCRIPTION rather than summarize the title or slot names and "
        "never mention the prompt or schema; idempotency_key "
        "must identify the replay key; return at least three concrete implementation steps; never add renderer ownership."
    )


def _repair_prompt(case: Dict[str, Any], previous_raw: str, errors: List[str]) -> str:
    reset_guidance = (
        "\nRESET REPAIR: reset_behavior must be a sentence that names the exact owned state "
        "and says whether that state is cleared or restored. Do not return a single verb."
        if "generic-reset-behavior" in errors
        else ""
    )
    contribution_guidance = (
        "\nMODEL CONTRIBUTION REPAIR: do not copy the prior draft. Write purpose as one normal "
        "sentence of six to fourteen spaced words describing the behavior this kit owns. Also "
        "replace a generic domain with one to three specific subject words from TITLE."
        if "no-model-draft-contribution" in errors
        else ""
    )
    prior_draft = (
        "PRIOR DRAFT OMITTED: it had no surviving semantic contribution. Regenerate from TITLE "
        "and DESCRIPTION only."
        if "no-model-draft-contribution" in errors
        else f"PRIOR DRAFT: {previous_raw[:1600]}"
    )
    return (
        f"REPAIR ONE KIT.\n{_draft_prompt(case)}\n"
        f"FAILED CHECKS: {', '.join(errors) or 'invalid prior draft'}"
        f"{reset_guidance}{contribution_guidance}\n"
        f"{prior_draft}\n"
        "Return a complete replacement. Re-read DESCRIPTION for missed state, input, and idempotency facts."
    )


def _validate_controls(
    benchmark: bool,
    universe_turn: bool,
    idea: Optional[str],
    title: Optional[str],
    description: Optional[str],
    context_length: int,
    max_context_tokens: int,
    max_tokens: int,
    repair_passes: int,
    repair_delay_seconds: float,
    max_repair_delay_seconds: float,
    task_concurrency: int,
    max_predictions: int,
    universe_target: int,
) -> None:
    if benchmark and universe_turn:
        raise ValueError("--universe-turn accepts exactly one kit and cannot be combined with --benchmark")
    if not benchmark and not idea and (not title or not description):
        raise ValueError("provide --idea, --benchmark, or both --title and --description")
    if max_predictions != 1:
        raise ValueError("GuidedKitBuilder requires --max-predictions 1")
    if context_length < 1024:
        raise ValueError("--context-length must be at least 1024")
    if max_context_tokens < 128 or max_context_tokens > context_length:
        raise ValueError("--max-context-tokens must be between 128 and --context-length")
    if max_tokens < 128:
        raise ValueError("--max-tokens must be at least 128")
    if repair_passes < 0 or repair_passes > 4:
        raise ValueError("--repair-passes must be between 0 and 4")
    if repair_delay_seconds < 0:
        raise ValueError("--repair-delay-seconds must be non-negative")
    if max_repair_delay_seconds < repair_delay_seconds:
        raise ValueError("--max-repair-delay-seconds must be at least --repair-delay-seconds")
    if task_concurrency < 1:
        raise ValueError("--task-concurrency must be at least 1")
    if universe_turn and universe_target < 1:
        raise ValueError("--universe-target must be at least 1")


def _csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _run_id() -> str:
    return f"{time.strftime('%Y%m%d-%H%M%S')}-{int((time.time() % 1) * 1000):03d}"


def _slug(value: str) -> str:
    return "-".join(part for part in "".join(character.lower() if character.isalnum() else " " for character in value).split())


def _title_from_idea(idea: str) -> str:
    words = [word.strip(".,:;!?()[]{}") for word in idea.split() if word.strip(".,:;!?()[]{}")]
    if not words:
        return "Guided Kit"
    ignored = {"a", "an", "the", "kit", "that", "which", "for", "to", "of"}
    useful = [word for word in words if word.lower() not in ignored]
    selected = (useful or words)[:5]
    return " ".join(word.capitalize() for word in selected)


def _count_jsonl(path: Path) -> Tuple[int, int]:
    lines = 0
    malformed = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            lines += 1
            try:
                json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
    return lines, malformed


def _write_markdown_report(path: Path, report: Dict[str, Any]) -> None:
    lines = [
        "# Guided Kit Builder Report",
        "",
        f"- ok: `{str(report['ok']).lower()}`",
        f"- model: `{report['model']}`",
        f"- cases accepted: `{report['cases_accepted']}/{report['cases_requested']}`",
        f"- average benchmark coverage: `{report['average_benchmark_coverage']}`",
        f"- minimum benchmark coverage: `{report['minimum_benchmark_coverage']}`",
        f"- link injections: `{report['link_injection_count']}`",
        f"- max predictions: `{report['router']['max_predictions']}`",
        f"- peak active predictions: `{report['router']['peak_active_predictions']}`",
        f"- final kits: `{report['final_kits_jsonl']}`",
        f"- link injections: `{report['link_injections_jsonl']}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
