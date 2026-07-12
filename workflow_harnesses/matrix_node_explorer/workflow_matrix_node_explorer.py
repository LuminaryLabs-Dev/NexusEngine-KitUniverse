from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from kituniverse_harness.ingestion import ShardedJsonlIngestionService
from kituniverse_harness.providers import LMStudioProvider
from kituniverse_harness.smart_router import SmartRoutingService


DEFAULT_BASE_URL = "http://10.0.0.137:1234/v1"
DEFAULT_MODEL = "lfm2.5-350m-heretic-high-reasoning-i1"
DEFAULT_MATRIX_SOURCE_TYPE = "game"
DEFAULT_MATRIX_SOURCE = (
    "party board game, dice movement, coin economy"
)
DEFAULT_SEEDS = "dice-roll, board-navigation, coin-collection"
DEFAULT_LIST_GOAL = "Game Ideas"
DEFAULT_SYSTEM = "Reply with only the requested token or short comma item."
FIRST_STAGE_POLICY = "expand aggressively, reject only obvious waste"
FIRST_STAGE_REJECT_ONLY = [
    "empty output",
    "exact duplicate",
    "pure formatting junk",
    "record-unsafe text",
    "Y/N relevance failure",
]
FIRST_STAGE_KEEP_IF = [
    "non-empty",
    "not exact duplicate",
    "passes Y/N relevance check",
    "loosely connected to LIST GOAL",
    "record-safe bounded text",
]
FIRST_STAGE_DO_NOT_REJECT_FOR = [
    "awkward wording",
    "near-duplicate meaning",
    "weak grammar",
    "rough labels",
    "odd but connected ideas",
]
FIRST_STAGE_PROMPT_STANCE = {
    "rule": "Generate unusual but relevant new items.",
    "avoid": "exact repeats only",
    "allow": "rough names, strange angles, partial concepts",
    "do_not_polish": "preserve awkward, partial, or weird connected ideas",
    "return": "comma-separated list",
}
EXPLORATION_AXES = [
    "player-action",
    "board-space",
    "resource",
    "reward",
    "hazard",
    "minigame",
    "turn-rule",
    "item-effect",
    "score-state",
    "feedback",
    "multiplayer",
    "win-condition",
]
PLACEHOLDER_PARTS = {
    "blue",
    "code",
    "dash",
    "format",
    "item",
    "lower",
    "name",
    "node",
    "one",
    "rough",
    "roughly",
    "roughwording",
    "roughwordy",
    "sep",
    "three",
    "two",
}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-matrix-node-explorer",
        description="Explore matrix nodes with async short-output generation and Y/N game checks.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--matrix-source-type", default=DEFAULT_MATRIX_SOURCE_TYPE)
    parser.add_argument("--matrix-source", default=DEFAULT_MATRIX_SOURCE)
    parser.add_argument("--list-goal", default=DEFAULT_LIST_GOAL)
    parser.add_argument("--seeds", default=DEFAULT_SEEDS)
    parser.add_argument("--nodes", type=int, default=256)
    parser.add_argument("--concurrency", type=int, default=256)
    parser.add_argument("--shards", type=int, default=256)
    parser.add_argument("--chain-steps", type=int, default=4)
    parser.add_argument("--min-chain-steps", type=int)
    parser.add_argument("--max-chain-steps", type=int)
    parser.add_argument("--items-per-step", type=int, default=3)
    parser.add_argument("--temperature", type=float, default=1.2)
    parser.add_argument("--max-tokens", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--provider-retries", type=int, default=2)
    parser.add_argument("--max-predictions", type=int, default=128)
    parser.add_argument("--max-context-tokens", type=int, default=100)
    parser.add_argument("--variation-window", type=int, default=80)
    parser.add_argument("--run-root", default="runs/workflow-harnesses/matrix-node-explorer")
    parser.add_argument("--ingestion-root", default="runs/matrix-node-explorer-ingestion")
    args = parser.parse_args(argv)

    report = asyncio.run(
        run_matrix_node_explorer(
            base_url=args.base_url,
            model=args.model,
            matrix_source_type=args.matrix_source_type,
            matrix_source=args.matrix_source,
            list_goal=args.list_goal,
            seeds=_parse_csv(args.seeds),
            nodes=args.nodes,
            concurrency=args.concurrency,
            shards=args.shards,
            chain_steps=args.chain_steps,
            min_chain_steps=args.min_chain_steps,
            max_chain_steps=args.max_chain_steps,
            items_per_step=args.items_per_step,
            temperature=args.temperature,
            max_tokens=args.max_tokens,
            timeout_seconds=args.timeout_seconds,
            provider_retries=args.provider_retries,
            max_predictions=args.max_predictions,
            max_context_tokens=args.max_context_tokens,
            variation_window=args.variation_window,
            run_root=Path(args.run_root),
            ingestion_root=Path(args.ingestion_root),
        )
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


async def run_matrix_node_explorer(
    base_url: str,
    model: str,
    matrix_source_type: str,
    matrix_source: str,
    list_goal: str,
    seeds: List[str],
    nodes: int,
    concurrency: int,
    shards: int,
    chain_steps: int,
    min_chain_steps: Optional[int],
    max_chain_steps: Optional[int],
    items_per_step: int,
    temperature: float,
    max_tokens: int,
    timeout_seconds: int,
    provider_retries: int,
    max_predictions: int,
    max_context_tokens: int,
    variation_window: int,
    run_root: Path,
    ingestion_root: Path,
) -> Dict[str, Any]:
    if nodes < 1:
        raise ValueError("--nodes must be at least 1")
    if concurrency < 1:
        raise ValueError("--concurrency must be at least 1")
    if len(seeds) < 3:
        raise ValueError("--seeds must include at least 3 comma-separated items")
    if chain_steps < 1:
        raise ValueError("--chain-steps must be at least 1")
    chain_step_min = min_chain_steps if min_chain_steps is not None else chain_steps
    chain_step_max = max_chain_steps if max_chain_steps is not None else chain_steps
    if chain_step_min < 1 or chain_step_max < chain_step_min:
        raise ValueError("--max-chain-steps must be >= --min-chain-steps >= 1")
    if items_per_step < 1:
        raise ValueError("--items-per-step must be at least 1")

    run_id = _run_id()
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    ingestion = ShardedJsonlIngestionService.create(
        root=ingestion_root,
        run_id=run_id,
        shard_count=shards,
    )
    provider = LMStudioProvider(
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
    )
    health = provider.health()
    _write_json(run_dir / "provider-health.json", health)
    _write_json(
        run_dir / "manifest.json",
        {
            "run_id": run_id,
            "workflow_harness": "MatrixNodeExplorer-Harness",
            "base_url": base_url,
            "model": model,
            "matrix_source_type": matrix_source_type,
            "matrix_source": matrix_source,
            "list_goal": list_goal,
            "seeds": seeds,
            "nodes": nodes,
            "concurrency": concurrency,
            "shards": shards,
            "chain_steps": chain_steps,
            "chain_step_min": chain_step_min,
            "chain_step_max": chain_step_max,
            "items_per_step": items_per_step,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "max_predictions": max_predictions,
            "max_context_tokens": max_context_tokens,
            "variation_window": variation_window,
            "first_stage_policy": FIRST_STAGE_POLICY,
            "first_stage_prompt_stance": FIRST_STAGE_PROMPT_STANCE,
            "first_stage_reject_only": FIRST_STAGE_REJECT_ONLY,
            "first_stage_keep_if": FIRST_STAGE_KEEP_IF,
            "first_stage_do_not_reject_for": FIRST_STAGE_DO_NOT_REJECT_FOR,
            "later_stages": ["filter", "merge", "reduce", "map"],
            "ingestion_run_dir": str(ingestion.run_dir),
        },
    )
    if not health.get("ok"):
        report = _report(
            ok=False,
            run_id=run_id,
            run_dir=run_dir,
            ingestion=ingestion,
            nodes_requested=nodes,
            results=[],
            error="provider health failed",
        )
        _write_json(run_dir / "report.json", report)
        return report

    accepted_memory: List[str] = list(seeds)
    seen_keys = {_key(seed) for seed in seeds}
    memory_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(concurrency)
    router = SmartRoutingService(
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
        max_predictions=max_predictions,
        max_context_tokens=max_context_tokens,
    )
    started = time.time()

    async def run_one(index: int) -> Dict[str, Any]:
        async with semaphore:
            async with memory_lock:
                matrix_items = _matrix_items_for(index, accepted_memory)
                recent_outputs = accepted_memory[-variation_window:]
            gen_started = time.time()
            chain_records: List[Dict[str, Any]] = []
            instance_outputs: List[str] = []
            last_output = matrix_items[-1]
            step_span = chain_step_max - chain_step_min + 1
            instance_chain_steps = chain_step_min + (index % step_span)
            for step in range(1, instance_chain_steps + 1):
                async with memory_lock:
                    recent_outputs = accepted_memory[-variation_window:]
                chain_items = [matrix_items[0], matrix_items[1], last_output]
                gen_prompt = _generation_prompt(
                    matrix_source_type=matrix_source_type,
                    matrix_source=matrix_source,
                    list_goal=list_goal,
                    matrix_items=chain_items,
                    recent_outputs=recent_outputs,
                    axis=EXPLORATION_AXES[(index + step - 1) % len(EXPLORATION_AXES)],
                    index=index,
                    step=step,
                    items_per_step=items_per_step,
                    max_tokens=max_tokens,
                )
                gen_response, gen_attempts = await router.chat(
                    messages=[
                        {"role": "system", "content": DEFAULT_SYSTEM},
                        {"role": "user", "content": gen_prompt},
                    ],
                    temperature=temperature,
                    max_tokens=max(max_tokens * items_per_step + items_per_step, max_tokens),
                    retries=provider_retries,
                )
                candidates = _clean_candidates(gen_response.content, limit=items_per_step)
                candidate_records = []
                step_accepted = []
                for candidate in candidates:
                    convert_prompt = _convert_prompt(
                        list_goal=list_goal,
                        matrix_source_type=matrix_source_type,
                        matrix_source=matrix_source,
                        candidate=candidate,
                        max_tokens=max_tokens,
                    )
                    convert_response, convert_attempts = await router.chat(
                        messages=[
                            {"role": "system", "content": DEFAULT_SYSTEM},
                            {"role": "user", "content": convert_prompt},
                        ],
                        temperature=temperature,
                        max_tokens=max_tokens,
                        retries=provider_retries,
                    )
                    converted_candidate = (
                        _clean_candidates(convert_response.content, limit=1) or [candidate]
                    )[0]
                    selected_candidate = _select_candidate(candidate, converted_candidate)
                    async with memory_lock:
                        heuristic = _heuristic_variation(selected_candidate, recent_outputs, seen_keys)
                        if heuristic["ok"]:
                            seen_keys.add(heuristic["key"])
                    judge_prompt = _judge_prompt(
                        list_goal=list_goal,
                        matrix_source_type=matrix_source_type,
                        matrix_source=matrix_source,
                        matrix_items=chain_items,
                        candidate=selected_candidate,
                    )
                    judge_response, judge_attempts = await router.chat(
                        messages=[
                            {"role": "system", "content": "Reply only Y or N."},
                            {"role": "user", "content": judge_prompt},
                        ],
                        temperature=0.1,
                        max_tokens=max_tokens,
                        retries=provider_retries,
                    )
                    semantic_y_or_n = _clean_yes_no(judge_response.content)
                    accepted = bool(
                        gen_response.ok
                        and convert_response.ok
                        and judge_response.ok
                        and selected_candidate
                        and heuristic["ok"]
                        and semantic_y_or_n == "Y"
                    )
                    if accepted:
                        step_accepted.append(selected_candidate)
                        instance_outputs.append(selected_candidate)
                    candidate_records.append(
                        {
                            "raw_candidate": candidate,
                            "convert_prompt": convert_prompt,
                            "convert_raw": convert_response.content,
                            "convert_error": convert_response.error,
                            "convert_attempts": convert_attempts,
                            "converted_candidate": converted_candidate,
                            "candidate": selected_candidate,
                            "semantic_y_or_n": semantic_y_or_n,
                            "judge_raw": judge_response.content,
                            "judge_error": judge_response.error,
                            "judge_attempts": judge_attempts,
                            "judge_usage": judge_response.usage,
                            "heuristic": heuristic,
                            "accepted": accepted,
                        }
                    )
                if candidates:
                    last_output = candidates[-1]
                if step_accepted:
                    async with memory_lock:
                        for output in step_accepted:
                            if _key(output) not in {_key(item) for item in accepted_memory}:
                                accepted_memory.append(output)
                chain_records.append(
                    {
                        "step": step,
                        "matrix_items": chain_items,
                        "generation_prompt": gen_prompt,
                        "raw_output": gen_response.content,
                        "generation_error": gen_response.error,
                        "generation_attempts": gen_attempts,
                        "generation_usage": gen_response.usage,
                        "candidates": candidate_records,
                        "accepted_outputs": step_accepted,
                        "last_output_for_next_step": last_output,
                    }
                )

            record = {
                "record_id": f"{run_id}-{index:06d}",
                "source": "MatrixNodeExplorer-Harness",
                "payload": {
                    "index": index,
                    "matrix_source_type": matrix_source_type,
                    "matrix_source": matrix_source,
                    "matrix_items": matrix_items,
                    "recent_outputs": recent_outputs,
                    "chain_steps": chain_records,
                    "outputs": instance_outputs,
                    "comma_separated": ", ".join(instance_outputs),
                    "accepted": bool(instance_outputs),
                    "latency_ms": int((time.time() - gen_started) * 1000),
                    "model": model,
                },
            }
            ingest_result = await ingestion.ingest(record)
            return {
                "index": index,
                "ok": ingest_result.ok,
                "accepted": bool(instance_outputs),
                "candidate": instance_outputs[-1] if instance_outputs else "",
                "outputs": instance_outputs,
                "semantic_y_or_n": "Y" if instance_outputs else "N",
                "heuristic_ok": bool(instance_outputs),
                "ingest": ingest_result.to_dict(),
                "error": ingest_result.error,
            }

    try:
        results = await asyncio.gather(*(run_one(index) for index in range(nodes)))
    finally:
        router.shutdown()

    elapsed = round(time.time() - started, 3)
    ingestion_report = ingestion.report()
    outputs = [result["candidate"] for result in results if result.get("accepted")]
    all_outputs = [output for result in results for output in result.get("outputs", [])]
    _write_json(
        run_dir / "outputs.json",
        {
            "outputs": all_outputs,
            "comma_separated": ", ".join(all_outputs),
        },
    )
    report = _report(
        ok=ingestion_report["ok"] and all(result["ok"] for result in results),
        run_id=run_id,
        run_dir=run_dir,
        ingestion=ingestion,
        nodes_requested=nodes,
        results=results,
        error=None,
        elapsed_seconds=elapsed,
        ingestion_report=ingestion_report,
        router_stats=router.stats(),
    )
    _write_json(run_dir / "report.json", report)
    _write_markdown_report(run_dir / "report.md", report, all_outputs)
    return report


def _generation_prompt(
    matrix_source_type: str,
    matrix_source: str,
    list_goal: str,
    matrix_items: List[str],
    recent_outputs: List[str],
    axis: str,
    index: int,
    step: int,
    items_per_step: int,
    max_tokens: int,
) -> str:
    recent = ", ".join(recent_outputs[-12:]) if recent_outputs else "none"
    return (
        f'LIST GOAL: {{"{list_goal}"}}\n'
        f"SOURCE TYPE: {matrix_source_type}\n"
        f"SOURCE: {matrix_source}\n"
        f"AXIS: {axis}\n"
        f"INPUT: {matrix_items[0]}, {matrix_items[1]}, {matrix_items[2]}\n"
        f"EXACT REPEATS: {recent}\n"
        f"RULE: {FIRST_STAGE_PROMPT_STANCE['rule']}\n"
        f"AVOID: {FIRST_STAGE_PROMPT_STANCE['avoid']}.\n"
        f"ALLOW: {FIRST_STAGE_PROMPT_STANCE['allow']}.\n"
        f"DO NOT POLISH: {FIRST_STAGE_PROMPT_STANCE['do_not_polish']}.\n"
        f"RETURN: {items_per_step} comma-separated list items.\n"
        f"RULE: each item <= {max_tokens} tokens; no prose."
    )


def _judge_prompt(
    list_goal: str,
    matrix_source_type: str,
    matrix_source: str,
    matrix_items: List[str],
    candidate: str,
) -> str:
    return (
        f'LIST GOAL: {{"{list_goal}"}}\n'
        f"MATRIX SOURCE TYPE: {matrix_source_type}\n"
        f"MATRIX SOURCE: {matrix_source}\n"
        f"INPUTS: {', '.join(matrix_items)}\n"
        f"CANDIDATE: {candidate}\n"
        "Y if loosely connected, including odd but relevant tangents.\n"
        "N only if empty, exact repeat, pure formatting junk, or unrelated.\n"
        "QUESTION: Is this loosely connected to LIST GOAL and the matrix source? Y or N?"
    )


def _convert_prompt(
    list_goal: str,
    matrix_source_type: str,
    matrix_source: str,
    candidate: str,
    max_tokens: int,
) -> str:
    return (
        f'CONVERT TO GOAL: {{"{list_goal}"}}\n'
        f"SOURCE TYPE: {matrix_source_type}\n"
        f"SOURCE: {matrix_source}\n"
        f"INPUT: {candidate}\n"
        "ALLOW: rough wording if relevant.\n"
        f"RETURN: one coherent item <= {max_tokens} tokens; no prose."
    )


def _matrix_items_for(index: int, memory: List[str]) -> List[str]:
    if len(memory) < 3:
        raise ValueError("memory needs at least 3 items")
    a = memory[index % len(memory)]
    b = memory[(index * 3 + 1) % len(memory)]
    c = memory[(index * 7 + 2) % len(memory)]
    return [a, b, c]


def _heuristic_variation(
    candidate: str,
    recent_outputs: List[str],
    seen_keys: set[str],
) -> Dict[str, Any]:
    key = _key(candidate)
    recent_keys = {_key(item) for item in recent_outputs}
    errors = []
    if not key:
        errors.append("empty")
    if key.isdigit():
        errors.append("numeric")
    if _is_pure_formatting_junk(candidate):
        errors.append("formatting-junk")
    if key in recent_keys or key in seen_keys:
        errors.append("duplicate")
    if not _is_record_safe(candidate):
        errors.append("record-unsafe")
    return {
        "ok": not errors,
        "errors": errors,
        "key": key,
        "policy": "first-stage-expansive",
        "keeps_rough_relevant_outputs": True,
        "semantic_merging_deferred": True,
        "reject_only": FIRST_STAGE_REJECT_ONLY,
        "keep_if": FIRST_STAGE_KEEP_IF,
        "do_not_reject_for": FIRST_STAGE_DO_NOT_REJECT_FOR,
    }


def _select_candidate(raw_candidate: str, converted_candidate: str) -> str:
    converted_key = _key(converted_candidate)
    raw_key = _key(raw_candidate)
    if not converted_key:
        return raw_candidate
    if converted_key in PLACEHOLDER_PARTS and raw_key:
        return raw_candidate
    if len(converted_key) <= 2 and raw_key:
        return raw_candidate
    if "-" not in converted_key and len(raw_key.split("-")) > 1:
        return raw_candidate
    return converted_candidate


def _is_pure_formatting_junk(value: str) -> bool:
    stripped = value.strip()
    if not stripped:
        return True
    if any(character.isalnum() for character in stripped):
        return False
    return True


def _is_record_safe(value: str) -> bool:
    if not value or len(value) > 120:
        return False
    return all(character.isprintable() or character.isspace() for character in value)


def _report(
    ok: bool,
    run_id: str,
    run_dir: Path,
    ingestion: ShardedJsonlIngestionService,
    nodes_requested: int,
    results: List[Dict[str, Any]],
    error: Optional[str],
    elapsed_seconds: float = 0,
    ingestion_report: Optional[Dict[str, Any]] = None,
    router_stats: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    accepted = [result for result in results if result.get("accepted")]
    output_count = sum(len(result.get("outputs", [])) for result in results)
    y_count = sum(1 for result in results if result.get("semantic_y_or_n") == "Y")
    return {
        "ok": ok,
        "workflow_harness": "MatrixNodeExplorer-Harness",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "ingestion_run_dir": str(ingestion.run_dir),
        "nodes_requested": nodes_requested,
        "nodes_completed": len(results),
        "accepted_instance_count": len(accepted),
        "accepted_count": output_count,
        "semantic_y_count": y_count,
        "heuristic_ok_count": sum(1 for result in results if result.get("heuristic_ok")),
        "first_stage_policy": FIRST_STAGE_POLICY,
        "first_stage_prompt_stance": FIRST_STAGE_PROMPT_STANCE,
        "first_stage_reject_only": FIRST_STAGE_REJECT_ONLY,
        "first_stage_keep_if": FIRST_STAGE_KEEP_IF,
        "first_stage_do_not_reject_for": FIRST_STAGE_DO_NOT_REJECT_FOR,
        "later_stages": ["filter", "merge", "reduce", "map"],
        "elapsed_seconds": elapsed_seconds,
        "ingestion_report": ingestion_report,
        "router_stats": router_stats,
        "sample_outputs": [result.get("candidate") for result in accepted[:20]],
        "error": error,
    }


def _write_markdown_report(path: Path, report: Dict[str, Any], outputs: List[str]) -> None:
    lines = [
        "# MatrixNodeExplorer-Harness Report",
        "",
        f"- ok: `{str(report['ok']).lower()}`",
        f"- nodes: `{report['nodes_completed']}/{report['nodes_requested']}`",
        f"- accepted: `{report['accepted_count']}`",
        f"- semantic Y: `{report['semantic_y_count']}`",
        f"- heuristic ok: `{report['heuristic_ok_count']}`",
        f"- elapsed seconds: `{report['elapsed_seconds']}`",
        f"- ingestion: `{report['ingestion_run_dir']}`",
        "",
        "## Outputs",
        "",
        ", ".join(outputs[:80]),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _clean_candidate(content: str) -> str:
    first = content.strip().splitlines()[0] if content.strip() else ""
    first = first.split(",", 1)[0]
    first = re.sub(r"^\s*(?:[-*]|\d+[.)])\s*", "", first)
    cleaned = " ".join(first.strip(" .;:\"'`").split())[:80]
    return cleaned


def _clean_candidates(content: str, limit: int) -> List[str]:
    values = []
    seen = set()
    if "," in content or "\n" in content:
        parts = re.split(r",|\n", content.strip())
    else:
        parts = [content.strip()]
    for part in parts:
        cleaned = _clean_candidate(part)
        key = _key(cleaned)
        if cleaned and key not in seen:
            seen.add(key)
            values.append(cleaned)
        if len(values) >= limit:
            break
    return values


def _clean_yes_no(content: str) -> str:
    value = content.strip().upper()
    if value.startswith("Y"):
        return "Y"
    if value.startswith("N"):
        return "N"
    return "N"


def _parse_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _run_id() -> str:
    return f"{time.strftime('%Y%m%d-%H%M%S')}-{int((time.time() % 1) * 1000):03d}"


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
