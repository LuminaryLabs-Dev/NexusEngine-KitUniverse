from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from kituniverse_harness.providers import LMStudioProvider


DEFAULT_BASE_URL = "http://10.0.0.137:1234/v1"
DEFAULT_MODEL = "lfm2.5-350m-heretic-high-reasoning-i1"
DEFAULT_SEED = (
    "Start with one useful idea."
)
DEFAULT_SYSTEM = (
    "You are Chainstormer, a tangent-thinking workflow harness worker. "
    "Follow the requested idea type and reply length."
)
DEFAULT_TRANSLATOR_SYSTEM = (
    "You are Chainstormer Translator. Convert loose thoughts into coherent "
    "ideas that match the requested idea type and reply length."
)
DEFAULT_IDEA_TYPE = "game idea"
DEFAULT_REPLY_LENGTH = "one line"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow_chainstormer",
        description="Loop one model over chained input/output and record every turn.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--loops", type=int, default=100)
    parser.add_argument("--seed", default=DEFAULT_SEED)
    parser.add_argument("--idea-type", default=DEFAULT_IDEA_TYPE)
    parser.add_argument("--reply-length", default=DEFAULT_REPLY_LENGTH)
    parser.add_argument("--system", default=DEFAULT_SYSTEM)
    parser.add_argument("--translator-system", default=DEFAULT_TRANSLATOR_SYSTEM)
    parser.add_argument("--run-root", default="runs/workflow-harnesses/chainstormer")
    parser.add_argument("--temperature", type=float, default=0.2)
    parser.add_argument("--max-tokens", type=int, default=700)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--continue-on-error", action="store_true")
    args = parser.parse_args(argv)

    report = run_chainstormer(
        base_url=args.base_url,
        model=args.model,
        loops=args.loops,
        seed=args.seed,
        idea_type=args.idea_type,
        reply_length=args.reply_length,
        system=args.system,
        translator_system=args.translator_system,
        run_root=Path(args.run_root),
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        timeout_seconds=args.timeout_seconds,
        continue_on_error=args.continue_on_error,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


def run_chainstormer(
    base_url: str,
    model: str,
    loops: int,
    seed: str,
    idea_type: str,
    reply_length: str,
    system: str,
    translator_system: str,
    run_root: Path,
    temperature: float,
    max_tokens: int,
    timeout_seconds: int,
    continue_on_error: bool,
) -> Dict[str, Any]:
    if loops < 1:
        raise ValueError("--loops must be at least 1")

    run_id = time.strftime("%Y%m%d-%H%M%S")
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    provider = LMStudioProvider(
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
    )
    health = provider.health()
    _write_json(run_dir / "provider-health.json", health)

    records: List[Dict[str, Any]] = []
    records_path = run_dir / "chainstorm.json"
    _write_json_list(records_path, records)

    if not health.get("ok"):
        report = _report(
            ok=False,
            run_id=run_id,
            run_dir=run_dir,
            loops_requested=loops,
            loops_completed=0,
            model=model,
            base_url=base_url,
            records_path=records_path,
            idea_type=idea_type,
            reply_length=reply_length,
            error="provider health failed",
        )
        _write_json(run_dir / "report.json", report)
        return report

    last_thought = seed
    calls_completed = 0
    ok = True
    error = None

    for index in range(1, loops + 1):
        thought_prompt = _build_thought_prompt(
            idea_type=idea_type,
            last_thought=last_thought,
            reply_length=reply_length,
        )
        thought_started = time.time()
        thought_response = provider.chat(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": thought_prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        calls_completed += 1
        thought_elapsed = round(time.time() - thought_started, 3)

        idea_prompt = _build_idea_prompt(
            idea_type=idea_type,
            last_thought=thought_response.content,
            reply_length=reply_length,
        )
        idea_started = time.time()
        idea_response = provider.chat(
            messages=[
                {"role": "system", "content": translator_system},
                {"role": "user", "content": idea_prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        calls_completed += 1
        idea_elapsed = round(time.time() - idea_started, 3)

        step_ok = thought_response.ok and idea_response.ok
        record = {
            "index": index,
            "ok": step_ok,
            "idea_type": idea_type,
            "reply_length": reply_length,
            "last_thought": last_thought,
            "thought_prompt": thought_prompt,
            "thought_output": thought_response.content,
            "thought_usage": thought_response.usage,
            "thought_error": thought_response.error,
            "thought_elapsed_seconds": thought_elapsed,
            "idea_prompt": idea_prompt,
            "idea_output": idea_response.content,
            "idea_usage": idea_response.usage,
            "idea_error": idea_response.error,
            "idea_elapsed_seconds": idea_elapsed,
            "model": idea_response.model or thought_response.model,
            "calls_completed": calls_completed,
        }
        records.append(record)
        _write_json_list(records_path, records)

        if not step_ok:
            ok = False
            error = (
                thought_response.error
                or idea_response.error
                or "provider returned empty output"
            )
            if not continue_on_error:
                break

        last_thought = _clean_next_thought(idea_response.content)

    report = _report(
        ok=ok,
        run_id=run_id,
        run_dir=run_dir,
        loops_requested=loops,
        loops_completed=len(records),
        model=model,
        base_url=base_url,
        records_path=records_path,
        idea_type=idea_type,
        reply_length=reply_length,
        calls_completed=calls_completed,
        error=error,
    )
    _write_json(run_dir / "report.json", report)
    _write_markdown_report(run_dir / "report.md", report, records)
    return report


def _build_thought_prompt(idea_type: str, last_thought: str, reply_length: str) -> str:
    return (
        f"IDEA TYPE: {idea_type}\n"
        f"THINK TANGENTIALLY ABOUT: {last_thought}\n"
        f"RULE: Align to Idea type, reply in {reply_length}"
    )


def _build_idea_prompt(idea_type: str, last_thought: str, reply_length: str) -> str:
    return (
        f"CONVERT THOUGHT INTO A COHERENT {idea_type} IDEA:\n"
        f"LAST THOUGHT: {last_thought}\n"
        f"RULE: Reply in {reply_length}"
    )


def _clean_next_thought(output: str) -> str:
    return " ".join(output.strip().split())


def _report(
    ok: bool,
    run_id: str,
    run_dir: Path,
    loops_requested: int,
    loops_completed: int,
    model: str,
    base_url: str,
    records_path: Path,
    idea_type: str,
    reply_length: str,
    calls_completed: int = 0,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "ok": ok,
        "workflow_harness": "chainstormer",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "records_path": str(records_path),
        "loops_requested": loops_requested,
        "loops_completed": loops_completed,
        "model": model,
        "base_url": base_url,
        "idea_type": idea_type,
        "reply_length": reply_length,
        "calls_completed": calls_completed,
        "calls_expected": loops_requested * 2,
        "error": error,
    }


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_json_list(path: Path, data: List[Dict[str, Any]]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_markdown_report(
    path: Path,
    report: Dict[str, Any],
    records: List[Dict[str, Any]],
) -> None:
    lines = [
        "# Chainstormer Workflow Report",
        "",
        f"- ok: `{str(report['ok']).lower()}`",
        f"- run_id: `{report['run_id']}`",
        f"- model: `{report['model']}`",
        f"- loops: `{report['loops_completed']}/{report['loops_requested']}`",
        f"- records: `{report['records_path']}`",
    ]
    if report.get("error"):
        lines.append(f"- error: `{report['error']}`")
    lines.extend(["", "## Records", ""])
    for record in records:
        preview = " ".join(record.get("idea_output", "").split())[:160]
        lines.append(f"- {record['index']}: ok=`{str(record['ok']).lower()}` {preview}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
