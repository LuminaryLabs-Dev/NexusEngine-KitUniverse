from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from kituniverse_harness.providers import LMStudioProvider


DEFAULT_BASE_URL = "http://10.0.0.137:1234/v1"
DEFAULT_MODEL = "lfm2.5-1.2b-instruct"
DEFAULT_NAME = "Mario Party"
DEFAULT_INPUT = (
    "A multiplayer party board game where players roll dice to move around a "
    "board, trigger spaces, collect coins, buy stars, compete in short "
    "minigames, use items, and try to have the most stars when the match ends."
)
DEFAULT_SYSTEM = (
    "You discover flat categories associated with an input. Return only the "
    "requested comma-separated lowercase dash-name list."
)
CATEGORY_LANES = [
    "actions-and-controls",
    "resources-and-economy",
    "rules-and-win-state",
    "world-map-and-spaces",
    "events-and-randomness",
    "feedback-and-rewards",
    "multiplayer-and-turn-flow",
    "content-and-minigames",
    "state-and-persistence",
    "ui-and-player-information",
    "tools-and-items",
    "tests-and-validation",
]
BANNED_WORDS = {
    "api",
    "category",
    "domain",
    "framework",
    "gameplay",
    "general",
    "kit",
    "module",
    "mechanic",
    "mechanics",
    "service",
    "system",
}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-category-discovery",
        description="Discover a flat list of categories associated with an input.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--name", default=DEFAULT_NAME)
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--loops", type=int, default=8)
    parser.add_argument("--max-add", type=int, default=8)
    parser.add_argument("--memory-window", type=int, default=60)
    parser.add_argument("--system", default=DEFAULT_SYSTEM)
    parser.add_argument("--run-root", default="runs/workflow-harnesses/category-discovery")
    parser.add_argument("--temperature", type=float, default=0.35)
    parser.add_argument("--max-tokens", type=int, default=320)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--repair-attempts", type=int, default=1)
    parser.add_argument("--continue-on-error", action="store_true")
    args = parser.parse_args(argv)

    report = run_category_discovery(
        base_url=args.base_url,
        model=args.model,
        source_name=args.name,
        source_input=args.input,
        loops=args.loops,
        max_add=args.max_add,
        memory_window=args.memory_window,
        system=args.system,
        run_root=Path(args.run_root),
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        timeout_seconds=args.timeout_seconds,
        repair_attempts=args.repair_attempts,
        continue_on_error=args.continue_on_error,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


def run_category_discovery(
    base_url: str,
    model: str,
    source_name: str,
    source_input: str,
    loops: int,
    max_add: int,
    memory_window: int,
    system: str,
    run_root: Path,
    temperature: float,
    max_tokens: int,
    timeout_seconds: int,
    repair_attempts: int,
    continue_on_error: bool,
) -> Dict[str, Any]:
    if loops < 1:
        raise ValueError("--loops must be at least 1")
    if not 1 <= max_add <= 12:
        raise ValueError("--max-add must be between 1 and 12")

    run_id = _run_id()
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    provider = LMStudioProvider(
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
    )
    health = provider.health()
    _write_json(run_dir / "provider-health.json", health)

    ledger = {
        "source_name": source_name,
        "source_input": source_input,
        "config": {
            "loops": loops,
            "max_add": max_add,
            "memory_window": memory_window,
        },
        "categories": [],
        "category_keys": [],
        "turns": [],
        "stats": {},
    }
    ledger_path = run_dir / "category-ledger.json"
    categories_path = run_dir / "categories.json"
    _write_json(ledger_path, ledger)

    if not health.get("ok"):
        report = _report(
            ok=False,
            run_id=run_id,
            run_dir=run_dir,
            ledger_path=ledger_path,
            categories_path=categories_path,
            model=model,
            base_url=base_url,
            calls_completed=0,
            ledger=ledger,
            error="provider health failed",
        )
        _write_json(run_dir / "report.json", report)
        return report

    calls_completed = 0
    error = None
    for index in range(1, loops + 1):
        prompt = _build_prompt(
            source_name=source_name,
            source_input=source_input,
            categories=ledger["categories"],
            loop_index=index,
            max_add=max_add,
            memory_window=memory_window,
        )
        response = provider.chat(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        calls_completed += 1
        accepted, rejected, proposed = _process_output(
            content=response.content,
            existing=set(ledger["category_keys"]),
            source_name=source_name,
            max_add=max_add,
        )
        attempts = [
            _attempt(
                attempt=1,
                prompt=prompt,
                response=response,
                proposed=proposed,
                accepted=accepted,
                rejected=rejected,
            )
        ]

        for repair_index in range(1, repair_attempts + 1):
            if accepted:
                break
            repair_prompt = (
                prompt
                + "\n\nPREVIOUS BAD OUTPUT:\n"
                + response.content
                + "\n\nREPAIR:\nReturn only new comma-separated categories not already listed."
            )
            response = provider.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": repair_prompt},
                ],
                temperature=temperature,
                max_tokens=max_tokens,
            )
            calls_completed += 1
            accepted, rejected, proposed = _process_output(
                content=response.content,
                existing=set(ledger["category_keys"]),
                source_name=source_name,
                max_add=max_add,
            )
            attempts.append(
                _attempt(
                    attempt=repair_index + 1,
                    prompt=repair_prompt,
                    response=response,
                    proposed=proposed,
                    accepted=accepted,
                    rejected=rejected,
                )
            )

        for category in accepted:
            ledger["categories"].append(category)
            ledger["category_keys"].append(category)

        turn = {
            "turn": index,
            "ok": bool(accepted),
            "status": "accepted" if accepted else "exhausted",
            "lane": CATEGORY_LANES[(index - 1) % len(CATEGORY_LANES)],
            "accepted": accepted,
            "attempts": attempts,
            "memory_after": ledger["categories"],
            "calls_completed": calls_completed,
        }
        ledger["turns"].append(turn)
        ledger["stats"] = _stats(ledger)
        _write_json(ledger_path, ledger)

        if not accepted and not continue_on_error:
            error = "turn produced no accepted categories"
            break

    _write_json(
        categories_path,
        {
            "source_name": source_name,
            "source_input": source_input,
            "categories": ledger["categories"],
            "comma_separated": ", ".join(ledger["categories"]),
        },
    )
    ok = bool(ledger["categories"]) and error is None
    report = _report(
        ok=ok,
        run_id=run_id,
        run_dir=run_dir,
        ledger_path=ledger_path,
        categories_path=categories_path,
        model=model,
        base_url=base_url,
        calls_completed=calls_completed,
        ledger=ledger,
        error=error,
    )
    _write_json(run_dir / "report.json", report)
    _write_markdown_report(run_dir / "report.md", report, ledger)
    return report


def _build_prompt(
    source_name: str,
    source_input: str,
    categories: List[str],
    loop_index: int,
    max_add: int,
    memory_window: int,
) -> str:
    lane = CATEGORY_LANES[(loop_index - 1) % len(CATEGORY_LANES)]
    recent = categories[-memory_window:]
    memory = ", ".join(recent) if recent else "none"
    return (
        f"INPUT NAME:\n{source_name}\n\n"
        f"INPUT DESCRIPTION:\n{source_input}\n\n"
        f"CATEGORY DISCOVERY PASS:\n{loop_index}\n\n"
        f"DISCOVERY LANE:\n{lane}\n\n"
        f"CATEGORIES ALREADY FOUND:\n{memory}\n\n"
        "GOAL:\n"
        f"List up to {max_add} flat categories associated with the INPUT.\n\n"
        "RULES:\n"
        f"- Output only a comma-separated list of 1 to {max_add} category names.\n"
        "- Each category must be lowercase dash-separated.\n"
        "- Do not make a hierarchy.\n"
        "- Do not explain.\n"
        "- Do not repeat CATEGORIES ALREADY FOUND.\n"
        "- Include concrete mechanics, resources, rules, states, actions, feedback, UI, progression, multiplayer, timing, scoring, content, tools, or tests.\n"
        "- Do not include product words: api, service, kit, module, framework, domain, category.\n"
        "- Stay grounded in the input."
    )


def _process_output(
    content: str,
    existing: set[str],
    source_name: str,
    max_add: int,
) -> tuple[List[str], List[Dict[str, str]], List[str]]:
    proposed = _parse_comma_names(content)
    accepted: List[str] = []
    rejected: List[Dict[str, str]] = []
    title_words = set(_key(source_name).split("-"))
    local = set(existing)
    for item in proposed:
        key = _key(item)
        words = key.split("-") if key else []
        reason = None
        if not key:
            reason = "empty"
        elif key in local:
            reason = "duplicate"
        elif len(words) < 2:
            reason = "too broad"
        elif len(words) > 5:
            reason = "too long"
        elif any(word in BANNED_WORDS for word in words):
            reason = "generic or product word"
        elif any(word in title_words for word in words):
            reason = "input title word"

        if reason:
            rejected.append({"category": item, "reason": reason})
            continue
        accepted.append(key)
        local.add(key)
        if len(accepted) >= max_add:
            break
    return accepted, rejected, proposed


def _parse_comma_names(content: str) -> List[str]:
    cleaned = re.sub(r"^```(?:text)?|```$", "", content.strip(), flags=re.MULTILINE).strip()
    names = []
    seen = set()
    for part in re.split(r",|\n", cleaned):
        part = re.sub(r"^\s*(?:[-*]|\d+[.)])\s*", "", part).strip(" .;:")
        key = _key(part)
        if key and key not in seen:
            seen.add(key)
            names.append(part)
    return names


def _attempt(
    attempt: int,
    prompt: str,
    response: Any,
    proposed: List[str],
    accepted: List[str],
    rejected: List[Dict[str, str]],
) -> Dict[str, Any]:
    return {
        "attempt": attempt,
        "prompt": prompt,
        "raw_output": response.content,
        "proposed": proposed,
        "accepted": accepted,
        "rejected": rejected,
        "ok": response.ok and bool(accepted),
        "error": response.error,
        "usage": response.usage,
    }


def _report(
    ok: bool,
    run_id: str,
    run_dir: Path,
    ledger_path: Path,
    categories_path: Path,
    model: str,
    base_url: str,
    calls_completed: int,
    ledger: Dict[str, Any],
    error: Optional[str],
) -> Dict[str, Any]:
    return {
        "ok": ok,
        "workflow_harness": "category-discovery",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "ledger_path": str(ledger_path),
        "categories_path": str(categories_path),
        "model": model,
        "base_url": base_url,
        "source_name": ledger["source_name"],
        "calls_completed": calls_completed,
        "stats": _stats(ledger),
        "categories": ledger["categories"],
        "comma_separated": ", ".join(ledger["categories"]),
        "error": error,
    }


def _stats(ledger: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "categories": len(ledger["categories"]),
        "turns": len(ledger["turns"]),
        "accepted_turns": sum(1 for turn in ledger["turns"] if turn.get("status") == "accepted"),
        "exhausted_turns": sum(1 for turn in ledger["turns"] if turn.get("status") == "exhausted"),
    }


def _key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _run_id() -> str:
    return f"{time.strftime('%Y%m%d-%H%M%S')}-{int((time.time() % 1) * 1000):03d}"


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_markdown_report(path: Path, report: Dict[str, Any], ledger: Dict[str, Any]) -> None:
    lines = [
        "# Category Discovery Report",
        "",
        f"- ok: `{str(report['ok']).lower()}`",
        f"- source: `{report['source_name']}`",
        f"- model: `{report['model']}`",
        f"- calls: `{report['calls_completed']}`",
        f"- categories: `{report['stats']['categories']}`",
        f"- turns: `{report['stats']['turns']}`",
        f"- accepted turns: `{report['stats']['accepted_turns']}`",
        f"- exhausted turns: `{report['stats']['exhausted_turns']}`",
        f"- ledger: `{report['ledger_path']}`",
        f"- categories file: `{report['categories_path']}`",
    ]
    if report.get("error"):
        lines.append(f"- error: `{report['error']}`")
    lines.extend(["", "## Categories", "", report.get("comma_separated", "")])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
