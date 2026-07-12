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
DEFAULT_DOMAIN_KIT = (
    "Domain service kits are reusable, bounded capability modules that turn "
    "a domain idea into a clear service contract with inputs, outputs, state "
    "rules, and tests."
)
DEFAULT_SYSTEM = (
    "You discover concrete game domain service kits. Return exactly the "
    "requested comma-separated lowercase dash-name list and no extra prose."
)
DEFAULT_NAME = "Mario Party"
DEFAULT_DESCRIPTION = (
    "A multiplayer party board game where players roll dice to move around a "
    "board, trigger spaces, collect coins, buy stars, compete in short "
    "minigames, use items, and try to have the most stars when the match ends."
)
BENCHMARK_GAMES = [
    {
        "name": "Mario Party",
        "description": DEFAULT_DESCRIPTION,
    },
    {
        "name": "The Legend of Zelda: Breath of the Wild",
        "description": (
            "An open-world action adventure where the player climbs, glides, "
            "cooks, solves shrine puzzles, fights enemies, manages weapons, "
            "uses physics and elemental interactions, and explores a large map."
        ),
    },
    {
        "name": "Stardew Valley",
        "description": (
            "A farming life sim where the player grows crops, raises animals, "
            "mines, fishes, crafts machines, builds relationships, restores "
            "community bundles, and manages seasons, energy, and money."
        ),
    },
    {
        "name": "Fortnite Battle Royale",
        "description": (
            "A multiplayer battle royale where players drop onto an island, "
            "loot weapons, gather resources, build structures, survive a "
            "shrinking storm circle, fight opponents, and pursue quests."
        ),
    },
]
DISCOVERY_LANES = [
    "player-actions-and-controls",
    "world-board-map-and-position",
    "resources-inventory-and-economy",
    "events-challenges-and-encounters",
    "rules-scoring-and-win-state",
    "feedback-rewards-and-progression",
    "tools-items-and-abilities",
    "session-turns-and-multiplayer-flow",
]


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-domain-discovery",
        description="Loop a model to discover game domain service kits 3 at a time.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--name", default=DEFAULT_NAME)
    parser.add_argument("--description", default=DEFAULT_DESCRIPTION)
    parser.add_argument("--domain-kit", default=DEFAULT_DOMAIN_KIT)
    parser.add_argument("--loops", type=int, default=10)
    parser.add_argument("--max-add", type=int, default=3)
    parser.add_argument("--system", default=DEFAULT_SYSTEM)
    parser.add_argument("--run-root", default="runs/workflow-harnesses/domain-discovery")
    parser.add_argument("--temperature", type=float, default=0.35)
    parser.add_argument("--max-tokens", type=int, default=220)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--repair-attempts", type=int, default=1)
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    args = parser.parse_args(argv)

    if args.benchmark:
        report = run_benchmark(
            base_url=args.base_url,
            model=args.model,
            domain_kit=args.domain_kit,
            loops=args.loops,
            max_add=args.max_add,
            system=args.system,
            run_root=Path(args.run_root),
            temperature=args.temperature,
            max_tokens=args.max_tokens,
            timeout_seconds=args.timeout_seconds,
            repair_attempts=args.repair_attempts,
            continue_on_error=args.continue_on_error,
        )
    else:
        report = run_domain_discovery(
            base_url=args.base_url,
            model=args.model,
            idea_source_name=args.name,
            idea_source_description=args.description,
            domain_kit=args.domain_kit,
            loops=args.loops,
            max_add=args.max_add,
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


def run_benchmark(
    base_url: str,
    model: str,
    domain_kit: str,
    loops: int,
    max_add: int,
    system: str,
    run_root: Path,
    temperature: float,
    max_tokens: int,
    timeout_seconds: int,
    repair_attempts: int,
    continue_on_error: bool,
) -> Dict[str, Any]:
    run_id = _run_id()
    bench_dir = run_root / "benchmarks" / run_id
    bench_dir.mkdir(parents=True, exist_ok=True)

    reports = []
    for game in BENCHMARK_GAMES:
        game_root = bench_dir / _slug(game["name"])
        report = run_domain_discovery(
            base_url=base_url,
            model=model,
            idea_source_name=game["name"],
            idea_source_description=game["description"],
            domain_kit=domain_kit,
            loops=loops,
            max_add=max_add,
            system=system,
            run_root=game_root,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout_seconds=timeout_seconds,
            repair_attempts=repair_attempts,
            continue_on_error=continue_on_error,
        )
        reports.append(report)
        if not report["ok"] and not continue_on_error:
            break

    summary = {
        "ok": all(report["ok"] for report in reports),
        "workflow_harness": "domain-discovery-benchmark",
        "run_id": run_id,
        "run_dir": str(bench_dir),
        "base_url": base_url,
        "model": model,
        "games_requested": len(BENCHMARK_GAMES),
        "games_completed": len(reports),
        "loops_per_game": loops,
        "max_add_per_loop": max_add,
        "reports": reports,
        "scorecard": _benchmark_scorecard(reports),
    }
    _write_json(bench_dir / "benchmark-report.json", summary)
    _write_markdown_benchmark(bench_dir / "benchmark-report.md", summary)
    return summary


def run_domain_discovery(
    base_url: str,
    model: str,
    idea_source_name: str,
    idea_source_description: str,
    domain_kit: str,
    loops: int,
    max_add: int,
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
    if not 1 <= max_add <= 3:
        raise ValueError("--max-add must be between 1 and 3")

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

    records: List[Dict[str, Any]] = []
    domains: List[str] = []
    domain_keys: set[str] = set()
    calls_completed = 0
    ok = True
    error = None

    records_path = run_dir / "domain-discovery.json"
    _write_json(records_path, records)

    if not health.get("ok"):
        report = _report(
            ok=False,
            run_id=run_id,
            run_dir=run_dir,
            records_path=records_path,
            domains_path=run_dir / "domains.json",
            model=model,
            base_url=base_url,
            idea_source_name=idea_source_name,
            loops_requested=loops,
            loops_completed=0,
            calls_completed=0,
            max_add=max_add,
            domains=[],
            error="provider health failed",
        )
        _write_json(run_dir / "report.json", report)
        return report

    for index in range(1, loops + 1):
        prompt = _build_prompt(
            domain_kit=domain_kit,
            idea_source_name=idea_source_name,
            idea_source_description=idea_source_description,
            existing_domains=domains,
            max_add=max_add,
            loop_index=index,
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
        proposals = _parse_comma_domains(response.content)
        accepted, rejected = _accept_new_domains(
            proposals=proposals,
            existing_keys=domain_keys,
            max_add=max_add,
            idea_source_name=idea_source_name,
        )

        attempts = [
            {
                "attempt": 1,
                "prompt": prompt,
                "raw_output": response.content,
                "proposed_domains": proposals,
                "accepted_domains": accepted,
                "rejected_domains": rejected,
                "ok": response.ok and bool(accepted),
                "error": response.error,
            }
        ]

        for repair_index in range(1, repair_attempts + 1):
            if accepted:
                break
            repair_prompt = _build_repair_prompt(
                domain_kit=domain_kit,
                idea_source_name=idea_source_name,
                idea_source_description=idea_source_description,
                existing_domains=domains,
                bad_output=response.content,
                max_add=max_add,
                loop_index=index,
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
            proposals = _parse_comma_domains(response.content)
            accepted, rejected = _accept_new_domains(
                proposals=proposals,
                existing_keys=domain_keys,
                max_add=max_add,
                idea_source_name=idea_source_name,
            )
            attempts.append(
                {
                    "attempt": repair_index + 1,
                    "prompt": repair_prompt,
                    "raw_output": response.content,
                    "proposed_domains": proposals,
                    "accepted_domains": accepted,
                    "rejected_domains": rejected,
                    "ok": response.ok and bool(accepted),
                    "error": response.error,
                }
            )

        for domain in accepted:
            domain_keys.add(domain)
            domains.append(domain)

        step_ok = bool(accepted)
        record = {
            "index": index,
            "ok": step_ok,
            "idea_source_name": idea_source_name,
            "domains_before": attempts[0]["prompt"].split("DOMAINS ALREADY FOUND:\n", 1)[-1].split("\n\nGOAL:", 1)[0],
            "attempts": attempts,
            "accepted_domains": accepted,
            "domain_memory_after": domains,
            "calls_completed": calls_completed,
        }
        records.append(record)
        _write_json(records_path, records)

        if not step_ok:
            ok = False
            error = "loop produced no new accepted domains"
            if not continue_on_error:
                break

    domains_path = run_dir / "domains.json"
    _write_json(
        domains_path,
        {
            "idea_source_name": idea_source_name,
            "idea_source_description": idea_source_description,
            "domains": domains,
            "comma_separated": ", ".join(domains),
        },
    )
    report = _report(
        ok=ok,
        run_id=run_id,
        run_dir=run_dir,
        records_path=records_path,
        domains_path=domains_path,
        model=model,
        base_url=base_url,
        idea_source_name=idea_source_name,
        loops_requested=loops,
        loops_completed=len(records),
        calls_completed=calls_completed,
        max_add=max_add,
        domains=domains,
        error=error,
    )
    _write_json(run_dir / "report.json", report)
    _write_markdown_report(run_dir / "report.md", report, records)
    return report


def _build_prompt(
    domain_kit: str,
    idea_source_name: str,
    idea_source_description: str,
    existing_domains: List[str],
    max_add: int,
    loop_index: int,
) -> str:
    memory = ", ".join(existing_domains) if existing_domains else "none"
    lane = DISCOVERY_LANES[(loop_index - 1) % len(DISCOVERY_LANES)]
    source_hints = _source_hints(idea_source_description, existing_domains)
    required_hint = source_hints[(loop_index - 1) % len(source_hints)] if source_hints else "none"
    hints = ", ".join(source_hints)
    return (
        f"DOMAIN KIT DEFINITION:\n{domain_kit}\n\n"
        f"IDEA SOURCE NAME:\n{idea_source_name}\n\n"
        f"IDEA SOURCE DESCRIPTION:\n{idea_source_description}\n\n"
        f"DISCOVERY PASS:\n{loop_index}\n\n"
        f"CURRENT DISCOVERY LANE:\n{lane}\n\n"
        f"REQUIRED SOURCE PHRASE THIS PASS:\n{required_hint}\n\n"
        f"UNUSED SOURCE PHRASE HINTS:\n{hints or 'none'}\n\n"
        f"DOMAINS ALREADY FOUND:\n{memory}\n\n"
        f"FORBIDDEN OUTPUTS:\n{memory}\n\n"
        "GOAL:\n"
        f"Find up to {max_add} new concrete domain service kits that exist inside this game.\n\n"
        "RULES:\n"
        f"- Output only a comma-separated list of 1 to {max_add} domain names.\n"
        "- Each name must be lowercase dash-separated, like dice-roll or board-navigation.\n"
        "- Do not number the list.\n"
        "- Do not include explanations.\n"
        "- Do not repeat any domain already found.\n"
        "- Do not output anything from FORBIDDEN OUTPUTS.\n"
        "- At least one name must use REQUIRED SOURCE PHRASE THIS PASS when it is not none.\n"
        "- Prefer UNUSED SOURCE PHRASE HINTS before broad abstractions.\n"
        "- Do not include the game title or another game's title.\n"
        "- Do not use product words: api, service, kit, module, engine, suite, hub, asset, mod, toolkit.\n"
        "- Do not invent mechanics outside the idea source description.\n"
        "- Prefer concrete game mechanics, rules, resources, states, feedback, or progression systems."
    )


def _build_repair_prompt(
    domain_kit: str,
    idea_source_name: str,
    idea_source_description: str,
    existing_domains: List[str],
    bad_output: str,
    max_add: int,
    loop_index: int,
) -> str:
    return (
        _build_prompt(
            domain_kit=domain_kit,
            idea_source_name=idea_source_name,
            idea_source_description=idea_source_description,
            existing_domains=existing_domains,
            max_add=max_add,
            loop_index=loop_index,
        )
        + "\n\n"
        f"PREVIOUS BAD OUTPUT:\n{bad_output}\n\n"
        "REPAIR:\nReturn only new comma-separated domain names."
    )


def _parse_comma_domains(content: str) -> List[str]:
    cleaned = content.strip()
    cleaned = re.sub(r"^```(?:text)?|```$", "", cleaned, flags=re.MULTILINE).strip()
    parts = re.split(r",|\n", cleaned)
    domains = []
    seen = set()
    for part in parts:
        part = re.sub(r"^\s*(?:[-*]|\d+[.)])\s*", "", part).strip()
        part = re.sub(r"\s+", " ", part)
        part = part.strip(" .;:")
        if not part:
            continue
        key = _domain_key(part)
        if key and key not in seen:
            seen.add(key)
            domains.append(part)
    return domains


def _accept_new_domains(
    proposals: List[str],
    existing_keys: set[str],
    max_add: int,
    idea_source_name: str,
) -> tuple[List[str], List[Dict[str, str]]]:
    accepted = []
    rejected = []
    local = set(existing_keys)
    banned = {
        "api",
        "asset",
        "assets",
        "core",
        "domain",
        "engine",
        "expansion",
        "framework",
        "gameplay",
        "mechanics",
        "general",
        "hub",
        "kit",
        "kits",
        "mechanic",
        "misc",
        "mod",
        "mods",
        "module",
        "modules",
        "pack",
        "service",
        "services",
        "simulator",
        "suite",
        "system",
        "systems",
        "toolkit",
        "tools",
    }
    title_words = set(_domain_key(idea_source_name).split("-"))
    external_titles = {
        "minecraft",
        "pokemon",
        "zelda",
        "mario",
        "fortnite",
        "stardew",
        "halo",
        "roblox",
        "sonic",
    } - title_words
    for proposal in proposals:
        key = _domain_key(proposal)
        words = key.split("-") if key else []
        reason = None
        if not key:
            reason = "empty"
        elif key in local:
            reason = "duplicate"
        elif len(words) < 2:
            reason = "too broad"
        elif any(word in banned for word in words):
            reason = "product or generic word"
        elif any(word in title_words for word in words):
            reason = "game title word"
        elif any(word in external_titles for word in words):
            reason = "other game title"
        elif any(word in {"and", "or"} for word in words):
            reason = "compound connector"
        elif len(words) > 5:
            reason = "too long"

        if reason:
            rejected.append({"domain": proposal, "reason": reason})
            continue
        accepted.append(key)
        local.add(key)
        if len(accepted) >= max_add:
            break
    return accepted, rejected


def _report(
    ok: bool,
    run_id: str,
    run_dir: Path,
    records_path: Path,
    domains_path: Path,
    model: str,
    base_url: str,
    idea_source_name: str,
    loops_requested: int,
    loops_completed: int,
    calls_completed: int,
    max_add: int,
    domains: List[str],
    error: Optional[str],
) -> Dict[str, Any]:
    return {
        "ok": ok,
        "workflow_harness": "domain-discovery",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "records_path": str(records_path),
        "domains_path": str(domains_path),
        "model": model,
        "base_url": base_url,
        "idea_source_name": idea_source_name,
        "loops_requested": loops_requested,
        "loops_completed": loops_completed,
        "calls_completed": calls_completed,
        "max_add_per_loop": max_add,
        "domain_count": len(domains),
        "domains": domains,
        "comma_separated": ", ".join(domains),
        "error": error,
    }


def _benchmark_scorecard(reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_domains = sum(report.get("domain_count", 0) for report in reports)
    total_loops = sum(report.get("loops_completed", 0) for report in reports)
    return {
        "total_domains": total_domains,
        "total_loops": total_loops,
        "average_domains_per_loop": round(total_domains / total_loops, 2) if total_loops else 0,
        "game_scores": [
            {
                "name": report.get("idea_source_name"),
                "ok": report.get("ok"),
                "domain_count": report.get("domain_count"),
                "loops_completed": report.get("loops_completed"),
                "average_domains_per_loop": round(
                    report.get("domain_count", 0) / report.get("loops_completed", 1),
                    2,
                )
                if report.get("loops_completed")
                else 0,
                "sample": report.get("domains", [])[:8],
            }
            for report in reports
        ],
    }


def _domain_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _source_hints(description: str, existing_domains: List[str]) -> List[str]:
    existing_text = " ".join(existing_domains).lower()
    pieces = re.split(r",|\band\b|\bwhere\b|\bwith\b", description.lower())
    hints = []
    seen = set()
    for piece in pieces:
        words = [
            re.sub(r"[^a-z0-9]+", "", word)
            for word in piece.strip().split()
            if len(re.sub(r"[^a-z0-9]+", "", word)) > 2
        ]
        words = [
            word
            for word in words
            if word
            not in {
                "the",
                "player",
                "players",
                "where",
                "uses",
                "large",
                "short",
                "onto",
                "most",
                "when",
                "game",
            }
        ]
        if not words:
            continue
        hint = "-".join(words[:4])
        if hint in seen or hint.replace("-", " ") in existing_text:
            continue
        seen.add(hint)
        hints.append(hint)
        if len(hints) >= 10:
            break
    return hints


def _slug(value: str) -> str:
    return _domain_key(value) or "game"


def _run_id() -> str:
    return f"{time.strftime('%Y%m%d-%H%M%S')}-{int((time.time() % 1) * 1000):03d}"


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_markdown_report(
    path: Path,
    report: Dict[str, Any],
    records: List[Dict[str, Any]],
) -> None:
    lines = [
        "# Domain Discovery Report",
        "",
        f"- ok: `{str(report['ok']).lower()}`",
        f"- source: `{report['idea_source_name']}`",
        f"- model: `{report['model']}`",
        f"- loops: `{report['loops_completed']}/{report['loops_requested']}`",
        f"- calls: `{report['calls_completed']}`",
        f"- domains: `{report['domain_count']}`",
        f"- records: `{report['records_path']}`",
        f"- final: `{report['domains_path']}`",
    ]
    if report.get("error"):
        lines.append(f"- error: `{report['error']}`")
    lines.extend(["", "## Domains", "", report.get("comma_separated", ""), "", "## Turns", ""])
    for record in records:
        lines.append(
            f"- {record['index']}: accepted "
            f"`{', '.join(record.get('accepted_domains', []))}`"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_markdown_benchmark(path: Path, summary: Dict[str, Any]) -> None:
    lines = [
        "# Domain Discovery Benchmark",
        "",
        f"- ok: `{str(summary['ok']).lower()}`",
        f"- model: `{summary['model']}`",
        f"- games: `{summary['games_completed']}/{summary['games_requested']}`",
        f"- loops per game: `{summary['loops_per_game']}`",
        f"- total domains: `{summary['scorecard']['total_domains']}`",
        f"- avg domains per loop: `{summary['scorecard']['average_domains_per_loop']}`",
        "",
        "## Games",
        "",
    ]
    for score in summary["scorecard"]["game_scores"]:
        lines.append(
            f"- `{score['name']}`: ok=`{str(score['ok']).lower()}` "
            f"domains=`{score['domain_count']}` avg=`{score['average_domains_per_loop']}`"
        )
        lines.append(f"  - sample: {', '.join(score['sample'])}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
