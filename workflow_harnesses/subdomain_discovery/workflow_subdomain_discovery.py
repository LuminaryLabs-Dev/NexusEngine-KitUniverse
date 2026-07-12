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
DEFAULT_DESCRIPTION = (
    "A multiplayer party board game where players roll dice to move around a "
    "board, trigger spaces, collect coins, buy stars, compete in short "
    "minigames, use items, and try to have the most stars when the match ends."
)
DEFAULT_DOMAINS = "dice-roll, board-navigation, coin-collection, star-acquisition, minigame-resolution"
DEFAULT_SYSTEM = (
    "You discover concrete child domain service kits. Return only the requested "
    "comma-separated lowercase dash-name list."
)
DISCOVERY_LANES = [
    "inputs-and-events",
    "state-and-storage",
    "rules-and-validation",
    "outputs-and-feedback",
    "edge-cases-and-failure",
    "progression-and-rewards",
    "multiplayer-and-turn-flow",
    "tooling-and-tests",
]
BANNED_WORDS = {
    "api",
    "asset",
    "core",
    "domain",
    "engine",
    "feature",
    "features",
    "framework",
    "gameplay",
    "general",
    "hub",
    "details",
    "kit",
    "module",
    "process",
    "service",
    "special",
    "specials",
    "system",
    "tool",
    "tools",
    "toolkit",
    "variant",
    "variants",
}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-subdomain-discovery",
        description="Discover subdomains for each domain through a ledger-backed loop.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--name", default=DEFAULT_NAME)
    parser.add_argument("--description", default=DEFAULT_DESCRIPTION)
    parser.add_argument("--domains", default=DEFAULT_DOMAINS)
    parser.add_argument("--domains-file")
    parser.add_argument("--depth", type=int, default=1)
    parser.add_argument("--passes-per-parent", type=int, default=1)
    parser.add_argument("--max-add", type=int, default=3)
    parser.add_argument("--memory-window", type=int, default=40)
    parser.add_argument("--system", default=DEFAULT_SYSTEM)
    parser.add_argument("--run-root", default="runs/workflow-harnesses/subdomain-discovery")
    parser.add_argument("--temperature", type=float, default=0.35)
    parser.add_argument("--max-tokens", type=int, default=220)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--repair-attempts", type=int, default=1)
    parser.add_argument("--continue-on-error", action="store_true")
    args = parser.parse_args(argv)

    domains = _load_domains(args.domains, args.domains_file)
    report = run_subdomain_discovery(
        base_url=args.base_url,
        model=args.model,
        idea_source_name=args.name,
        idea_source_description=args.description,
        seed_domains=domains,
        depth=args.depth,
        passes_per_parent=args.passes_per_parent,
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


def run_subdomain_discovery(
    base_url: str,
    model: str,
    idea_source_name: str,
    idea_source_description: str,
    seed_domains: List[str],
    depth: int,
    passes_per_parent: int,
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
    if depth < 1:
        raise ValueError("--depth must be at least 1")
    if passes_per_parent < 1:
        raise ValueError("--passes-per-parent must be at least 1")
    if not 1 <= max_add <= 3:
        raise ValueError("--max-add must be between 1 and 3")
    if not seed_domains:
        raise ValueError("at least one seed domain is required")

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

    ledger = _new_ledger(
        idea_source_name=idea_source_name,
        idea_source_description=idea_source_description,
        seed_domains=seed_domains,
        depth=depth,
        passes_per_parent=passes_per_parent,
        max_add=max_add,
        memory_window=memory_window,
    )
    ledger_path = run_dir / "subdomain-ledger.json"
    _write_json(ledger_path, ledger)

    if not health.get("ok"):
        report = _report(
            ok=False,
            run_id=run_id,
            run_dir=run_dir,
            ledger_path=ledger_path,
            tree_path=run_dir / "subdomain-tree.json",
            model=model,
            base_url=base_url,
            calls_completed=0,
            error="provider health failed",
            ledger=ledger,
        )
        _write_json(run_dir / "report.json", report)
        return report

    calls_completed = 0
    hard_error = None
    frontier = list(ledger["root_ids"])

    for layer in range(1, depth + 1):
        next_frontier: List[str] = []
        for parent_id in frontier:
            parent = ledger["nodes"][parent_id]
            for pass_index in range(1, passes_per_parent + 1):
                prompt = _build_prompt(
                    idea_source_name=idea_source_name,
                    idea_source_description=idea_source_description,
                    ledger=ledger,
                    parent=parent,
                    layer=layer,
                    pass_index=pass_index,
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
                accepted, rejected, proposals = _process_output(
                    content=response.content,
                    ledger=ledger,
                    parent=parent,
                    max_add=max_add,
                    idea_source_name=idea_source_name,
                )
                attempts = [
                    _attempt(
                        attempt=1,
                        prompt=prompt,
                        response=response,
                        proposals=proposals,
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
                        + "\n\nREPAIR:\nReturn only new child subdomains for the active parent."
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
                    accepted, rejected, proposals = _process_output(
                        content=response.content,
                        ledger=ledger,
                        parent=parent,
                        max_add=max_add,
                        idea_source_name=idea_source_name,
                    )
                    attempts.append(
                        _attempt(
                            attempt=repair_index + 1,
                            prompt=repair_prompt,
                            response=response,
                            proposals=proposals,
                            accepted=accepted,
                            rejected=rejected,
                        )
                    )

                child_ids = []
                for name in accepted:
                    child_id = _add_node(ledger, name=name, parent_id=parent_id, layer=layer)
                    child_ids.append(child_id)
                    next_frontier.append(child_id)

                turn = {
                    "turn": len(ledger["turns"]) + 1,
                    "ok": bool(accepted),
                    "status": "accepted" if accepted else "exhausted",
                    "layer": layer,
                    "pass": pass_index,
                    "parent_id": parent_id,
                    "parent": parent["name"],
                    "accepted": accepted,
                    "child_ids": child_ids,
                    "attempts": attempts,
                    "calls_completed": calls_completed,
                }
                ledger["turns"].append(turn)
                ledger["stats"] = _ledger_stats(ledger)
                _write_json(ledger_path, ledger)
        frontier = next_frontier
        if not frontier:
            break

    tree_path = _write_tree(run_dir, ledger)
    stats = _ledger_stats(ledger)
    return _finish(
        run_dir=run_dir,
        run_id=run_id,
        ledger_path=ledger_path,
        tree_path=tree_path,
        model=model,
        base_url=base_url,
        calls_completed=calls_completed,
        ok=stats["subdomains"] > 0,
        error=hard_error,
        ledger=ledger,
    )


def _new_ledger(
    idea_source_name: str,
    idea_source_description: str,
    seed_domains: List[str],
    depth: int,
    passes_per_parent: int,
    max_add: int,
    memory_window: int,
) -> Dict[str, Any]:
    ledger: Dict[str, Any] = {
        "idea_source_name": idea_source_name,
        "idea_source_description": idea_source_description,
        "config": {
            "depth": depth,
            "passes_per_parent": passes_per_parent,
            "max_add": max_add,
            "memory_window": memory_window,
        },
        "root_ids": [],
        "nodes": {},
        "name_to_id": {},
        "turns": [],
        "stats": {},
    }
    for domain in seed_domains:
        node_id = _add_node(ledger, name=domain, parent_id=None, layer=0)
        ledger["root_ids"].append(node_id)
    ledger["stats"] = _ledger_stats(ledger)
    return ledger


def _add_node(ledger: Dict[str, Any], name: str, parent_id: Optional[str], layer: int) -> str:
    key = _domain_key(name)
    node_id = f"n{len(ledger['nodes']) + 1:04d}-{key}"
    path = [key]
    if parent_id:
        path = ledger["nodes"][parent_id]["path"] + [key]
    ledger["nodes"][node_id] = {
        "id": node_id,
        "name": key,
        "parent_id": parent_id,
        "layer": layer,
        "path": path,
        "children": [],
    }
    ledger["name_to_id"][key] = node_id
    if parent_id:
        ledger["nodes"][parent_id]["children"].append(node_id)
    return node_id


def _build_prompt(
    idea_source_name: str,
    idea_source_description: str,
    ledger: Dict[str, Any],
    parent: Dict[str, Any],
    layer: int,
    pass_index: int,
    max_add: int,
    memory_window: int,
) -> str:
    lane = DISCOVERY_LANES[(pass_index + layer - 2) % len(DISCOVERY_LANES)]
    parent_children = [ledger["nodes"][child_id]["name"] for child_id in parent["children"]]
    global_recent = list(ledger["name_to_id"].keys())[-memory_window:]
    path = " > ".join(parent["path"])
    return (
        f"IDEA SOURCE NAME:\n{idea_source_name}\n\n"
        f"IDEA SOURCE DESCRIPTION:\n{idea_source_description}\n\n"
        f"ACTIVE PARENT DOMAIN:\n{parent['name']}\n\n"
        f"PARENT PATH:\n{path}\n\n"
        f"DEPTH LAYER:\n{layer}\n\n"
        f"DISCOVERY PASS FOR THIS PARENT:\n{pass_index}\n\n"
        f"DISCOVERY LANE:\n{lane}\n\n"
        f"CHILDREN ALREADY FOUND FOR THIS PARENT:\n{', '.join(parent_children) if parent_children else 'none'}\n\n"
        f"RECENT GLOBAL LEDGER MEMORY:\n{', '.join(global_recent) if global_recent else 'none'}\n\n"
        "GOAL:\n"
        f"Find up to {max_add} new child subdomains that belong under ACTIVE PARENT DOMAIN.\n\n"
        "RULES:\n"
        f"- Output only a comma-separated list of 1 to {max_add} child subdomain names.\n"
        "- Each name must be lowercase dash-separated.\n"
        "- Do not explain.\n"
        "- Do not repeat CHILDREN ALREADY FOUND or RECENT GLOBAL LEDGER MEMORY.\n"
        "- Do not rename the parent.\n"
        "- Do not include filler/product words: api, service, kit, module, engine, hub, framework, details, variants, features, tools, specials.\n"
        "- Child names must be more specific than the parent.\n"
        "- Stay inside the idea source description."
    )


def _process_output(
    content: str,
    ledger: Dict[str, Any],
    parent: Dict[str, Any],
    max_add: int,
    idea_source_name: str,
) -> tuple[List[str], List[Dict[str, str]], List[str]]:
    proposals = _parse_comma_names(content)
    accepted: List[str] = []
    rejected: List[Dict[str, str]] = []
    title_words = set(_domain_key(idea_source_name).split("-"))
    existing = set(ledger["name_to_id"].keys())
    parent_key = parent["name"]
    parent_words = set(parent_key.split("-"))

    for proposal in proposals:
        key = _domain_key(proposal)
        words = key.split("-") if key else []
        reason = None
        if not key:
            reason = "empty"
        elif key in existing:
            reason = "global duplicate"
        elif key in accepted:
            reason = "local duplicate"
        elif key == parent_key:
            reason = "parent copy"
        elif len(words) < 2:
            reason = "too broad"
        elif len(words) > 5:
            reason = "too long"
        elif any(word in BANNED_WORDS for word in words):
            reason = "generic or product word"
        elif any(word in title_words for word in words):
            reason = "game title word"
        elif not parent_words.intersection(words):
            reason = "not anchored to parent"

        if reason:
            rejected.append({"name": proposal, "reason": reason})
            continue
        accepted.append(key)
        existing.add(key)
        if len(accepted) >= max_add:
            break
    return accepted, rejected, proposals


def _attempt(
    attempt: int,
    prompt: str,
    response: Any,
    proposals: List[str],
    accepted: List[str],
    rejected: List[Dict[str, str]],
) -> Dict[str, Any]:
    return {
        "attempt": attempt,
        "prompt": prompt,
        "raw_output": response.content,
        "proposed": proposals,
        "accepted": accepted,
        "rejected": rejected,
        "ok": response.ok and bool(accepted),
        "error": response.error,
        "usage": response.usage,
    }


def _finish(
    run_dir: Path,
    run_id: str,
    ledger_path: Path,
    tree_path: Path,
    model: str,
    base_url: str,
    calls_completed: int,
    ok: bool,
    error: Optional[str],
    ledger: Dict[str, Any],
) -> Dict[str, Any]:
    report = _report(
        ok=ok,
        run_id=run_id,
        run_dir=run_dir,
        ledger_path=ledger_path,
        tree_path=tree_path,
        model=model,
        base_url=base_url,
        calls_completed=calls_completed,
        error=error,
        ledger=ledger,
    )
    _write_json(run_dir / "report.json", report)
    _write_markdown_report(run_dir / "report.md", report, ledger)
    return report


def _report(
    ok: bool,
    run_id: str,
    run_dir: Path,
    ledger_path: Path,
    tree_path: Path,
    model: str,
    base_url: str,
    calls_completed: int,
    error: Optional[str],
    ledger: Dict[str, Any],
) -> Dict[str, Any]:
    stats = _ledger_stats(ledger)
    return {
        "ok": ok,
        "workflow_harness": "subdomain-discovery",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "ledger_path": str(ledger_path),
        "tree_path": str(tree_path),
        "model": model,
        "base_url": base_url,
        "idea_source_name": ledger["idea_source_name"],
        "calls_completed": calls_completed,
        "stats": stats,
        "error": error,
    }


def _write_tree(run_dir: Path, ledger: Dict[str, Any]) -> Path:
    tree_path = run_dir / "subdomain-tree.json"
    roots = [_tree_node(ledger, root_id) for root_id in ledger["root_ids"]]
    _write_json(tree_path, {"domains": roots})
    return tree_path


def _tree_node(ledger: Dict[str, Any], node_id: str) -> Dict[str, Any]:
    node = ledger["nodes"][node_id]
    return {
        "id": node_id,
        "name": node["name"],
        "layer": node["layer"],
        "children": [_tree_node(ledger, child_id) for child_id in node["children"]],
    }


def _ledger_stats(ledger: Dict[str, Any]) -> Dict[str, Any]:
    nodes = list(ledger["nodes"].values())
    root_count = len(ledger["root_ids"])
    return {
        "root_domains": root_count,
        "total_nodes": len(nodes),
        "subdomains": max(0, len(nodes) - root_count),
        "turns": len(ledger["turns"]),
        "accepted_turns": sum(1 for turn in ledger["turns"] if turn.get("status") == "accepted"),
        "exhausted_turns": sum(1 for turn in ledger["turns"] if turn.get("status") == "exhausted"),
        "max_layer": max((node["layer"] for node in nodes), default=0),
    }


def _load_domains(domains: str, domains_file: Optional[str]) -> List[str]:
    if domains_file:
        data = json.loads(Path(domains_file).read_text(encoding="utf-8"))
        if isinstance(data, dict):
            raw = data.get("domains") or []
        elif isinstance(data, list):
            raw = data
        else:
            raw = []
        return [_domain_key(str(item)) for item in raw if _domain_key(str(item))]
    return [_domain_key(part) for part in _parse_comma_names(domains) if _domain_key(part)]


def _parse_comma_names(content: str) -> List[str]:
    cleaned = content.strip()
    cleaned = re.sub(r"^```(?:text)?|```$", "", cleaned, flags=re.MULTILINE).strip()
    names = []
    seen = set()
    for part in re.split(r",|\n", cleaned):
        part = re.sub(r"^\s*(?:[-*]|\d+[.)])\s*", "", part).strip(" .;:")
        if not part:
            continue
        key = _domain_key(part)
        if key and key not in seen:
            seen.add(key)
            names.append(part)
    return names


def _domain_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _run_id() -> str:
    return f"{time.strftime('%Y%m%d-%H%M%S')}-{int((time.time() % 1) * 1000):03d}"


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_markdown_report(path: Path, report: Dict[str, Any], ledger: Dict[str, Any]) -> None:
    lines = [
        "# Subdomain Discovery Report",
        "",
        f"- ok: `{str(report['ok']).lower()}`",
        f"- source: `{report['idea_source_name']}`",
        f"- model: `{report['model']}`",
        f"- calls: `{report['calls_completed']}`",
        f"- root domains: `{report['stats']['root_domains']}`",
        f"- subdomains: `{report['stats']['subdomains']}`",
        f"- turns: `{report['stats']['turns']}`",
        f"- ledger: `{report['ledger_path']}`",
        f"- tree: `{report['tree_path']}`",
    ]
    if report.get("error"):
        lines.append(f"- error: `{report['error']}`")
    lines.extend(["", "## Roots", ""])
    for root_id in ledger["root_ids"]:
        root = ledger["nodes"][root_id]
        children = [ledger["nodes"][child_id]["name"] for child_id in root["children"]]
        lines.append(f"- `{root['name']}`: {', '.join(children) if children else 'no children'}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
