from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from kituniverse_harness.providers import LMStudioProvider


DEFAULT_BASE_URL = "http://10.0.0.38:1234/v1"
DEFAULT_MODEL = "lfm2.5-350m-heretic-high-reasoning"
DEFAULT_IDEA = "A game where players discover reusable systems by exploring a living toolkit universe."
DEFAULT_REPLY_LENGTH = "one line"
DEFAULT_SYSTEM = (
    "You are a DSK domain translator. Convert ideas into reusable work domains. "
    "Prefer concrete domain names, clear work boundaries, and concise output. "
    "Never use filler names such as slot, slice, info, misc, general, or domain. "
    "Never rename the parent as a child."
)
DOMAIN_AXES = [
    "runtime-loop",
    "player-action",
    "world-state",
    "rule-system",
    "content-pipeline",
    "progression",
    "feedback",
    "tooling",
    "validation",
    "storage",
    "interface",
    "composition",
    "simulation",
    "resource-flow",
    "challenge-design",
    "reward-system",
    "navigation",
    "interaction-model",
    "authoring-tool",
    "test-harness",
]
GAMEPLAY_SURFACES = [
    {
        "surface": "jump-arc",
        "action": "press and release jump",
        "state": "airborne velocity",
        "rule": "jump height changes with hold time",
    },
    {
        "surface": "stomp-collision",
        "action": "land on enemy",
        "state": "enemy defeat state",
        "rule": "downward contact defeats enemy and bounces player",
    },
    {
        "surface": "enemy-patrol",
        "action": "avoid or bait enemy",
        "state": "patrol route",
        "rule": "enemy reverses at wall or ledge",
    },
    {
        "surface": "coin-route",
        "action": "collect coins",
        "state": "route reward trail",
        "rule": "coins guide safe and optional paths",
    },
    {
        "surface": "powerup-state",
        "action": "collect powerup",
        "state": "player ability mode",
        "rule": "powerup changes movement or collision affordance",
    },
    {
        "surface": "moving-platform",
        "action": "time platform landing",
        "state": "platform motion phase",
        "rule": "platform moves on authored timing curve",
    },
    {
        "surface": "hazard-contact",
        "action": "touch hazard",
        "state": "damage or fail state",
        "rule": "hazard contact triggers damage response",
    },
    {
        "surface": "checkpoint-respawn",
        "action": "reach checkpoint",
        "state": "respawn anchor",
        "rule": "latest checkpoint sets retry location",
    },
    {
        "surface": "door-key-route",
        "action": "unlock door",
        "state": "key and lock state",
        "rule": "matching key opens matching route gate",
    },
    {
        "surface": "camera-scroll",
        "action": "move through level",
        "state": "camera follow window",
        "rule": "camera reveals forward challenge without hiding landing zones",
    },
    {
        "surface": "terrain-tile",
        "action": "read ground shape",
        "state": "tile collision affordance",
        "rule": "tile shape communicates walk jump slide or block",
    },
    {
        "surface": "level-exit",
        "action": "reach goal",
        "state": "stage completion",
        "rule": "exit validates required objective and transitions stage",
    },
    {
        "surface": "secret-block",
        "action": "hit hidden block",
        "state": "revealed reward",
        "rule": "hidden blocks reward exploration and spatial inference",
    },
    {
        "surface": "enemy-spawn",
        "action": "enter trigger zone",
        "state": "spawned obstacle set",
        "rule": "spawn timing creates readable challenge pacing",
    },
    {
        "surface": "water-movement",
        "action": "swim",
        "state": "buoyancy movement mode",
        "rule": "water changes acceleration gravity and hazards",
    },
    {
        "surface": "shell-throw",
        "action": "kick carried object",
        "state": "projectile object path",
        "rule": "thrown shell interacts with enemies blocks and walls",
    },
    {
        "surface": "timer-pressure",
        "action": "finish before timer",
        "state": "remaining time",
        "rule": "timer creates urgency and scoring pressure",
    },
    {
        "surface": "score-feedback",
        "action": "chain successful actions",
        "state": "score multiplier",
        "rule": "success chains increase feedback and reward",
    },
    {
        "surface": "boss-pattern",
        "action": "learn attack rhythm",
        "state": "boss phase",
        "rule": "boss exposes pattern then punish window",
    },
    {
        "surface": "world-map",
        "action": "choose next stage",
        "state": "unlocked route graph",
        "rule": "completed stages unlock map routes",
    },
]
BANNED_DOMAIN_PARTS = {
    "slot",
    "slice",
    "info",
    "misc",
    "general",
    "domain",
    "kebab",
    "kitchen",
}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-dsk-domain-translator",
        description="Translate an idea into a DSK-style domain tree.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--idea", default=DEFAULT_IDEA)
    parser.add_argument("--subdomains", type=int, default=3)
    parser.add_argument("--depth", type=int, default=1)
    parser.add_argument("--reply-length", default=DEFAULT_REPLY_LENGTH)
    parser.add_argument("--system", default=DEFAULT_SYSTEM)
    parser.add_argument("--run-root", default="runs/workflow-harnesses/dsk-domain-translator")
    parser.add_argument("--temperature", type=float, default=0.2)
    parser.add_argument("--max-tokens", type=int, default=700)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--repair-attempts", type=int, default=2)
    parser.add_argument("--continue-on-error", action="store_true")
    args = parser.parse_args(argv)

    report = run_dsk_domain_translator(
        base_url=args.base_url,
        model=args.model,
        idea=args.idea,
        subdomains=args.subdomains,
        depth=args.depth,
        reply_length=args.reply_length,
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


def run_dsk_domain_translator(
    base_url: str,
    model: str,
    idea: str,
    subdomains: int,
    depth: int,
    reply_length: str,
    system: str,
    run_root: Path,
    temperature: float,
    max_tokens: int,
    timeout_seconds: int,
    repair_attempts: int,
    continue_on_error: bool,
) -> Dict[str, Any]:
    if subdomains < 0:
        raise ValueError("--subdomains must be 0 or greater")
    if depth < 0:
        raise ValueError("--depth must be 0 or greater")

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
    records_path = run_dir / "domain-records.json"
    tree_path = run_dir / "domain-tree.json"
    coverage_path = run_dir / "coverage-matrix.json"
    _write_json(records_path, records)

    if not health.get("ok"):
        report = _report(
            ok=False,
            run_id=run_id,
            run_dir=run_dir,
            records_path=records_path,
            tree_path=tree_path,
            coverage_path=coverage_path,
            model=model,
            base_url=base_url,
            idea=idea,
            subdomains=subdomains,
            depth=depth,
            calls_completed=0,
            error="provider health failed",
        )
        _write_json(run_dir / "report.json", report)
        return report

    calls_completed = 0
    ok = True
    error = None
    used_domains: set[str] = set()
    used_purposes: set[str] = set()

    root_response = _translate_domain(
        provider=provider,
        system=system,
        source_idea=idea,
        parent_domain=None,
        existing_domains=[],
        matrix_cell=None,
        layer=0,
        reply_length=reply_length,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    calls_completed += 1
    root = _record_domain(
        index=len(records) + 1,
        layer=0,
        parent_id=None,
        source_idea=idea,
        prompt=root_response["prompt"],
        response=root_response["response"],
        parent_domain=None,
        axis="root",
    )
    root_errors = _root_validation_errors(root)
    if root_errors:
        root = _fallback_root_record(root, idea, root_errors)
    root["validation_errors"] = []
    records.append(root)
    used_domains.add(root["domain"])
    if root["purpose"]:
        used_purposes.add(_normalize_compare(root["purpose"]))
    _write_json(records_path, records)

    if not root["ok"]:
        ok = False
        error = root.get("error") or "root domain translation failed"
        if not continue_on_error:
            return _finish(
                run_dir,
                run_id,
                records,
                records_path,
                tree_path,
                coverage_path,
                model,
                base_url,
                idea,
                subdomains,
                depth,
                calls_completed,
                ok,
                error,
            )

    frontier = [root]
    for layer in range(1, depth + 1):
        next_frontier: List[Dict[str, Any]] = []
        for parent in frontier:
            for sub_index in range(1, subdomains + 1):
                axis = _axis_for(sub_index, layer)
                matrix_cell = _matrix_cell_for(sub_index, layer)
                sub_idea_response = _chainstorm_subdomain_idea(
                    provider=provider,
                    system=system,
                    parent=parent,
                    existing_domains=sorted(used_domains),
                    axis=axis,
                    matrix_cell=matrix_cell,
                    sub_index=sub_index,
                    subdomains=subdomains,
                    layer=layer,
                    reply_length=reply_length,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                calls_completed += 1
                domain_response = _translate_domain(
                    provider=provider,
                    system=system,
                    source_idea=sub_idea_response["response"].content,
                    parent_domain=parent,
                    existing_domains=sorted(used_domains),
                    matrix_cell=matrix_cell,
                    layer=layer,
                    reply_length=reply_length,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                calls_completed += 1
                record, repair_calls = _translate_with_repair(
                    provider=provider,
                    system=system,
                    index=len(records) + 1,
                    layer=layer,
                    parent=parent,
                    source_idea=sub_idea_response["response"].content,
                    initial_prompt=domain_response["prompt"],
                    initial_response=domain_response["response"],
                    used_domains=used_domains,
                    used_purposes=used_purposes,
                    axis=axis,
                    matrix_cell=matrix_cell,
                    repair_attempts=repair_attempts,
                    reply_length=reply_length,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    subdomain_prompt=sub_idea_response["prompt"],
                    subdomain_output=sub_idea_response["response"].content,
                )
                calls_completed += repair_calls
                records.append(record)
                used_domains.add(record["domain"])
                if record["purpose"]:
                    used_purposes.add(_normalize_compare(record["purpose"]))
                next_frontier.append(record)
                _write_json(records_path, records)
                if not record["ok"]:
                    ok = False
                    error = record.get("error") or "domain translation failed"
                    if not continue_on_error:
                        return _finish(
                            run_dir,
                            run_id,
                            records,
                            records_path,
                            tree_path,
                            coverage_path,
                            model,
                            base_url,
                            idea,
                            subdomains,
                            depth,
                            calls_completed,
                            ok,
                            error,
                        )
        frontier = next_frontier

    return _finish(
        run_dir,
        run_id,
        records,
        records_path,
        tree_path,
        coverage_path,
        model,
        base_url,
        idea,
        subdomains,
        depth,
        calls_completed,
        ok,
        error,
    )


def _chainstorm_subdomain_idea(
    provider: LMStudioProvider,
    system: str,
    parent: Dict[str, Any],
    existing_domains: List[str],
    axis: str,
    matrix_cell: Dict[str, str],
    sub_index: int,
    subdomains: int,
    layer: int,
    reply_length: str,
    temperature: float,
    max_tokens: int,
) -> Dict[str, Any]:
    prompt = (
        f"PARENT DOMAIN: {parent.get('domain')}\n"
        f"PARENT PURPOSE: {parent.get('purpose')}\n"
        f"EXISTING DOMAINS: {', '.join(existing_domains) if existing_domains else 'none'}\n"
        f"DEPTH LAYER: {layer}\n"
        f"SUBDOMAIN SLOT: {sub_index} of {subdomains}\n"
        f"AXIS: {axis}\n"
        f"GAMEPLAY SURFACE: {matrix_cell['surface']}\n"
        f"PLAYER ACTION: {matrix_cell['action']}\n"
        f"GAME STATE: {matrix_cell['state']}\n"
        f"GAME RULE: {matrix_cell['rule']}\n"
        "OBJECT: one child\n"
        "EXPAND: one angle\n"
        "MAKE: subsystem\n"
        "AVOID: parent copy\n"
        "AVOID: filler names\n"
        "NEED: distinct work\n"
        f"RULE: Reply in {reply_length}."
    )
    response = provider.chat(
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return {"prompt": prompt, "response": response}


def _translate_domain(
    provider: LMStudioProvider,
    system: str,
    source_idea: str,
    parent_domain: Optional[Dict[str, Any]],
    existing_domains: List[str],
    matrix_cell: Optional[Dict[str, str]],
    layer: int,
    reply_length: str,
    temperature: float,
    max_tokens: int,
) -> Dict[str, Any]:
    parent_text = "none"
    if parent_domain:
        parent_text = f"{parent_domain.get('domain')} - {parent_domain.get('purpose')}"
    prompt = (
        "OBJECT: one domain\n"
        "COLLAPSE: one work\n"
        f"PARENT DOMAIN: {parent_text}\n"
        f"EXISTING DOMAINS: {', '.join(existing_domains) if existing_domains else 'none'}\n"
        f"MATRIX: {_matrix_text(matrix_cell)}\n"
        f"DEPTH LAYER: {layer}\n"
        f"IDEA: {source_idea}\n"
        "NAME: dash technical\n"
        "PURPOSE: work boundary\n"
        "AVOID: parent copy\n"
        "AVOID: slot slice info misc general domain kebab kitchen\n"
        f"RULE: Reply in {reply_length} and return only JSON with keys "
        "`domain`, `purpose`, `inputs`, `outputs`, `acceptance`. "
        "Use 2 inputs, 2 outputs, and 2 acceptance criteria."
    )
    response = provider.chat(
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return {"prompt": prompt, "response": response}


def _record_domain(
    index: int,
    layer: int,
    parent_id: Optional[str],
    source_idea: str,
    prompt: str,
    response: Any,
    parent_domain: Optional[Dict[str, Any]],
    axis: str,
    matrix_cell: Optional[Dict[str, str]] = None,
    subdomain_prompt: Optional[str] = None,
    subdomain_output: Optional[str] = None,
    repair_history: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    parsed = _extract_json_object(response.content)
    domain = _domain_name(parsed, response.content, index)
    record_id = f"d{index:04d}-{domain}"
    purpose = _purpose(parsed, response.content)
    return {
        "id": record_id,
        "index": index,
        "ok": response.ok,
        "layer": layer,
        "parent_id": parent_id,
        "axis": axis,
        "matrix_cell": matrix_cell,
        "domain": domain,
        "purpose": purpose,
        "inputs": _list_field(parsed, "inputs"),
        "outputs": _list_field(parsed, "outputs"),
        "acceptance": _list_field(parsed, "acceptance"),
        "source_idea": _clean(source_idea),
        "subdomain_prompt": subdomain_prompt,
        "subdomain_output": _clean(subdomain_output or ""),
        "domain_prompt": prompt,
        "domain_output": response.content,
        "parsed": parsed,
        "parent_domain": parent_domain.get("domain") if parent_domain else None,
        "repair_history": repair_history or [],
        "usage": response.usage,
        "error": response.error,
    }


def _translate_with_repair(
    provider: LMStudioProvider,
    system: str,
    index: int,
    layer: int,
    parent: Dict[str, Any],
    source_idea: str,
    initial_prompt: str,
    initial_response: Any,
    used_domains: set[str],
    used_purposes: set[str],
    axis: str,
    matrix_cell: Dict[str, str],
    repair_attempts: int,
    reply_length: str,
    temperature: float,
    max_tokens: int,
    subdomain_prompt: str,
    subdomain_output: str,
) -> tuple[Dict[str, Any], int]:
    repair_history: List[Dict[str, Any]] = []
    record = _record_domain(
        index=index,
        layer=layer,
        parent_id=parent["id"],
        source_idea=source_idea,
        prompt=initial_prompt,
        response=initial_response,
        parent_domain=parent,
        axis=axis,
        matrix_cell=matrix_cell,
        subdomain_prompt=subdomain_prompt,
        subdomain_output=subdomain_output,
    )
    errors = _domain_validation_errors(record, used_domains, used_purposes, parent)
    extra_calls = 0

    for attempt in range(1, repair_attempts + 1):
        if not errors:
            break
        repair_prompt = _repair_prompt(
            record=record,
            parent=parent,
            used_domains=sorted(used_domains),
            errors=errors,
            axis=axis,
            matrix_cell=matrix_cell,
            reply_length=reply_length,
        )
        repair_response = provider.chat(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": repair_prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        extra_calls += 1
        repair_history.append(
            {
                "attempt": attempt,
                "errors": errors,
                "prompt": repair_prompt,
                "output": repair_response.content,
                "ok": repair_response.ok,
            }
        )
        record = _record_domain(
            index=index,
            layer=layer,
            parent_id=parent["id"],
            source_idea=source_idea,
            prompt=repair_prompt,
            response=repair_response,
            parent_domain=parent,
            axis=axis,
            matrix_cell=matrix_cell,
            subdomain_prompt=subdomain_prompt,
            subdomain_output=subdomain_output,
            repair_history=repair_history,
        )
        errors = _domain_validation_errors(record, used_domains, used_purposes, parent)

    if errors:
        record = _fallback_record(
            index=index,
            layer=layer,
            parent=parent,
            source_idea=source_idea,
            axis=axis,
            matrix_cell=matrix_cell,
            used_domains=used_domains,
            errors=errors,
            subdomain_prompt=subdomain_prompt,
            subdomain_output=subdomain_output,
            repair_history=repair_history,
        )
        errors = []

    record["validation_errors"] = errors
    record["accepted_by"] = "repair" if repair_history else "initial"
    if record.get("fallback"):
        record["accepted_by"] = "fallback"
    return record, extra_calls


def _repair_prompt(
    record: Dict[str, Any],
    parent: Dict[str, Any],
    used_domains: List[str],
    errors: List[str],
    axis: str,
    matrix_cell: Dict[str, str],
    reply_length: str,
) -> str:
    return (
        "OBJECT: one repair\n"
        f"PARENT: {parent.get('domain')} - {parent.get('purpose')}\n"
        f"AXIS: {axis}\n"
        f"MATRIX: {_matrix_text(matrix_cell)}\n"
        f"BAD DOMAIN: {record.get('domain')}\n"
        f"BAD PURPOSE: {record.get('purpose')}\n"
        f"EXISTING: {', '.join(used_domains) if used_domains else 'none'}\n"
        f"FAIL: {'; '.join(errors)}\n"
        "REPAIR: one object\n"
        "USE: matrix evidence\n"
        "MAKE: unique child\n"
        "AVOID: parent words\n"
        "AVOID: filler names\n"
        f"RULE: Reply in {reply_length}; JSON keys `domain`, `purpose`, `inputs`, `outputs`, `acceptance`."
    )


def _domain_validation_errors(
    record: Dict[str, Any],
    used_domains: set[str],
    used_purposes: set[str],
    parent: Dict[str, Any],
) -> List[str]:
    errors: List[str] = []
    domain = str(record.get("domain", "")).strip().lower()
    purpose = str(record.get("purpose", "")).strip()
    parent_domain = str(parent.get("domain", "")).strip().lower()
    parent_purpose = str(parent.get("purpose", "")).strip()

    if not record.get("ok"):
        errors.append("model response empty")
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", domain):
        errors.append("domain must be lowercase dash-separated")
    if domain in used_domains:
        errors.append("domain already used")
    if domain == parent_domain or domain.startswith(f"{parent_domain}-"):
        errors.append("domain copies parent")
    if any(part in domain.split("-") for part in BANNED_DOMAIN_PARTS):
        errors.append("domain uses filler or drift word")
    if re.search(r"(?:^|-)(slot|slice|info|misc|general|domain)(?:-|$)", domain):
        errors.append("domain uses banned filler token")
    if len(domain.split("-")) < 2:
        errors.append("domain too broad")
    if len(purpose.split()) < 4:
        errors.append("purpose too short")
    if _normalize_compare(purpose) == _normalize_compare(parent_purpose):
        errors.append("purpose copies parent")
    if _normalize_compare(purpose) in used_purposes:
        errors.append("purpose already used")
    if not record.get("inputs") or not record.get("outputs") or not record.get("acceptance"):
        errors.append("missing inputs outputs or acceptance")
    return errors


def _root_validation_errors(record: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    domain = str(record.get("domain", "")).strip().lower()
    purpose = str(record.get("purpose", "")).strip()
    if not record.get("ok"):
        errors.append("root response empty")
    if any(part in domain.split("-") for part in BANNED_DOMAIN_PARTS):
        errors.append("root uses filler or drift word")
    if len(domain.split("-")) < 2:
        errors.append("root domain too broad")
    if len(purpose.split()) < 3:
        errors.append("root purpose too short")
    return errors


def _fallback_root_record(record: Dict[str, Any], idea: str, errors: List[str]) -> Dict[str, Any]:
    return {
        **record,
        "id": "d0001-platformer-system-map",
        "domain": "platformer-system-map",
        "purpose": "maps reusable platformer gameplay systems from the source idea",
        "inputs": ["source-idea", "domain-goal"],
        "outputs": ["root-domain", "gameplay-matrix"],
        "acceptance": ["root is specific", "children can branch"],
        "fallback": True,
        "accepted_by": "fallback",
        "rejected_errors": errors,
    }


def _fallback_record(
    index: int,
    layer: int,
    parent: Dict[str, Any],
    source_idea: str,
    axis: str,
    matrix_cell: Dict[str, str],
    used_domains: set[str],
    errors: List[str],
    subdomain_prompt: str,
    subdomain_output: str,
    repair_history: List[Dict[str, Any]],
) -> Dict[str, Any]:
    surface = matrix_cell["surface"]
    base = _unique_domain_name(f"{surface}-{axis}", used_domains)
    purpose = (
        f"controls {matrix_cell['rule']} for {surface} "
        f"through {axis.replace('-', ' ')} work"
    )
    return {
        "id": f"d{index:04d}-{base}",
        "index": index,
        "ok": True,
        "layer": layer,
        "parent_id": parent["id"],
        "axis": axis,
        "matrix_cell": matrix_cell,
        "domain": base,
        "purpose": purpose,
        "inputs": [matrix_cell["action"], matrix_cell["state"]],
        "outputs": [f"{surface}-decision", f"{axis}-validation"],
        "acceptance": [matrix_cell["rule"], "domain remains unique"],
        "source_idea": _clean(source_idea),
        "subdomain_prompt": subdomain_prompt,
        "subdomain_output": _clean(subdomain_output or ""),
        "domain_prompt": "fallback",
        "domain_output": "",
        "parsed": {},
        "parent_domain": parent.get("domain"),
        "repair_history": repair_history,
        "validation_errors": [],
        "rejected_errors": errors,
        "fallback": True,
        "usage": {},
        "error": None,
    }


def _finish(
    run_dir: Path,
    run_id: str,
    records: List[Dict[str, Any]],
    records_path: Path,
    tree_path: Path,
    coverage_path: Path,
    model: str,
    base_url: str,
    idea: str,
    subdomains: int,
    depth: int,
    calls_completed: int,
    ok: bool,
    error: Optional[str],
) -> Dict[str, Any]:
    tree = _build_tree(records)
    coverage = _build_coverage_matrix(records)
    _write_json(tree_path, tree)
    _write_json(coverage_path, coverage)
    report = _report(
        ok=ok,
        run_id=run_id,
        run_dir=run_dir,
        records_path=records_path,
        tree_path=tree_path,
        coverage_path=coverage_path,
        model=model,
        base_url=base_url,
        idea=idea,
        subdomains=subdomains,
        depth=depth,
        calls_completed=calls_completed,
        error=error,
    )
    _write_json(run_dir / "report.json", report)
    _write_markdown_report(run_dir / "report.md", report, records)
    return report


def _build_tree(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_id = {
        record["id"]: {
            "id": record["id"],
            "domain": record["domain"],
            "purpose": record["purpose"],
            "layer": record["layer"],
            "children": [],
        }
        for record in records
    }
    roots = []
    for record in records:
        node = by_id[record["id"]]
        parent_id = record.get("parent_id")
        if parent_id and parent_id in by_id:
            by_id[parent_id]["children"].append(node)
        else:
            roots.append(node)
    return {"domains": roots}


def _build_coverage_matrix(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    cells = []
    surfaces: Dict[str, int] = {}
    axes: Dict[str, int] = {}
    for record in records:
        matrix_cell = record.get("matrix_cell") or {}
        surface = matrix_cell.get("surface")
        axis = record.get("axis")
        if surface:
            surfaces[surface] = surfaces.get(surface, 0) + 1
        if axis:
            axes[axis] = axes.get(axis, 0) + 1
        cells.append(
            {
                "domain": record.get("domain"),
                "parent_id": record.get("parent_id"),
                "layer": record.get("layer"),
                "axis": axis,
                "surface": surface,
                "action": matrix_cell.get("action"),
                "state": matrix_cell.get("state"),
                "rule": matrix_cell.get("rule"),
                "accepted_by": record.get("accepted_by"),
            }
        )
    return {
        "surface_counts": surfaces,
        "axis_counts": axes,
        "cells": cells,
    }


def _report(
    ok: bool,
    run_id: str,
    run_dir: Path,
    records_path: Path,
    tree_path: Path,
    coverage_path: Path,
    model: str,
    base_url: str,
    idea: str,
    subdomains: int,
    depth: int,
    calls_completed: int,
    error: Optional[str],
) -> Dict[str, Any]:
    expected_nodes = sum(subdomains**layer for layer in range(depth + 1))
    expected_calls = 1 + (expected_nodes - 1) * 2
    return {
        "ok": ok,
        "workflow_harness": "dsk-domain-translator",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "records_path": str(records_path),
        "tree_path": str(tree_path),
        "coverage_path": str(coverage_path),
        "model": model,
        "base_url": base_url,
        "idea": idea,
        "subdomains": subdomains,
        "depth": depth,
        "nodes_expected": expected_nodes,
        "calls_expected": expected_calls,
        "calls_completed": calls_completed,
        "error": error,
    }


def _extract_json_object(content: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(content)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", content, flags=re.DOTALL)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _domain_name(parsed: Dict[str, Any], content: str, index: int) -> str:
    raw = str(parsed.get("domain") or parsed.get("name") or "").strip()
    if not raw:
        raw = content.strip().splitlines()[0] if content.strip() else f"domain-{index}"
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", raw.lower()).strip("-")
    return slug[:80] or f"domain-{index}"


def _unique_domain_name(domain: str, used_domains: set[str]) -> str:
    if domain not in used_domains:
        return domain
    suffix = 2
    while f"{domain}-{suffix}" in used_domains:
        suffix += 1
    return f"{domain}-{suffix}"


def _axis_for(sub_index: int, layer: int) -> str:
    offset = (layer - 1) * 7
    return DOMAIN_AXES[(sub_index - 1 + offset) % len(DOMAIN_AXES)]


def _matrix_cell_for(sub_index: int, layer: int) -> Dict[str, str]:
    zero_index = sub_index - 1
    batch = zero_index // len(DOMAIN_AXES)
    surface_index = (zero_index + batch * 7 + (layer - 1) * 11) % len(GAMEPLAY_SURFACES)
    return GAMEPLAY_SURFACES[surface_index]


def _matrix_text(matrix_cell: Optional[Dict[str, str]]) -> str:
    if not matrix_cell:
        return "root idea"
    return (
        f"surface={matrix_cell['surface']}; "
        f"action={matrix_cell['action']}; "
        f"state={matrix_cell['state']}; "
        f"rule={matrix_cell['rule']}"
    )


def _purpose(parsed: Dict[str, Any], content: str) -> str:
    raw = str(parsed.get("purpose") or "").strip()
    if raw:
        return _clean(raw)
    return _clean(content)[:240]


def _list_field(parsed: Dict[str, Any], key: str) -> List[str]:
    value = parsed.get(key)
    if isinstance(value, list):
        return [_clean(str(item)) for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [_clean(value)]
    return []


def _clean(value: str) -> str:
    return " ".join(value.strip().split())


def _normalize_compare(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _run_id() -> str:
    return f"{time.strftime('%Y%m%d-%H%M%S')}-{int((time.time() % 1) * 1000):03d}-{os.getpid()}"


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_markdown_report(
    path: Path,
    report: Dict[str, Any],
    records: List[Dict[str, Any]],
) -> None:
    lines = [
        "# DSK Domain Translator Report",
        "",
        f"- ok: `{str(report['ok']).lower()}`",
        f"- run_id: `{report['run_id']}`",
        f"- model: `{report['model']}`",
        f"- depth: `{report['depth']}`",
        f"- subdomains: `{report['subdomains']}`",
        f"- calls: `{report['calls_completed']}/{report['calls_expected']}`",
        f"- records: `{report['records_path']}`",
        f"- tree: `{report['tree_path']}`",
        f"- coverage: `{report['coverage_path']}`",
        "",
        "## Domains",
        "",
    ]
    lines.extend(_tree_markdown_lines(records))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _tree_markdown_lines(records: List[Dict[str, Any]]) -> List[str]:
    children: Dict[Optional[str], List[Dict[str, Any]]] = {}
    for record in records:
        children.setdefault(record.get("parent_id"), []).append(record)

    lines: List[str] = []

    def walk(parent_id: Optional[str], indent_level: int) -> None:
        for record in children.get(parent_id, []):
            indent = "  " * indent_level
            lines.append(f"{indent}- `{record['domain']}`: {record['purpose']}")
            walk(record["id"], indent_level + 1)

    walk(None, 0)
    return lines


if __name__ == "__main__":
    raise SystemExit(main())
