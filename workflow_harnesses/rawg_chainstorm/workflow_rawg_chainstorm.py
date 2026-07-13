from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from kituniverse_harness.providers import LMStudioProvider, ProviderResponse
from workflow_harnesses.guided_kit_builder.codex_cli_review import CODEX_BINARY, CODEX_MODEL
from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash
from workflow_harnesses.rawg_domain_loop.evidence_chain import select_evidence_units
from workflow_harnesses.rawg_domain_loop.workflow_rawg_domain_loop import load_record


DEFAULT_SOURCE_ROOT = Path("/Users/crimsonwheeler/Documents/GitHub/NexusRealtime-Ideas/games/rawg/chunks")
DEFAULT_BASE_URL = "http://10.0.0.137:1234/v1"
DEFAULT_MODEL = "lfm2.5-1.2b-instruct"
DEFAULT_RUN_ROOT = Path("runs/workflow-harnesses/rawg-chainstorm")
GENERIC_LABELS = {
    "action", "adventure", "atmosphere", "atmospheric", "capability", "city", "cute",
    "dark", "domain", "editorial", "exclusive", "feature", "game", "gameplay", "gay",
    "genre", "mechanic", "metadata", "moddable", "platform", "reusable", "social",
    "steam-achievements", "system", "tags", "true-exclusive",
}
GENERIC_TOKENS = {
    "capability", "domain", "engine", "feature", "game", "gameplay", "genre", "mechanic",
    "mechanics", "metadata", "parent", "platform", "root", "rules", "system",
}
NON_MECHANICAL_PHRASES = {
    "cinematic", "engine", "forged with", "greatest abilities", "price to pay",
    "story rich", "tale", "unprecedented freedom",
}
EVIDENCE_STOPWORDS = {
    "about", "after", "again", "around", "being", "from", "have", "into", "more",
    "only", "over", "player", "players", "that", "their", "there", "these", "they",
    "this", "through", "under", "with", "your",
}


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--game-id")
    parser.add_argument("--record-file", type=Path)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--context-length", type=int, default=2048)
    parser.add_argument("--parallel", type=int, default=1)
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--beam-width", type=int, default=3)
    parser.add_argument("--time-budget-seconds", type=float, default=10.0)
    parser.add_argument("--temperature", type=float, default=0.8)
    parser.add_argument("--max-tokens", type=int, default=128)
    parser.add_argument("--timeout-seconds", type=int, default=12)
    parser.add_argument("--skip-codex", action="store_true")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-rawg-chainstorm",
        description="Chainstorm one RAWG game under a local time budget, then ask Codex for kit proposals.",
    )
    configure_parser(parser)
    report = run_chainstorm(parser.parse_args(argv))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


def run_chainstorm(args: argparse.Namespace) -> Dict[str, Any]:
    if bool(args.game_id) == bool(args.record_file):
        raise ValueError("provide exactly one of --game-id or --record-file")
    if not 2 <= args.rounds <= 4:
        raise ValueError("--rounds must be between 2 and 4")
    if not 2 <= args.beam_width <= 8:
        raise ValueError("--beam-width must be between 2 and 8")
    if args.time_budget_seconds <= 0:
        raise ValueError("--time-budget-seconds must be positive")

    record = load_record(args.record_file, args.game_id, args.source_root)
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
    run_dir = args.run_root / run_id
    rounds_dir = run_dir / "rounds"
    rounds_dir.mkdir(parents=True, exist_ok=True)
    _write_json(run_dir / "source-record.json", record)

    evidence_units = [
        item for item in select_evidence_units(record)
        if _mechanical_seed_unit(item)
    ][:8]
    evidence_seed = {
        "schema_version": "rawg.chainstorm-seed.v1",
        "source_id": record.get("source_id"),
        "source_hash": record.get("source_hash"),
        "name": record.get("name"),
        "genres": (record.get("genres") or [])[:8],
        "tags": (record.get("tags") or [])[:12],
        "evidence_units": evidence_units,
    }
    _write_json(run_dir / "evidence-seed.json", evidence_seed)
    manifest = {
        "schema_version": "rawg.chainstorm-manifest.v1",
        "run_id": run_id,
        "source_id": record.get("source_id"),
        "model": args.model,
        "base_url": args.base_url,
        "controls": {
            "rounds": args.rounds,
            "beam_width": args.beam_width,
            "time_budget_seconds": args.time_budget_seconds,
            "context_length": args.context_length,
            "parallel": args.parallel,
            "max_tokens": args.max_tokens,
        },
        "stages": [
            "evidence-seed", "lfm-chainstorm", "deterministic-filter",
            "codex-kit-proposals", "kit-build-requests",
        ],
    }
    _write_json(run_dir / "manifest.json", manifest)

    provider = LMStudioProvider(args.base_url, args.model, args.timeout_seconds)
    health = provider.health()
    load = provider.ensure_loaded(args.context_length, args.parallel)
    _write_json(run_dir / "provider-preflight.json", {"health": health, "load": load})
    if not health.get("ok") or not load.get("ok") or not load.get("config_matches", True):
        report = _base_report(args, run_id, run_dir, record)
        report.update({"ok": False, "status": "hold", "reason": "provider-preflight-failed", "provider": {"health": health, "load": load}})
        _write_json(run_dir / "report.json", report)
        return report

    raw_records: List[Dict[str, Any]] = []
    accepted_nodes: List[Dict[str, Any]] = []
    rejected_nodes: List[Dict[str, Any]] = []
    seen = set()
    active = [{
        "node_id": "root",
        "label": _seed_label(evidence_seed),
        "round": 0,
        "lineage_evidence_source_ids": [item["source_id"] for item in evidence_units],
    }]
    started = time.monotonic()
    calls = 0
    call_durations: List[float] = []
    rounds_completed = 0
    stop_reason = "round-limit"

    for round_index in range(1, args.rounds + 1):
        elapsed = time.monotonic() - started
        if elapsed >= args.time_budget_seconds:
            stop_reason = "time-budget"
            break
        if round_index > 2 and call_durations:
            predicted_call = max(call_durations) * 1.25
            if elapsed + predicted_call > args.time_budget_seconds:
                stop_reason = "predicted-time-budget"
                break
        prompt = _build_prompt(record, active, round_index, args.beam_width)
        call_started = time.monotonic()
        response = provider.chat(
            messages=[
                {"role": "system", "content": "Generate short tangent ideas only. Do not plan, review, explain, or use tools."},
                {"role": "user", "content": prompt},
            ],
            temperature=args.temperature,
            max_tokens=args.max_tokens,
        )
        calls += 1
        call_elapsed = time.monotonic() - call_started
        call_durations.append(call_elapsed)
        proposals = _parse_lines(response, active, args.beam_width if round_index == 1 else 1)
        accepted, rejected = _filter_proposals(proposals, seen, round_index, record, active, evidence_units)
        raw_record = {
            "round": round_index,
            "input_pointers": [item["node_id"] for item in active],
            "prompt": prompt,
            "response": response.to_dict(),
            "elapsed_seconds": round(call_elapsed, 3),
            "proposals": proposals,
            "accepted": accepted,
            "rejected": rejected,
        }
        raw_records.append(raw_record)
        accepted_nodes.extend(accepted)
        rejected_nodes.extend(rejected)
        rounds_completed = round_index
        _write_json(rounds_dir / f"round-{round_index:02d}.json", raw_record)
        _write_jsonl(run_dir / "raw-chainstorm.jsonl", raw_records)
        _write_jsonl(run_dir / "filtered-nodes.jsonl", accepted_nodes)
        active = accepted[-args.beam_width:]
        _write_json(
            run_dir / "loop-state.json",
            {
                "original_goal": "generate tangential but category-adjacent reusable game capability ideas",
                "source_id": record.get("source_id"),
                "round": round_index,
                "accepted_node_ids": [item["node_id"] for item in accepted_nodes],
                "rejected_node_ids": [item["node_id"] for item in rejected_nodes],
                "active_pointers": [item["node_id"] for item in active],
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "next_action": "codex-kit-proposals" if round_index == args.rounds or not active else f"chainstorm-round-{round_index + 1}",
                "stop_criteria": {"rounds": args.rounds, "time_budget_seconds": args.time_budget_seconds},
            },
        )
        if not response.ok:
            stop_reason = "provider-failed"
            break
        if not active:
            stop_reason = "no-accepted-pointers"
            break

    local_elapsed = time.monotonic() - started
    if local_elapsed > args.time_budget_seconds and stop_reason == "round-limit":
        stop_reason = "round-limit-budget-overrun"

    codex = {"ok": True, "skipped": True, "reason": "skip-codex"}
    kits: List[Dict[str, Any]] = []
    if not args.skip_codex and accepted_nodes:
        codex = _run_codex(run_dir, record, accepted_nodes)
        kits = codex.get("kits") or []
    _write_json(run_dir / "codex-kit-proposals.json", codex)
    requests = _build_requests(record, kits, accepted_nodes)
    _write_jsonl(run_dir / "kit-build-requests.jsonl", requests)

    report = _base_report(args, run_id, run_dir, record)
    report.update(
        {
            "ok": bool(accepted_nodes) and codex.get("ok", False),
            "status": "complete",
            "stop_reason": stop_reason,
            "rounds_completed": rounds_completed,
            "local_calls": calls,
            "local_elapsed_seconds": round(local_elapsed, 3),
            "within_local_time_budget": local_elapsed <= args.time_budget_seconds,
            "accepted_node_count": len(accepted_nodes),
            "rejected_node_count": len(rejected_nodes),
            "codex_elapsed_seconds": codex.get("elapsed_seconds", 0),
            "kit_proposal_count": len(kits),
            "build_request_count": len(requests),
            "artifacts": {
                "manifest": str(run_dir / "manifest.json"),
                "raw_chainstorm": str(run_dir / "raw-chainstorm.jsonl"),
                "filtered_nodes": str(run_dir / "filtered-nodes.jsonl"),
                "codex_kit_proposals": str(run_dir / "codex-kit-proposals.json"),
                "kit_build_requests": str(run_dir / "kit-build-requests.jsonl"),
            },
        }
    )
    _write_json(run_dir / "report.json", report)
    _write_report_md(run_dir / "report.md", report, kits)
    return report


def _seed_label(seed: Dict[str, Any]) -> str:
    parts = [str(seed.get("name") or "")]
    parts.extend(str(value) for value in (seed.get("genres") or [])[:3])
    parts.extend(str(value) for value in (seed.get("tags") or [])[:8])
    parts.extend(str(item.get("text") or "")[:100] for item in (seed.get("evidence_units") or [])[:4])
    return " | ".join(value for value in parts if value)[:600]


def _mechanical_seed_unit(unit: Dict[str, str]) -> bool:
    text = str(unit.get("text") or "").lower()
    if any(phrase in text for phrase in NON_MECHANICAL_PHRASES):
        return False
    if " may " in f" {text} " and any(value in text for value in ("power", "price", "fate", "trust")):
        return False
    return True


def _build_prompt(record: Dict[str, Any], active: List[Dict[str, Any]], round_index: int, beam_width: int) -> str:
    parents = "\n".join(f"{item['node_id']} | {item['label']}" for item in active)
    count_rule = f"Return {beam_width} different children for root." if round_index == 1 else "Return exactly one child for every parent."
    return (
        "CATEGORY: reusable player-facing game rules, actions, state, relationships, limits, or outcomes\n"
        f"GAME: {record.get('name')}\nROUND: {round_index}\nPARENTS:\n{parents}\n"
        "Move one conceptual step sideways from each parent. Suggest a tangent that is new but still categorically related. "
        "Do not return a synonym, genre, story topic, platform, presentation tag, implementation technology, or generic system name.\n"
        f"{count_rule}\nRETURN: parent_id | 2 to 6 word idea, one line each; no prose."
    )


def _parse_lines(response: ProviderResponse, parents: List[Dict[str, Any]], children_per_parent: int) -> List[Dict[str, str]]:
    if not response.ok:
        return []
    parent_ids = [item["node_id"] for item in parents]
    lines = [value.strip() for value in response.content.splitlines() if value.strip()]
    if len(lines) == 1 and "|" not in lines[0] and "," in lines[0]:
        lines = [value.strip() for value in lines[0].split(",") if value.strip()]
    output = []
    fallback_index = 0
    for line in lines[: max(1, len(parents) * children_per_parent)]:
        cleaned = re.sub(r"^\s*(?:[-*]|\d+[.)])\s*", "", line).strip().strip("`\"")
        if "|" in cleaned:
            possible_parent, label = [value.strip() for value in cleaned.split("|", 1)]
            parent_id = possible_parent if possible_parent in parent_ids else parent_ids[min(fallback_index, len(parent_ids) - 1)]
        else:
            parent_id = parent_ids[min(fallback_index, len(parent_ids) - 1)]
            label = cleaned
        output.append({"parent_id": parent_id, "label": label})
        fallback_index += 1
    return output


def _filter_proposals(
    proposals: List[Dict[str, str]], seen: set[str], round_index: int,
    record: Dict[str, Any], parents: List[Dict[str, Any]], evidence_units: List[Dict[str, str]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    accepted = []
    rejected = []
    parent_map = {item["node_id"]: item for item in parents}
    for index, proposal in enumerate(proposals, start=1):
        label = _clean_label(proposal.get("label"))
        label_id = slug(label)
        tokens = [value for value in label_id.split("-") if value]
        reason = None
        if not label_id or label_id in GENERIC_LABELS:
            reason = "empty-or-generic"
        elif not 1 <= len(tokens) <= 6:
            reason = "label-length"
        elif any(value in GENERIC_TOKENS for value in tokens):
            reason = "generic-token"
        elif label_id == slug(record.get("name")) or label_id == slug(record.get("source_id")):
            reason = "source-metadata"
        elif label_id in seen:
            reason = "exact-duplicate"
        node_id = f"r{round_index:02d}-{index:02d}-{label_id or 'invalid'}"
        parent = parent_map.get(str(proposal.get("parent_id")), {})
        lineage_ids = list(
            parent.get("lineage_evidence_source_ids")
            or parent.get("evidence_source_ids")
            or []
        )
        direct_ids = _matching_evidence_ids(label, evidence_units)
        record_value = {
            "schema_version": "rawg.chainstorm-node.v1",
            "node_id": node_id,
            "parent_id": proposal.get("parent_id"),
            "round": round_index,
            "label": label,
            "semantic_key": label_id,
            "source_id": record.get("source_id"),
            "source_hash": record.get("source_hash"),
            "evidence_source_ids": direct_ids,
            "lineage_evidence_source_ids": lineage_ids,
        }
        if reason:
            rejected.append({**record_value, "accepted": False, "reason": reason})
        else:
            seen.add(label_id)
            accepted.append({**record_value, "accepted": True, "reason": "shape-and-exact-uniqueness"})
    return accepted, rejected


def _clean_label(value: Any) -> str:
    label = " ".join(str(value or "").split()).strip(" .,:;|-")
    if "→" in label:
        label = label.rsplit("→", 1)[-1].strip()
    label = re.sub(r"^r\d{1,2}[-_ ]\d{1,2}(?:[-_ ]+)", "", label, flags=re.IGNORECASE)
    return label.strip(" .,:;|-")


def _matching_evidence_ids(label: str, units: List[Dict[str, str]]) -> List[str]:
    label_tokens = {
        value for value in slug(label).split("-")
        if len(value) >= 4 and value not in GENERIC_TOKENS and value not in EVIDENCE_STOPWORDS
    }
    output = []
    for unit in units:
        unit_tokens = {
            value for value in slug(unit.get("text")).split("-")
            if len(value) >= 4 and value not in EVIDENCE_STOPWORDS
        }
        if label_tokens & unit_tokens:
            output.append(unit["source_id"])
    return output


def _run_codex(run_dir: Path, record: Dict[str, Any], nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
    output_path = run_dir / "codex-kit-proposals.raw.txt"
    node_ids = [item["node_id"] for item in nodes]
    prompt = f"""
Act as the agentic KitUniverse architect. The local LFM only brainstormed rough tangent nodes.

READ:
- source evidence: {run_dir / 'evidence-seed.json'}
- chainstorm nodes: {run_dir / 'filtered-nodes.jsonl'}
- existing KitUniverse memory: memory.md
- use targeted `rg` checks in `/Users/crimsonwheeler/Documents/GitHub/NexusEngine` and `/Users/crimsonwheeler/Documents/GitHub/NexusEngine-ProtoKits` only for concepts you may approve

Turn the useful grounded or mechanically plausible nodes into zero to six NEW atomic reusable kit proposals. You may combine related nodes when one owned behavior requires them. Reject genre, story, presentation, metadata, unsupported, duplicate, or non-reusable ideas. Do not edit files and do not promote anything.

Every kit must contain: kit_id, name, domain, owned_behavior, inputs, outputs, evidence_node_ids, novelty_reason. Evidence IDs must come from this exact list: {json.dumps(node_ids)}.
`evidence_source_ids` are direct lexical evidence. `lineage_evidence_source_ids` preserve ancestry only and are not proof by themselves.

Return only:
{{"ok":true,"kits":[{{"kit_id":"kebab-id","name":"Name","domain":"domain","owned_behavior":"one behavior","inputs":[],"outputs":[],"evidence_node_ids":["exact-node-id"],"novelty_reason":"why"}}],"rejected_node_ids":[],"systemic_errors":[]}}
""".strip()
    command = [
        str(CODEX_BINARY), "exec", "--ephemeral", "--color", "never", "-C", str(Path.cwd()),
        "-s", "read-only", "-m", CODEX_MODEL, "-c", 'model_reasoning_effort="medium"',
        "-o", str(output_path), prompt,
    ]
    started = time.monotonic()
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=900, check=False)
        parsed = _parse_object(output_path.read_text(encoding="utf-8")) if result.returncode == 0 and output_path.exists() else {}
    except (OSError, subprocess.TimeoutExpired, ValueError, json.JSONDecodeError) as error:
        return {"ok": False, "kits": [], "error": str(error), "elapsed_seconds": round(time.monotonic() - started, 3)}
    valid_nodes = set(node_ids)
    kits = []
    for item in parsed.get("kits") or []:
        if not isinstance(item, dict):
            continue
        kit_id = slug(item.get("kit_id") or item.get("name"))
        evidence_ids = list(dict.fromkeys(str(value) for value in item.get("evidence_node_ids") or [] if str(value) in valid_nodes))
        if not kit_id or not evidence_ids or not str(item.get("owned_behavior") or "").strip():
            continue
        kits.append({
            "kit_id": kit_id,
            "name": str(item.get("name") or kit_id.replace("-", " ").title()),
            "domain": slug(item.get("domain")) or "unmapped",
            "owned_behavior": str(item.get("owned_behavior")),
            "inputs": [str(value) for value in item.get("inputs") or []],
            "outputs": [str(value) for value in item.get("outputs") or []],
            "evidence_node_ids": evidence_ids,
            "novelty_reason": str(item.get("novelty_reason") or ""),
        })
    return {
        **parsed,
        "ok": bool(parsed.get("ok")) and result.returncode == 0,
        "kits": kits[:6],
        "model": CODEX_MODEL,
        "returncode": result.returncode,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "stderr_tail": result.stderr[-2000:],
        "raw_output": str(output_path),
    }


def _build_requests(record: Dict[str, Any], kits: List[Dict[str, Any]], nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    node_map = {item["node_id"]: item for item in nodes}
    requests = []
    for kit in kits:
        evidence_nodes = [node_map[value] for value in kit["evidence_node_ids"] if value in node_map]
        requests.append(
            {
                "schema_version": "kit.build-request.v1",
                "source_id": f"rawg-chainstorm-{record.get('source_id')}-{kit['kit_id']}",
                "name": kit["name"],
                "description": kit["owned_behavior"],
                "constraints": ["render-agnostic", "idempotent", "snapshot-and-reset", "codex-reviewed"],
                "seed_domains": [kit["domain"]],
                "source_context": {
                    "dataset": record.get("dataset"),
                    "rawg_source_ids": [record.get("source_id")],
                    "source_hash": record.get("source_hash"),
                    "chainstorm_node_ids": kit["evidence_node_ids"],
                    "chainstorm_evidence_hash": stable_hash(evidence_nodes),
                    "promotion_level": "proposal-only",
                },
            }
        )
    return requests


def _parse_object(content: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", content, re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else content[content.find("{") : content.rfind("}") + 1]
    parsed = json.loads(candidate)
    if not isinstance(parsed, dict):
        raise ValueError("Codex output is not an object")
    return parsed


def _base_report(args: argparse.Namespace, run_id: str, run_dir: Path, record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": "rawg.chainstorm-report.v1",
        "workflow": "rawg-chainstorm",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "game_id": record.get("source_id"),
        "model": args.model,
        "context_length": args.context_length,
        "parallel": args.parallel,
        "rounds_requested": args.rounds,
        "time_budget_seconds": args.time_budget_seconds,
    }


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _write_jsonl(path: Path, values: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text("".join(json.dumps(value, sort_keys=True) + "\n" for value in values), encoding="utf-8")
    temporary.replace(path)


def _write_report_md(path: Path, report: Dict[str, Any], kits: List[Dict[str, Any]]) -> None:
    lines = [
        "# RAWG Chainstorm Report", "",
        f"- Game: `{report['game_id']}`",
        f"- Local rounds: `{report['rounds_completed']}/{report['rounds_requested']}`",
        f"- Local elapsed: `{report['local_elapsed_seconds']}s` / `{report['time_budget_seconds']}s`",
        f"- Accepted nodes: `{report['accepted_node_count']}`",
        f"- Codex kit proposals: `{report['kit_proposal_count']}`", "", "## Kits", "",
    ]
    lines.extend(f"- `{item['kit_id']}`: {item['owned_behavior']}" for item in kits)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
