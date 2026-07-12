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
from workflow_harnesses.rawg_capability_pipeline.source_adapter import normalize_rawg_record, stream_rawg_records
from .evidence_chain import evidence_packet_tokens, fact_ids_for_candidate, run_evidence_chain, seed_capability_candidates


DEFAULT_SOURCE_ROOT = Path("/Users/crimsonwheeler/Documents/GitHub/NexusRealtime-Ideas/games/rawg/chunks")
DEFAULT_BASE_URL = "http://10.0.0.137:1234/v1"
DEFAULT_MODEL = "lfm2.5-1.2b-instruct"
DEFAULT_RUN_ROOT = Path("runs/workflow-harnesses/rawg-domain-loop")

DOMAIN_TYPES = [
    "state-ownership-and-resources",
    "lifecycle-reset-and-persistence",
    "progression-rewards-and-power",
    "choice-dialogue-and-relationships",
    "multiplayer-party-and-session",
    "world-simulation-and-interaction",
    "combat-abilities-and-status",
    "unusual-game-specific-capabilities",
]

BANNED_EXACT = {
    "adventure", "capability", "domain", "engine", "engine-capability", "engine-features",
    "fantasy", "feature", "game", "game-mechanics", "gameplay", "genre", "mechanic",
    "metadata", "multiplayer", "platform", "platform-compatibility", "rpg", "singleplayer",
    "staging-capability", "strategy", "system", "world-mechanics",
}
BANNED_TOKENS = {"capability", "domain", "engine", "feature", "features", "game", "gameplay", "genre", "mechanic", "mechanics", "metadata", "platform", "staging", "system", "systems"}
NON_EVIDENCE_TOKENS = {
    "and", "apply", "atomic", "control", "core", "data", "dynamic", "event", "exact", "flow",
    "id", "kebab", "management", "manager", "mode", "new", "policy", "process", "service",
    "specific", "state", "support", "the", "tracking",
}
PLACEHOLDER_TOKENS = {"atomic", "candidate", "core", "data", "exact", "id", "item", "kebab", "new", "specific"}
PLACEHOLDER_RE = re.compile(r"(?:acts?|candidate|domain|item|example)[-_]?\d+$")

ACT_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "domain_act",
        "strict": "true",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "candidates": {
                    "type": "array",
                    "minItems": 0,
                    "maxItems": 5,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "label": {"type": "string"},
                        },
                        "required": ["label"],
                    },
                }
            },
            "required": ["candidates"],
        },
    },
}

REVIEW_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "domain_review",
        "strict": "true",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "decisions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "candidate_id": {"type": "string"},
                            "accepted": {"type": "boolean"},
                            "reason": {"type": "string"},
                        },
                        "required": ["candidate_id", "accepted", "reason"],
                    },
                },
                "continue": {"type": "boolean"},
            },
            "required": ["decisions", "continue"],
        },
    },
}


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--game-id")
    parser.add_argument("--record-file", type=Path)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--context-length", type=int, default=24576)
    parser.add_argument("--parallel", type=int, default=1)
    parser.add_argument("--max-passes", type=int, default=8)
    parser.add_argument("--max-empty-passes", type=int, default=2)
    parser.add_argument("--act-temperature", type=float, default=0.8)
    parser.add_argument("--review-temperature", type=float, default=0.1)
    parser.add_argument("--max-tokens", type=int, default=1200)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--min-accepted", type=int, default=1)
    parser.add_argument("--codex-review", action="store_true")
    parser.add_argument("--skip-evidence-chain", action="store_true")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-rawg-domain-loop",
        description="Run a bounded evidence-gated act-review domain discovery loop.",
    )
    configure_parser(parser)
    return run_from_namespace(parser.parse_args(argv))


def run_from_namespace(args: argparse.Namespace) -> int:
    report = run_domain_loop(args)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


def run_domain_loop(args: argparse.Namespace) -> Dict[str, Any]:
    if bool(args.game_id) == bool(args.record_file):
        raise ValueError("provide exactly one of --game-id or --record-file")
    if not 1 <= args.max_passes <= len(DOMAIN_TYPES):
        raise ValueError(f"--max-passes must be between 1 and {len(DOMAIN_TYPES)}")
    if not 1 <= args.max_empty_passes <= args.max_passes:
        raise ValueError("--max-empty-passes must be between 1 and max passes")
    if args.parallel != 1:
        raise ValueError("this indexed per-record loop currently requires --parallel 1")

    record = load_record(args.record_file, args.game_id, args.source_root)
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
    run_dir = args.run_root / run_id
    passes_dir = run_dir / "passes"
    passes_dir.mkdir(parents=True, exist_ok=True)
    write_json(run_dir / "source-record.json", record)

    provider = LMStudioProvider(args.base_url, args.model, args.timeout_seconds)
    health = provider.health()
    load = provider.ensure_loaded(context_length=args.context_length, parallel=args.parallel)
    write_json(run_dir / "provider-preflight.json", {"health": health, "load": load})
    if not health.get("ok") or not load.get("ok") or not load.get("config_matches", True):
        report = base_report(args, run_id, run_dir, record)
        report.update({"ok": False, "status": "hold", "reason": "provider-preflight-failed", "provider": {"health": health, "load": load}})
        write_json(run_dir / "report.json", report)
        return report

    evidence = {
        "ok": True,
        "calls_completed": 0,
        "accepted_facts": [],
        "reason": "skipped-by-flag",
    }
    if not args.skip_evidence_chain:
        evidence = run_evidence_chain(provider, record, run_dir, args.max_tokens, args.review_temperature)
        if not evidence.get("ok"):
            report = base_report(args, run_id, run_dir, record)
            report.update(
                {
                    "ok": False,
                    "status": "complete",
                    "reason": "mechanical-evidence-chain-failed",
                    "mechanical_evidence": evidence,
                    "calls_completed": evidence.get("calls_completed", 0),
                }
            )
            write_json(run_dir / "report.json", report)
            return report
    evidence_facts = evidence.get("accepted_facts") or []

    state: Dict[str, Any] = {
        "schema_version": "rawg.domain-loop-state.v1",
        "original_goal": "discover exhaustive evidence-bound atomic reusable capabilities for later semantic merging",
        "game_id": record["source_id"],
        "accepted": [],
        "rejected": {},
        "explored_types": [],
        "next_domain_type": DOMAIN_TYPES[0],
        "next_action": f"act:{DOMAIN_TYPES[0]}",
        "open_issues": [],
        "last_review": None,
        "mechanical_evidence_packet": evidence.get("packet_path"),
        "mechanical_evidence_hash": evidence.get("packet_hash"),
        "mechanical_fact_count": len(evidence_facts),
        "empty_passes": 0,
        "pass": 0,
        "stop_criteria": {"max_passes": args.max_passes, "max_empty_passes": args.max_empty_passes},
    }
    write_json(run_dir / "loop-state.json", state)
    source_tokens = evidence_packet_tokens(evidence_facts) if evidence_facts else metadata_tokens(record)
    started = time.time()
    calls = int(evidence.get("calls_completed") or 0)
    fact_seed = {"calls_completed": 0, "proposals": [], "reason": "no-evidence-facts"}
    if evidence_facts:
        fact_seed = seed_capability_candidates(provider, evidence_facts, run_dir)
        calls += int(fact_seed.get("calls_completed") or 0)
        seed_gated, seed_rejections = gate_candidates(
            fact_seed.get("proposals") or [], source_tokens, set(), set(), evidence_facts, None
        )
        for item in seed_rejections:
            state["rejected"][item["candidate_id"]] = f"fact-seed:{item['reason']}"
        for item in seed_gated:
            item.update(
                {
                    "domain_type": "fact-to-capability-seed",
                    "discovery_pass": 0,
                    "source_id": record["source_id"],
                    "source_hash": record["source_hash"],
                }
            )
            if item["candidate_id"] not in {value["candidate_id"] for value in state["accepted"]}:
                state["accepted"].append(item)
        fact_seed.update({"accepted": seed_gated, "rejected": seed_rejections})
        write_json(run_dir / "mechanical-evidence-chain" / "06-fact-seed-gate.json", fact_seed)
        write_json(run_dir / "loop-state.json", state)
    stop_reason = "max-passes"

    for pass_index, domain_type in enumerate(DOMAIN_TYPES[: args.max_passes], start=1):
        pass_dir = passes_dir / f"pass-{pass_index:02d}-{domain_type}"
        pass_dir.mkdir(parents=True, exist_ok=True)
        state["pass"] = pass_index
        state["next_domain_type"] = domain_type
        act_prompt = build_act_prompt(record, state, domain_type, evidence_facts)
        act_response = provider.chat(
            messages=[
                {"role": "system", "content": "Act as a high-recall capability scout. Return only the requested JSON."},
                {"role": "user", "content": act_prompt},
            ],
            temperature=args.act_temperature,
            max_tokens=args.max_tokens,
            response_format=ACT_SCHEMA,
        )
        calls += 1
        proposals, act_error = parse_act(act_response)
        write_json(pass_dir / "act.json", response_artifact(act_prompt, act_response, {"parsed": proposals, "error": act_error}))

        gated, gate_rejections = gate_candidates(
            proposals, source_tokens, {item["candidate_id"] for item in state["accepted"]}, set(state["rejected"]), evidence_facts, domain_type
        )
        write_json(pass_dir / "evidence-gate.json", {"accepted_for_review": gated, "rejected": gate_rejections})
        for item in gate_rejections:
            state["rejected"][item["candidate_id"]] = item["reason"]

        kept: List[Dict[str, Any]] = []
        review_error = None
        decision = None
        if gated:
            review_prompt = build_review_prompt(record, state, domain_type, gated, evidence_facts)
            review_response = provider.chat(
                messages=[
                    {"role": "system", "content": "Review candidates conservatively. Return only the requested JSON."},
                    {"role": "user", "content": review_prompt},
                ],
                temperature=args.review_temperature,
                max_tokens=min(args.max_tokens, 800),
                response_format=REVIEW_SCHEMA,
            )
            calls += 1
            decision, review_error = parse_review(review_response, [item["candidate_id"] for item in gated])
            write_json(pass_dir / "review.json", response_artifact(review_prompt, review_response, {"parsed": decision, "error": review_error}))
            if decision:
                keep_ids = set(decision["keep"])
                kept = [item for item in gated if item["candidate_id"] in keep_ids]
                for rejected in decision["reject"]:
                    state["rejected"][rejected["candidate_id"]] = rejected["reason"]
            else:
                for item in gated:
                    state["rejected"][item["candidate_id"]] = f"invalid-review:{review_error}"
        else:
            write_json(pass_dir / "review.json", {"skipped": True, "reason": "no-candidates-passed-evidence-gate"})

        for item in kept:
            item.update({"domain_type": domain_type, "discovery_pass": pass_index, "source_id": record["source_id"], "source_hash": record["source_hash"]})
            state["accepted"].append(item)
        state["explored_types"].append(domain_type)
        state["empty_passes"] = 0 if kept else state["empty_passes"] + 1
        state["next_domain_type"] = DOMAIN_TYPES[pass_index] if pass_index < len(DOMAIN_TYPES) else None
        state["next_action"] = f"act:{state['next_domain_type']}" if state["next_domain_type"] else "final-review"
        state["last_review"] = decision or {"skipped": not gated, "error": review_error}
        state["open_issues"] = [review_error] if review_error else []
        write_json(run_dir / "loop-state.json", state)
        if pass_index >= min(4, args.max_passes) and state["empty_passes"] >= args.max_empty_passes:
            stop_reason = "consecutive-empty-passes"
            break

    candidates_path = run_dir / "accepted-candidates.json"
    write_json(candidates_path, state["accepted"])
    codex = {"ok": True, "skipped": True, "reason": "not-requested"}
    if args.codex_review and state["accepted"]:
        codex = run_codex_review(run_dir, record, candidates_path, [item["candidate_id"] for item in state["accepted"]])
    write_json(run_dir / "codex-review.json", codex)
    accepted_count = len(state["accepted"])
    codex_accepted_ids = {
        item["candidate_id"]
        for item in codex.get("decisions") or []
        if isinstance(item, dict) and item.get("accepted") is True
    }
    codex_approved = [item for item in state["accepted"] if item["candidate_id"] in codex_accepted_ids] if args.codex_review else []
    write_json(run_dir / "codex-approved-candidates.json", codex_approved)
    report = base_report(args, run_id, run_dir, record)
    report.update(
        {
            "ok": accepted_count >= args.min_accepted and codex.get("ok", False) and (not args.codex_review or len(codex_approved) >= args.min_accepted),
            "status": "complete",
            "stop_reason": stop_reason,
            "passes_completed": state["pass"],
            "calls_completed": calls,
            "accepted_count": accepted_count,
            "codex_approved_count": len(codex_approved),
            "codex_approved_candidates": codex_approved,
            "rejected_count": len(state["rejected"]),
            "accepted_candidates": state["accepted"],
            "codex_review": codex,
            "mechanical_evidence": evidence,
            "fact_capability_seed": fact_seed,
            "elapsed_seconds": round(time.time() - started, 3),
        }
    )
    write_json(run_dir / "report.json", report)
    return report


def load_record(record_file: Optional[Path], game_id: Optional[str], source_root: Path) -> Dict[str, Any]:
    if record_file:
        raw = json.loads(record_file.read_text(encoding="utf-8"))
        if raw.get("schema_version") == "rawg.source.v1":
            return raw
        return normalize_rawg_record(raw, record_file, 1, "domain-loop-input")
    wanted = slug(game_id)
    wanted_compact = re.sub(r"[^a-z0-9]", "", str(game_id or "").lower())
    for record, _, _ in stream_rawg_records(source_root, "domain-loop-input"):
        keys = {slug(record.get("source_id")), slug(record.get("name"))}
        compact_keys = {re.sub(r"[^a-z0-9]", "", str(value or "").lower()) for value in (record.get("source_id"), record.get("name"))}
        if wanted in keys or wanted_compact in compact_keys:
            return record
    raise FileNotFoundError(f"RAWG game not found: {game_id}")


def build_act_prompt(record: Dict[str, Any], state: Dict[str, Any], domain_type: str, evidence_facts: List[Dict[str, Any]]) -> str:
    accepted = [item["candidate_id"] for item in state["accepted"]][-80:]
    rejected = list(state["rejected"].items())[-40:]
    return f"""
Find 3 to 5 NEW atomic reusable game-engine capabilities of type: {domain_type}.

This is an ACT pass in a continuing discovery loop. The candidates will be evidence-gated and reviewed next. Return short source-specific capability labels only; the harness owns IDs. Different owned state, lifecycle, rule, relationship, input, or output must remain separate for later semantic merging.

Do not return genres, themes, platforms, graphics engines, generic systems, or placeholders. Do not repeat accepted or rejected IDs. A candidate must contain meaningful words that occur in the metadata. If fewer than 3 grounded candidates exist, return only those. It is valid to return none.

ACCEPTED SO FAR: {json.dumps(accepted)}
RECENT REJECTIONS: {json.dumps(rejected)}
EXPLICIT SOURCE TERMS AVAILABLE FOR CANDIDATE NAMES: {json.dumps(sorted(source_terms_for_loop(record, evidence_facts)))}
ACCEPTED MECHANICAL FACTS: {json.dumps(evidence_facts, ensure_ascii=False) if evidence_facts else 'not available; use metadata directly'}
GAME METADATA FALLBACK: {json.dumps(compact_record(record), ensure_ascii=False) if not evidence_facts else 'omitted because the evidence chain succeeded'}

The response schema is supplied separately. Populate labels with source-specific values only. Never copy fact IDs, prior rejection text, schema descriptions, domain-type text, or placeholder-like values.
""".strip()


def build_review_prompt(record: Dict[str, Any], state: Dict[str, Any], domain_type: str, gated: List[Dict[str, Any]], evidence_facts: List[Dict[str, Any]]) -> str:
    return f"""
Review every proposed capability for the original goal: exhaustive evidence-bound atomic reusable capabilities for later semantic merging.

DOMAIN TYPE: {domain_type}
CURRENT ACCEPTED: {json.dumps([item['candidate_id'] for item in state['accepted']])}
PROPOSALS: {json.dumps(gated)}
ACCEPTED MECHANICAL FACTS: {json.dumps(evidence_facts, ensure_ascii=False) if evidence_facts else json.dumps(compact_record(record), ensure_ascii=False)}

KEEP only candidates that are explicitly supported, mechanically meaningful, atomic, reusable, and not a broad genre/theme/platform. REJECT unsupported inference, placeholders, renamed duplicates, story-only nouns, and presentation-only concepts. Decide every proposal exactly once. Continue is true only if other unexplored domain types may still yield grounded candidates.

The response schema is supplied separately. Return exactly one typed decision per proposal. Use only proposal IDs shown above; do not use labels and do not invent an ID.
""".strip()


def compact_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "source_id": record.get("source_id"),
        "name": record.get("name"),
        "description": str(record.get("description") or "")[:7000],
        "genres": record.get("genres") or [],
        "tags": record.get("tags") or [],
        "platforms": record.get("platforms") or [],
    }


def parse_object(content: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", content, re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else content[content.find("{") : content.rfind("}") + 1]
    parsed = json.loads(candidate)
    if not isinstance(parsed, dict):
        raise ValueError("response is not an object")
    return parsed


def parse_act(response: ProviderResponse) -> Tuple[List[Dict[str, str]], Optional[str]]:
    if not response.ok:
        return [], response.error or "provider-failed"
    try:
        parsed = parse_object(response.content)
        output = []
        for value in (parsed.get("candidates") or [])[:5]:
            if not isinstance(value, dict):
                continue
            label = str(value.get("label") or "").strip()
            candidate_id = slug(label)
            if candidate_id:
                output.append({"candidate_id": candidate_id, "label": label})
        return output, None
    except (ValueError, json.JSONDecodeError) as error:
        return [], str(error)


def parse_review(response: ProviderResponse, candidate_ids: List[str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not response.ok:
        return None, response.error or "provider-failed"
    try:
        parsed = parse_object(response.content)
        expected = set(candidate_ids)
        decisions = [value for value in parsed.get("decisions") or [] if isinstance(value, dict) and slug(value.get("candidate_id")) in expected]
        decided = [slug(value.get("candidate_id")) for value in decisions]
        if sorted(decided) != sorted(candidate_ids) or len(decided) != len(set(decided)) or not all(isinstance(value.get("accepted"), bool) for value in decisions):
            raise ValueError("review must decide every candidate exactly once")
        keep = [slug(value.get("candidate_id")) for value in decisions if value.get("accepted") is True]
        reject = [value for value in decisions if value.get("accepted") is False]
        return {
            "keep": keep,
            "reject": [{"candidate_id": slug(value.get("candidate_id")), "reason": str(value.get("reason") or "review-rejected")} for value in reject],
            "continue": bool(parsed.get("continue")),
        }, None
    except (ValueError, json.JSONDecodeError) as error:
        return None, str(error)


def gate_candidates(
    proposals: List[Dict[str, str]], source_tokens: set[str], accepted: set[str], rejected: set[str], evidence_facts: Optional[List[Dict[str, Any]]] = None, domain_type: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    passed = []
    failures = []
    seen = set()
    for proposal in proposals:
        candidate_id = slug(proposal.get("candidate_id"))
        tokens = [value for value in candidate_id.split("-") if value]
        substantive = [value for value in tokens if value not in NON_EVIDENCE_TOKENS and value not in BANNED_TOKENS]
        reason = None
        if not candidate_id or candidate_id in BANNED_EXACT or PLACEHOLDER_RE.fullmatch(candidate_id):
            reason = "generic-or-placeholder"
        elif domain_type and candidate_id == slug(domain_type):
            reason = "copied-domain-type"
        elif any(value in PLACEHOLDER_TOKENS for value in tokens) and any(value.isdigit() for value in tokens):
            reason = "generic-or-placeholder"
        elif candidate_id in accepted or candidate_id in rejected or candidate_id in seen:
            reason = "already-seen"
        elif not 2 <= len(tokens) <= 8:
            reason = "not-atomic-name-shape"
        elif any(value in BANNED_TOKENS for value in tokens):
            reason = "banned-broad-token"
        else:
            matched = sorted(set(substantive) & source_tokens)
            if not matched:
                reason = "no-source-term-overlap"
        if reason:
            failures.append({"candidate_id": candidate_id or "empty", "reason": reason})
            continue
        seen.add(candidate_id)
        declared_fact_ids = [value for value in proposal.get("evidence_fact_ids") or [] if value in {item["fact_id"] for item in evidence_facts or []}]
        fact_ids = declared_fact_ids or fact_ids_for_candidate(candidate_id, evidence_facts or [])
        if evidence_facts and not fact_ids:
            failures.append({"candidate_id": candidate_id, "reason": "no-mechanical-fact-link"})
            continue
        passed.append({**proposal, "candidate_id": candidate_id, "matched_source_terms": matched, "evidence_fact_ids": fact_ids, "gate_hash": stable_hash([candidate_id, matched, fact_ids])})
    return passed, failures


def metadata_tokens(record: Dict[str, Any]) -> set[str]:
    value = " ".join(
        [
            str(record.get("name") or ""),
            str(record.get("description") or ""),
            " ".join(record.get("genres") or []),
            " ".join(record.get("tags") or []),
            " ".join(record.get("platforms") or []),
        ]
    )
    output = set()
    for token in slug(value).split("-"):
        if len(token) < 3:
            continue
        output.add(token)
        if token.endswith("ies") and len(token) > 4:
            output.add(token[:-3] + "y")
        elif token.endswith("s") and not token.endswith("ss") and len(token) > 3:
            output.add(token[:-1])
    return output


def salient_source_terms(record: Dict[str, Any]) -> List[str]:
    ordered: List[str] = []
    values = [
        *(record.get("tags") or []),
        *(record.get("genres") or []),
        str(record.get("description") or ""),
    ]
    for value in values:
        for token in slug(value).split("-"):
            if len(token) < 4 or token in NON_EVIDENCE_TOKENS or token in BANNED_TOKENS or token in ordered:
                continue
            ordered.append(token)
            if len(ordered) >= 120:
                return ordered
    return ordered


def source_terms_for_loop(record: Dict[str, Any], evidence_facts: List[Dict[str, Any]]) -> set[str]:
    if evidence_facts:
        return evidence_packet_tokens(evidence_facts) - NON_EVIDENCE_TOKENS - BANNED_TOKENS
    return set(salient_source_terms(record))


def response_artifact(prompt: str, response: ProviderResponse, extra: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "prompt": prompt,
        "response": response.to_dict(),
        **extra,
    }


def run_codex_review(run_dir: Path, record: Dict[str, Any], candidates_path: Path, candidate_ids: List[str]) -> Dict[str, Any]:
    raw_path = run_dir / "codex-review.raw.txt"
    prompt = f"""
Review staged RAWG domain candidates in read-only mode.

READ source record: {run_dir / 'source-record.json'}
READ accepted mechanical evidence: {run_dir / 'mechanical-evidence-chain' / 'mechanical-evidence.json'}
READ candidates: {candidates_path}

Accept only atomic, reusable, mechanically meaningful candidates directly grounded in the source. Reject broad genres, story-only nouns, platforms, presentation details, unsupported inference, and renamed duplicates. Do not merge yet and do not edit files.

Return only {{"ok":true,"decisions":[{{"candidate_id":"exact-id","accepted":true,"reasons":[]}}],"systemic_errors":[]}}.
Decide every ID exactly once: {json.dumps(candidate_ids)}
""".strip()
    command = [
        str(CODEX_BINARY), "exec", "--ephemeral", "--color", "never", "-C", str(Path.cwd()),
        "-s", "read-only", "-m", CODEX_MODEL, "-c", 'model_reasoning_effort="medium"', "-o", str(raw_path), prompt,
    ]
    started = time.time()
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=900, check=False)
        parsed = parse_object(raw_path.read_text(encoding="utf-8")) if result.returncode == 0 and raw_path.exists() else {}
    except (OSError, subprocess.TimeoutExpired, ValueError, json.JSONDecodeError) as error:
        return {"ok": False, "error": str(error), "elapsed_seconds": round(time.time() - started, 3)}
    decisions = parsed.get("decisions") or []
    ids = [slug(item.get("candidate_id")) for item in decisions if isinstance(item, dict)]
    complete = sorted(ids) == sorted(candidate_ids) and len(ids) == len(set(ids))
    typed = all(isinstance(item.get("accepted"), bool) for item in decisions if isinstance(item, dict))
    return {
        **parsed,
        "ok": bool(complete and typed),
        "complete": complete,
        "typed": typed,
        "model": CODEX_MODEL,
        "returncode": result.returncode,
        "elapsed_seconds": round(time.time() - started, 3),
        "stderr_tail": result.stderr[-3000:],
        "raw_output": str(raw_path),
    }


def base_report(args: argparse.Namespace, run_id: str, run_dir: Path, record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": "rawg.domain-loop-report.v1",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "game_id": record.get("source_id"),
        "source_hash": record.get("source_hash"),
        "model": args.model,
        "base_url": args.base_url,
        "context_length": args.context_length,
        "parallel": args.parallel,
        "max_passes": args.max_passes,
    }


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
