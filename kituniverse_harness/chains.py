from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .providers import DEFAULT_BASE_URL, DEFAULT_MODEL, LMStudioProvider


CHAIN_NAME = "chain-ask-for-domain-list"


@dataclass
class ChainConfig:
    topic: str
    base_url: str = DEFAULT_BASE_URL
    model: str = DEFAULT_MODEL
    run_root: Path = Path("runs")
    max_retries: int = 1
    temperature: float = 0.1
    max_tokens: int = 2000
    timeout_seconds: int = 90


def run_chain_ask_for_domain_list(config: ChainConfig) -> Dict[str, Any]:
    run_id = time.strftime("%Y%m%d-%H%M%S")
    run_dir = config.run_root / CHAIN_NAME / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    provider = LMStudioProvider(
        base_url=config.base_url,
        model=config.model,
        timeout_seconds=config.timeout_seconds,
    )
    health = provider.health()
    _write_json(run_dir / "provider-health.json", health)
    if not health.get("ok"):
        report = {
            "ok": False,
            "chain": CHAIN_NAME,
            "run_id": run_id,
            "run_dir": str(run_dir),
            "error": "provider health failed",
            "health": health,
        }
        _write_json(run_dir / "report.json", report)
        return report

    step1 = _run_json_step(
        provider=provider,
        run_dir=run_dir,
        step_name="01-seed-domain-candidates",
        prompt=_step1_prompt(config.topic),
        required_key="domains",
        config=config,
        validator=lambda parsed: _validate_harness_domain_relevance(
            _normalize_domains(parsed.get("domains", []) if parsed else []),
            min_count=6,
        ),
    )
    if not step1["ok"]:
        return _final_report(run_dir, run_id, False, [step1], None)

    step2 = _run_json_step(
        provider=provider,
        run_dir=run_dir,
        step_name="02-expand-and-group-domains",
        prompt=_step2_prompt(config.topic, step1["parsed"]),
        required_key="domains",
        config=config,
        validator=lambda parsed: _validate_harness_domain_relevance(
            _normalize_domains(parsed.get("domains", []) if parsed else []),
            min_count=5,
        ),
    )
    if not step2["ok"]:
        return _final_report(run_dir, run_id, False, [step1, step2], None)

    step3 = _run_json_step(
        provider=provider,
        run_dir=run_dir,
        step_name="03-final-domain-list",
        prompt=_step3_prompt(config.topic, step2["parsed"]),
        required_key="domains",
        config=config,
        validator=lambda parsed: _validate_final_domains(
            _normalize_domains(parsed.get("domains", []) if parsed else [])
        ),
    )
    final_domains = _normalize_domains(step3.get("parsed", {}).get("domains", []))
    final_validation_error = _validate_final_domains(final_domains)
    ok = step3["ok"] and final_validation_error is None
    final = {"topic": config.topic, "domains": final_domains}
    _write_json(run_dir / "final-domains.json", final)
    return _final_report(
        run_dir,
        run_id,
        ok,
        [step1, step2, step3],
        final,
        final_validation_error,
    )


def _run_json_step(
    provider: LMStudioProvider,
    run_dir: Path,
    step_name: str,
    prompt: str,
    required_key: str,
    config: ChainConfig,
    validator: Optional[Callable[[Optional[Dict[str, Any]]], Optional[str]]] = None,
) -> Dict[str, Any]:
    system = (
        "You are a chain harness worker. Return only one JSON object. "
        "No markdown. No prose outside JSON."
    )
    attempts: List[Dict[str, Any]] = []
    last_error = ""
    for attempt_index in range(config.max_retries + 1):
        step_prompt = prompt
        if attempt_index:
            step_prompt = (
                f"{prompt}\n\nPrevious output failed validation. "
                f"Failure: {last_error}. Return only valid JSON with required key "
                f"`{required_key}` and no placeholder values."
            )
        response = provider.chat(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": step_prompt},
            ],
            temperature=config.temperature,
            max_tokens=config.max_tokens,
        )
        parsed, parse_error = _extract_json_object(response.content)
        validation_error = _validate_required_list(parsed, required_key)
        if validation_error is None and validator is not None:
            validation_error = validator(parsed)
        ok = response.ok and parsed is not None and validation_error is None
        last_error = parse_error or validation_error or ""
        attempt = {
            "attempt": attempt_index + 1,
            "ok": ok,
            "provider": response.to_dict(),
            "parsed": parsed,
            "parse_error": parse_error,
            "validation_error": validation_error,
        }
        attempts.append(attempt)
        _write_json(run_dir / f"{step_name}-attempt-{attempt_index + 1}.json", attempt)
        if ok:
            result = {
                "ok": True,
                "step": step_name,
                "attempts": attempts,
                "parsed": parsed,
            }
            _write_json(run_dir / f"{step_name}.json", result)
            return result

    result = {
        "ok": False,
        "step": step_name,
        "attempts": attempts,
        "parsed": attempts[-1].get("parsed") if attempts else None,
        "error": "step failed validation",
    }
    _write_json(run_dir / f"{step_name}.json", result)
    return result


def _step1_prompt(topic: str) -> str:
    return f"""
Create an initial domain list for: {topic}

This is about the harness software itself, not business apps, cloud products, payments, privacy law, buckets, cron jobs, or webhooks.

Return only a JSON object with key "domains".
Each domain object needs: name, purpose, reason.

Rules:
- include 6 to 10 domains
- names must be lowercase kebab-case
- each domain must describe a reusable capability area, not a one-off task
- do not use placeholder values such as "...", "domain-name", or "one sentence"
- at least 5 names must directly mention harness concepts such as provider, prompt, chain, validation, artifact, schema, retry, model, context, run, adapter, orchestration, output, or domain
""".strip()


def _step2_prompt(topic: str, previous: Dict[str, Any]) -> str:
    return f"""
Take this initial domain output and improve it for: {topic}

Initial output:
{json.dumps(previous, indent=2)}

This is about the harness software itself, not business apps, cloud products, payments, privacy law, buckets, cron jobs, or webhooks.

Return only a JSON object with key "domains".
Each domain object needs: name, purpose, inputs, outputs.

Rules:
- merge duplicates
- add missing core domains
- keep 5 to 8 strongest domains
- names must be lowercase kebab-case
- do not use placeholder values such as "...", "domain-name", "input", "output", or "one sentence"
- at least 5 names must directly mention harness concepts such as provider, prompt, chain, validation, artifact, schema, retry, model, context, run, adapter, orchestration, output, or domain
""".strip()


def _step3_prompt(topic: str, previous: Dict[str, Any]) -> str:
    return f"""
Finalize a clean domain list for: {topic}

Expanded output:
{json.dumps(previous, indent=2)}

This is about the harness software itself, not business apps, cloud products, payments, privacy law, buckets, cron jobs, or webhooks.

Return only a JSON object with key "domains".
Each domain object needs: name, purpose, acceptance.

Rules:
- exactly 5 final domains
- use exactly these names: provider-adapter, prompt-step-contract, chain-orchestration, output-validation, artifact-ledger
- each acceptance list must have 2 short criteria
- each purpose must explain the domain's role in the chain harness
- prefer domains like provider-adapter, prompt-step-contract, chain-orchestration, output-validation, artifact-ledger when they fit
- no prose outside JSON
- do not use placeholder values such as "...", "domain-name", "one sentence", or "testable criterion"
""".strip()


def _extract_json_object(content: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return parsed, None
        return None, "top-level JSON was not an object"
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", content, flags=re.DOTALL)
    if not match:
        return None, "no JSON object found"
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError as exc:
        return None, str(exc)
    if not isinstance(parsed, dict):
        return None, "extracted JSON was not an object"
    return parsed, None


def _validate_required_list(parsed: Optional[Dict[str, Any]], key: str) -> Optional[str]:
    if parsed is None:
        return "missing parsed JSON"
    value = parsed.get(key)
    if not isinstance(value, list):
        return f"`{key}` must be a list"
    if not value:
        return f"`{key}` must not be empty"
    placeholder_error = _find_placeholder_error(value)
    if placeholder_error:
        return placeholder_error
    return None


def _find_placeholder_error(value: Any) -> Optional[str]:
    banned = {
        "...",
        "domain-name",
        "one sentence",
        "input",
        "output",
        "testable criterion",
        "testable query",
    }

    def walk(item: Any) -> Optional[str]:
        if isinstance(item, str):
            normalized = item.strip().lower()
            if normalized in banned:
                return f"placeholder value found: {item}"
            if normalized.startswith("one sentence"):
                return f"placeholder-like value found: {item}"
        if isinstance(item, list):
            for child in item:
                found = walk(child)
                if found:
                    return found
        if isinstance(item, dict):
            for child in item.values():
                found = walk(child)
                if found:
                    return found
        return None

    return walk(value)


def _validate_final_domains(domains: List[Dict[str, Any]]) -> Optional[str]:
    relevance_error = _validate_harness_domain_relevance(domains, min_count=5)
    if relevance_error:
        return relevance_error
    if len(domains) != 5:
        return f"expected exactly 5 final domains, got {len(domains)}"
    expected_names = {
        "provider-adapter",
        "prompt-step-contract",
        "chain-orchestration",
        "output-validation",
        "artifact-ledger",
    }
    actual_names = {str(domain.get("name", "")).strip() for domain in domains}
    if actual_names != expected_names:
        return f"expected final names {sorted(expected_names)}, got {sorted(actual_names)}"
    for domain in domains:
        name = str(domain.get("name", "")).strip()
        purpose = str(domain.get("purpose", "")).strip()
        acceptance = domain.get("acceptance")
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", name):
            return f"invalid domain name: {name}"
        if len(purpose.split()) < 6:
            return f"purpose too short for {name}: {purpose}"
        if not isinstance(acceptance, list) or len(acceptance) != 2:
            return f"{name} must have exactly 2 acceptance criteria"
        for criterion in acceptance:
            if not isinstance(criterion, str) or len(criterion.split()) < 2:
                return f"weak acceptance criterion for {name}: {criterion}"
    return _find_placeholder_error(domains)


def _validate_harness_domain_relevance(
    domains: List[Dict[str, Any]],
    min_count: int,
) -> Optional[str]:
    if len(domains) < min_count:
        return f"expected at least {min_count} harness domains, got {len(domains)}"

    forbidden_terms = {
        "cloud",
        "storage",
        "payment",
        "gdpr",
        "s3",
        "bucket",
        "webhook",
        "cron",
        "crm",
        "transaction",
    }
    harness_terms = {
        "provider",
        "prompt",
        "chain",
        "validation",
        "artifact",
        "schema",
        "retry",
        "model",
        "context",
        "run",
        "adapter",
        "orchestration",
        "output",
        "domain",
        "step",
    }
    relevant_count = 0
    for domain in domains:
        text = " ".join(str(value).lower() for value in domain.values())
        found_forbidden = sorted(term for term in forbidden_terms if term in text)
        if found_forbidden:
            return f"unrelated business/domain terms found: {', '.join(found_forbidden)}"
        name = str(domain.get("name", "")).lower()
        if any(term in name for term in harness_terms):
            relevant_count += 1
    if relevant_count < min_count:
        return f"expected at least {min_count} harness-relevant names, got {relevant_count}"
    return None


def _normalize_domains(domains: Any) -> List[Dict[str, Any]]:
    if not isinstance(domains, list):
        return []
    normalized = []
    seen = set()
    for item in domains:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip().lower()
        if not name or name in seen:
            continue
        seen.add(name)
        normalized.append(item)
    return normalized


def _final_report(
    run_dir: Path,
    run_id: str,
    ok: bool,
    steps: List[Dict[str, Any]],
    final: Optional[Dict[str, Any]],
    final_validation_error: Optional[str] = None,
) -> Dict[str, Any]:
    report = {
        "ok": ok,
        "chain": CHAIN_NAME,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "steps": [{"step": step["step"], "ok": step["ok"]} for step in steps],
        "final": final,
        "final_validation_error": final_validation_error,
    }
    _write_json(run_dir / "report.json", report)
    _write_markdown_report(run_dir / "report.md", report)
    return report


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_markdown_report(path: Path, report: Dict[str, Any]) -> None:
    lines = [
        f"# {report['chain']} Report",
        "",
        f"- ok: `{str(report['ok']).lower()}`",
        f"- run_id: `{report['run_id']}`",
        f"- run_dir: `{report['run_dir']}`",
        "",
        "## Steps",
        "",
    ]
    for step in report["steps"]:
        lines.append(f"- {step['step']}: `{str(step['ok']).lower()}`")
    if report.get("final"):
        lines.extend(["", "## Final Domains", ""])
        for domain in report["final"]["domains"]:
            lines.append(f"- `{domain.get('name')}`: {domain.get('purpose', '')}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
