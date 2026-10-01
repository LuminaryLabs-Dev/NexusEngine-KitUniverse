from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from kituniverse_harness.providers import LMStudioProvider

from .contracts import ContractError, validate_domains, validate_ideas, validate_taxonomy
from .prompts import SYSTEM, domains_prompt, ideas_prompt, repair_prompt, taxonomy_prompt


DEFAULT_BASE_URL = "http://10.0.0.38:1234/v1"
DEFAULT_MODEL = "gemma-4-12b-obliterated"


@dataclass(frozen=True)
class ChainConfig:
    seed: str
    base_url: str = DEFAULT_BASE_URL
    model: str = DEFAULT_MODEL
    run_root: Path = Path("runs/gemma/chain-harness")
    max_retries: int = 1
    timeout_seconds: int = 120


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _append_jsonl(path: Path, value: Any) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, sort_keys=True) + "\n")


def _extract_json_object(content: str) -> Dict[str, Any]:
    candidates = [content.strip()]
    candidates.extend(re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", content, flags=re.DOTALL))
    first = content.find("{")
    last = content.rfind("}")
    if first >= 0 and last > first:
        candidates.append(content[first : last + 1])
    for candidate in candidates:
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise ContractError("response did not contain one parseable JSON object")


def _run_stage(
    provider: LMStudioProvider,
    run_dir: Path,
    stage: str,
    prompt: str,
    validator: Callable[[Any], Dict[str, Any]],
    temperature: float,
    max_tokens: int,
    max_retries: int,
) -> Dict[str, Any]:
    active_prompt = prompt
    for attempt in range(max_retries + 1):
        response = provider.chat(
            messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": active_prompt}],
            temperature=temperature,
            max_tokens=max_tokens,
            reasoning_effort="none",
        )
        record = {
            "stage": stage,
            "attempt": attempt + 1,
            "ok": response.ok,
            "content": response.content,
            "model": response.model,
            "usage": response.usage,
            "error": response.error,
            "captured_at": datetime.now(timezone.utc).isoformat(),
        }
        if not response.ok:
            error = response.error or "provider returned no content"
        else:
            try:
                result = validator(_extract_json_object(response.content))
            except (ContractError, ValueError, TypeError) as exc:
                error = str(exc)
            else:
                record["validation"] = "accepted"
                _append_jsonl(run_dir / "raw-responses.jsonl", record)
                return result
        record["validation"] = "rejected"
        record["validation_error"] = error
        _append_jsonl(run_dir / "raw-responses.jsonl", record)
        if attempt < max_retries:
            active_prompt = repair_prompt(prompt, response.content, error)
    raise ContractError(f"{stage} exhausted {max_retries + 1} attempts: {error}")


def run_chain(config: ChainConfig) -> Dict[str, Any]:
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
    run_dir = config.run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    _write_json(
        run_dir / "input.json",
        {
            "schema_version": "gemma.chain-input.v1",
            "seed": config.seed,
            "base_url": config.base_url,
            "model": config.model,
            "max_retries": config.max_retries,
        },
    )
    provider = LMStudioProvider(config.base_url, config.model, config.timeout_seconds)
    health = provider.health()
    _write_json(run_dir / "provider-health.json", health)
    if not health.get("ok"):
        report = {"ok": False, "run_dir": str(run_dir), "failed_stage": "provider-health", "error": health.get("error") or "requested model is not loaded"}
        _write_json(run_dir / "report.json", report)
        return report

    try:
        ideas = _run_stage(provider, run_dir, "five-ideas", ideas_prompt(config.seed), validate_ideas, 1.0, 700, config.max_retries)
        _write_json(run_dir / "01-five-ideas.json", ideas)
        idea_ids = {item["idea_id"] for item in ideas["ideas"]}
        domains = _run_stage(provider, run_dir, "domains", domains_prompt(ideas), lambda value: validate_domains(value, idea_ids), 0.5, 1100, config.max_retries)
        _write_json(run_dir / "02-domains.json", domains)
        domain_ids = {item["domain_id"] for item in domains["domains"]}
        taxonomy = _run_stage(provider, run_dir, "taxonomy", taxonomy_prompt(domains), lambda value: validate_taxonomy(value, domain_ids), 0.2, 1400, config.max_retries)
        _write_json(run_dir / "03-taxonomy.json", taxonomy)
    except ContractError as exc:
        report = {"ok": False, "run_dir": str(run_dir), "failed_stage": "chain", "error": str(exc)}
        _write_json(run_dir / "report.json", report)
        return report

    report = {
        "ok": True,
        "run_dir": str(run_dir),
        "seed": config.seed,
        "model": config.model,
        "reasoning_effort": "none",
        "idea_count": len(ideas["ideas"]),
        "domain_count": len(domains["domains"]),
        "taxonomy_domain_count": len(taxonomy["taxonomy"]),
        "promotion": "none",
    }
    _write_json(run_dir / "report.json", report)
    return report


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--seed", required=True, help="One raw game-system seed to expand")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--run-root", type=Path, default=Path("runs/gemma/chain-harness"))
    parser.add_argument("--max-retries", type=int, choices=range(0, 4), default=1)
    parser.add_argument("--timeout-seconds", type=int, default=120)


def run_from_namespace(args: argparse.Namespace) -> int:
    report = run_chain(ChainConfig(seed=args.seed, base_url=args.base_url, model=args.model, run_root=args.run_root, max_retries=args.max_retries, timeout_seconds=args.timeout_seconds))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="workflow-gemma-chain")
    configure_parser(parser)
    return run_from_namespace(parser.parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
