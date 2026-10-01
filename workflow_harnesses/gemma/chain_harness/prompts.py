from __future__ import annotations

import json
from typing import Any, Dict


SYSTEM = """You are a structured game-system ideation component.
Return one JSON object only. Do not use Markdown fences or commentary.
Prefer reusable mechanics and capabilities over story, art direction, or named content."""


def ideas_prompt(seed: str) -> str:
    return f"""Expand this raw seed into exactly five distinct reusable system ideas.

RAW SEED:
{seed}

Use each lens exactly once:
- mechanical
- environment-simulation
- progression-economy
- social-ai-multiplayer
- rare-hybrid-failure-emergent

Return:
{{"ideas":[{{"idea_id":"lowercase-kebab-id","title":"...","description":"...","lens":"one-required-lens"}}]}}
Every description must state an observable behavior, condition, or interaction."""


def domains_prompt(ideas: Dict[str, Any]) -> str:
    return f"""Convert these five ideas into 5 to 20 reusable system domains.

IDEAS:
{json.dumps(ideas, indent=2, sort_keys=True)}

Return:
{{"domains":[{{"domain_id":"lowercase-kebab-id","name":"...","purpose":"...","idea_refs":["idea-id"],"capability_signals":["specific reusable behavior"]}}]}}
Each domain must cite at least one supplied idea. Every supplied idea must be cited.
Do not invent idea ids. Avoid vague domains such as gameplay, content, miscellaneous, or other."""


def taxonomy_prompt(domains: Dict[str, Any]) -> str:
    return f"""Reconcile this domain list into a compact three-level taxonomy:
top-level domain -> subdomain -> reusable capabilities.

DOMAINS:
{json.dumps(domains, indent=2, sort_keys=True)}

Return:
{{"taxonomy":[{{"domain":"...","subdomains":[{{"name":"...","domain_refs":["domain-id"],"capabilities":["specific reusable capability"]}}]}}]}}
Every supplied domain_id must appear in domain_refs exactly once across the whole taxonomy.
Do not invent domain ids. Do not create Miscellaneous, Other, or catch-all groups."""


def repair_prompt(original_prompt: str, invalid_output: str, error: str) -> str:
    return f"""{original_prompt}

Your prior response failed deterministic validation.
ERROR: {error}
PRIOR RESPONSE:
{invalid_output}

Repair only the stated contract failure. Return the complete corrected JSON object only."""
