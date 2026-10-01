from __future__ import annotations

import re
from typing import Any, Dict, List, Set


LENSES = {
    "mechanical",
    "environment-simulation",
    "progression-economy",
    "social-ai-multiplayer",
    "rare-hybrid-failure-emergent",
}
FORBIDDEN_CATCH_ALLS = {"misc", "miscellaneous", "other", "uncategorized"}


class ContractError(ValueError):
    """Raised when a stage output cannot cross its deterministic gate."""


def _object(value: Any, label: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{label} must be a JSON object")
    return value


def _nonempty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{label} must be a non-empty string")
    return value.strip()


def _string_list(value: Any, label: str, minimum: int = 1) -> List[str]:
    if not isinstance(value, list) or len(value) < minimum:
        raise ContractError(f"{label} must contain at least {minimum} strings")
    result = [_nonempty_string(item, label) for item in value]
    if len({item.casefold() for item in result}) != len(result):
        raise ContractError(f"{label} must not contain duplicates")
    return result


def _slug(value: Any, label: str) -> str:
    result = _nonempty_string(value, label)
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", result):
        raise ContractError(f"{label} must be a lowercase kebab-case id")
    return result


def validate_ideas(value: Any) -> Dict[str, Any]:
    payload = _object(value, "stage 1 output")
    ideas = payload.get("ideas")
    if not isinstance(ideas, list) or len(ideas) != 5:
        raise ContractError("stage 1 must contain exactly five ideas")
    normalized = []
    seen_ids: Set[str] = set()
    seen_titles: Set[str] = set()
    seen_lenses: Set[str] = set()
    for index, item in enumerate(ideas, start=1):
        entry = _object(item, f"ideas[{index}]")
        idea_id = _slug(entry.get("idea_id"), f"ideas[{index}].idea_id")
        title = _nonempty_string(entry.get("title"), f"ideas[{index}].title")
        description = _nonempty_string(entry.get("description"), f"ideas[{index}].description")
        lens = _nonempty_string(entry.get("lens"), f"ideas[{index}].lens")
        if lens not in LENSES:
            raise ContractError(f"ideas[{index}].lens is not one of the five required lenses")
        if idea_id in seen_ids or title.casefold() in seen_titles or lens in seen_lenses:
            raise ContractError("idea ids, titles, and lenses must each be unique")
        seen_ids.add(idea_id)
        seen_titles.add(title.casefold())
        seen_lenses.add(lens)
        normalized.append(
            {"idea_id": idea_id, "title": title, "description": description, "lens": lens}
        )
    if seen_lenses != LENSES:
        raise ContractError("stage 1 must cover every required lens exactly once")
    return {"schema_version": "gemma.five-ideas.v1", "ideas": normalized}


def validate_domains(value: Any, idea_ids: Set[str]) -> Dict[str, Any]:
    payload = _object(value, "stage 2 output")
    domains = payload.get("domains")
    if not isinstance(domains, list) or not 5 <= len(domains) <= 20:
        raise ContractError("stage 2 must contain between 5 and 20 domains")
    normalized = []
    seen_ids: Set[str] = set()
    used_ideas: Set[str] = set()
    for index, item in enumerate(domains, start=1):
        entry = _object(item, f"domains[{index}]")
        domain_id = _slug(entry.get("domain_id"), f"domains[{index}].domain_id")
        name = _nonempty_string(entry.get("name"), f"domains[{index}].name")
        purpose = _nonempty_string(entry.get("purpose"), f"domains[{index}].purpose")
        refs = _string_list(entry.get("idea_refs"), f"domains[{index}].idea_refs")
        signals = _string_list(
            entry.get("capability_signals"), f"domains[{index}].capability_signals"
        )
        if domain_id in seen_ids:
            raise ContractError("domain ids must be unique")
        unknown_refs = set(refs) - idea_ids
        if unknown_refs:
            raise ContractError(f"domain {domain_id} cites unknown ideas: {sorted(unknown_refs)}")
        seen_ids.add(domain_id)
        used_ideas.update(refs)
        normalized.append(
            {
                "domain_id": domain_id,
                "name": name,
                "purpose": purpose,
                "idea_refs": refs,
                "capability_signals": signals,
            }
        )
    if used_ideas != idea_ids:
        raise ContractError(f"stage 2 left ideas unused: {sorted(idea_ids - used_ideas)}")
    return {"schema_version": "gemma.domain-list.v1", "domains": normalized}


def validate_taxonomy(value: Any, domain_ids: Set[str]) -> Dict[str, Any]:
    payload = _object(value, "stage 3 output")
    groups = payload.get("taxonomy")
    if not isinstance(groups, list) or not groups:
        raise ContractError("stage 3 taxonomy must contain at least one top-level group")
    normalized = []
    assigned: List[str] = []
    for group_index, item in enumerate(groups, start=1):
        group = _object(item, f"taxonomy[{group_index}]")
        group_name = _nonempty_string(group.get("domain"), f"taxonomy[{group_index}].domain")
        if group_name.casefold() in FORBIDDEN_CATCH_ALLS:
            raise ContractError(f"catch-all taxonomy domain is forbidden: {group_name}")
        subdomains = group.get("subdomains")
        if not isinstance(subdomains, list) or not subdomains:
            raise ContractError(f"taxonomy[{group_index}].subdomains must not be empty")
        normalized_subdomains = []
        for sub_index, sub_item in enumerate(subdomains, start=1):
            subdomain = _object(sub_item, f"taxonomy[{group_index}].subdomains[{sub_index}]")
            name = _nonempty_string(subdomain.get("name"), "subdomain.name")
            if name.casefold() in FORBIDDEN_CATCH_ALLS:
                raise ContractError(f"catch-all subdomain is forbidden: {name}")
            refs = _string_list(subdomain.get("domain_refs"), "subdomain.domain_refs")
            capabilities = _string_list(subdomain.get("capabilities"), "subdomain.capabilities")
            unknown_refs = set(refs) - domain_ids
            if unknown_refs:
                raise ContractError(f"taxonomy cites unknown domains: {sorted(unknown_refs)}")
            assigned.extend(refs)
            normalized_subdomains.append(
                {"name": name, "domain_refs": refs, "capabilities": capabilities}
            )
        normalized.append({"domain": group_name, "subdomains": normalized_subdomains})
    duplicate_refs = sorted({ref for ref in assigned if assigned.count(ref) > 1})
    missing_refs = sorted(domain_ids - set(assigned))
    if duplicate_refs or missing_refs:
        raise ContractError(
            f"every stage 2 domain must appear exactly once; duplicates={duplicate_refs}, missing={missing_refs}"
        )
    return {"schema_version": "gemma.domain-taxonomy.v1", "taxonomy": normalized}
