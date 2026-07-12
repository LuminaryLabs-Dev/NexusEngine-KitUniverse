from __future__ import annotations

from typing import Any, Dict, List

from workflow_harnesses.fractal_kit_pipeline.kit_contract import (
    DEPENDENCIES,
    DOMAIN_FAMILIES,
    NEEDS,
    OPERATIONS,
    PROOF_TYPES,
    SURFACES,
    make_kit_record,
)


def build_idea_matrix(target_size: int, reveal_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = []
    seen = set()
    reveal_values = [record.get("reduced") or record.get("expansion_point") for record in reveal_records] or [
        "runtime state",
        "event flow",
        "snapshot contract",
    ]
    domain_pairs = [(family, subdomain) for family, subdomains in DOMAIN_FAMILIES for subdomain in subdomains]
    index = 0
    max_attempts = target_size * 20
    while len(records) < target_size and index < max_attempts:
        family, subdomain = domain_pairs[index % len(domain_pairs)]
        need = NEEDS[(index // len(domain_pairs)) % len(NEEDS)]
        dependency = DEPENDENCIES[(index // (len(domain_pairs) * len(NEEDS))) % len(DEPENDENCIES)]
        operation = OPERATIONS[(index // 3) % len(OPERATIONS)]
        surface = SURFACES[(index // 5) % len(SURFACES)]
        proof = PROOF_TYPES[(index // 7) % len(PROOF_TYPES)]
        reveal = reveal_values[index % len(reveal_values)]
        record = make_kit_record(
            index=index,
            family=family,
            subdomain=subdomain,
            need=need,
            dependency=dependency,
            operation=operation,
            surface=surface,
            proof=proof,
            reveal=reveal,
        )
        key = record["payload"]["semantic_key"]
        if key not in seen:
            seen.add(key)
            records.append(record)
        index += 1
    return records
