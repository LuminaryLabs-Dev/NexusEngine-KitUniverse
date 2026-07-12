from __future__ import annotations

import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

from .contracts import slug


KIT_PATTERNS = [
    re.compile(r"defineDomainServiceKit\s*\(\s*\{([\s\S]{0,5000}?)\}\s*\)", re.MULTILINE),
    re.compile(r"export\s+function\s+(create[A-Za-z0-9]+Kit)\s*\("),
    re.compile(r"export\s+const\s+(create[A-Za-z0-9]+Kit)\s*="),
]

CAPABILITY_ALIASES = {
    "multiplayer-session": {"network", "multiplayer"},
    "cooperative-session": {"network", "multiplayer", "cooperative"},
    "open-world-state": {"world", "terrain", "spatial"},
    "inventory-state": {"inventory", "cargo", "collectible"},
    "dialogue-choice": {"dialogue", "sequence"},
    "branching-choice": {"sequence", "objective-flow"},
    "turn-order": {"schedule", "sequence"},
    "procedural-generation": {"procedural"},
    "run-progression": {"lifecycle-progression", "progression"},
    "survival-resource-pressure": {"resource-pressure"},
    "combat-resolution": {"combat"},
    "projectile-combat": {"combat", "projectile"},
    "race-progression": {"progression", "timing-window"},
    "puzzle-state": {"puzzle", "symbol-alignment", "lock-and-socket"},
    "platforming-movement": {"movement", "micro-platformer"},
    "role-progression": {"lifecycle-progression", "progression"},
    "strategy-command": {"command", "sequence"},
    "simulation-state": {"simulation"},
    "physics-interaction": {"physics", "interaction"},
    "immersive-session": {"ar", "xr"},
    "single-player-session": {"session-facade", "session"},
}


def _identifier_slug(value: str) -> str:
    return slug(re.sub(r"([a-z0-9])([A-Z])", r"\1-\2", str(value)))


def _git_head(path: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=path, capture_output=True, text=True, check=False
    )
    return result.stdout.strip() or "unversioned"


def _candidate_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    return (
        path
        for path in root.rglob("*")
        if path.is_file()
        and path.suffix in {".js", ".mjs", ".ts", ".json"}
        and not any(part in {"node_modules", ".git", "runs", "output"} for part in path.parts)
    )


def build_capability_inventory(engine_root: Path, protokits_root: Path) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    tokens: Set[str] = set()
    domains: Set[str] = set()
    for owner, root in [("nexus-engine", engine_root), ("nexus-engine-protokits", protokits_root)]:
        for path in _candidate_files(root):
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            if "Kit" not in text and "kit" not in path.name.lower():
                continue
            local_ids = set()
            for match in re.finditer(r"\bid\s*:\s*[`\"']([^`\"']+-kit)[`\"']", text):
                local_ids.add(match.group(1))
            for match in re.finditer(r"\bdomain\s*:\s*[`\"']([^`\"']+)[`\"']", text):
                domains.add(slug(match.group(1)))
            for match in re.finditer(r"[\"'`](n:[a-z0-9:_-]+)[\"'`]", text, flags=re.IGNORECASE):
                tokens.add(match.group(1).lower())
            for pattern in KIT_PATTERNS[1:]:
                for match in pattern.finditer(text):
                    local_ids.add(_identifier_slug(match.group(1)))
            for kit_id in sorted(local_ids):
                records.append(
                    {
                        "owner": owner,
                        "kit_id": kit_id,
                        "path": str(path),
                        "terms": sorted(set(slug(kit_id).replace("-kit", "").split("-"))),
                    }
                )
    commit = {"nexus_engine": _git_head(engine_root), "protokits": _git_head(protokits_root)}
    payload = {
        "schema_version": "nexus.capability-inventory.v1",
        "commits": commit,
        "records": records,
        "tokens": sorted(tokens),
        "domains": sorted(value for value in domains if value),
    }
    payload["inventory_hash"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return payload


def capability_status(capability_id: str, inventory: Dict[str, Any]) -> Dict[str, Any]:
    terms = {term for term in slug(capability_id).split("-") if len(term) > 2}
    candidates = []
    for record in inventory.get("records", []):
        record_terms = set(record.get("terms") or [])
        overlap = sorted(terms & record_terms)
        if overlap:
            candidates.append({"kit_id": record["kit_id"], "path": record["path"], "overlap": overlap})
    aliases = CAPABILITY_ALIASES.get(slug(capability_id), set())
    alias_matches = [
        record
        for record in inventory.get("records", [])
        if any(alias in slug(record.get("kit_id")) for alias in aliases)
    ]
    exact_token = f"n:{slug(capability_id)}" in set(inventory.get("tokens") or [])
    strong = [item for item in candidates if len(item["overlap"]) >= max(1, min(2, len(terms)))]
    return {
        "status": "already-supported" if exact_token or strong or alias_matches else "missing",
        "exact_token": exact_token,
        "matches": [*strong, *({"kit_id": item["kit_id"], "path": item["path"], "overlap": ["alias"]} for item in alias_matches)][:10],
    }
