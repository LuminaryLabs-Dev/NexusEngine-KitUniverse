from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .contracts import DATASET_ID, RAWG_SOURCE_SCHEMA, slug, split_terms, stable_hash


EDITION_WORDS = {
    "edition", "enhanced", "complete", "collection", "definitive", "deluxe",
    "game", "goty", "hd", "remaster", "remastered", "ultimate", "version",
}


def discover_chunks(source_root: Path) -> List[Path]:
    return sorted(source_root.glob("rawg-*.jsonl"))


def _canonical_name(value: str) -> str:
    words = [word for word in slug(value).split("-") if word and word not in EDITION_WORDS]
    return "-".join(words) or slug(value) or "unknown-game"


def normalize_rawg_record(
    raw: Dict[str, Any], source_file: Path, line_number: int, pipeline_epoch: str
) -> Dict[str, Any]:
    name = str(raw.get("name") or raw.get("title") or raw.get("slug") or raw.get("id") or "").strip()
    source_id = str(raw.get("id") or raw.get("sourceId") or raw.get("slug") or slug(name)).strip()
    description = " ".join(str(raw.get("description") or "").split())
    genres = split_terms(raw.get("genres"))
    tags = split_terms(raw.get("tags"))
    platforms = split_terms(raw.get("platforms"))
    developers = split_terms(raw.get("developers"))
    publishers = split_terms(raw.get("publishers"))
    evidence_fields = {
        key: value
        for key, value in {
            "description": description,
            "genres": genres,
            "tags": tags,
            "platforms": platforms,
            "developers": developers,
            "publishers": publishers,
            "released": raw.get("released"),
            "rating": raw.get("rating"),
            "metacritic": raw.get("metacritic"),
            "playtime": raw.get("timeToPlay") if raw.get("timeToPlay") is not None else raw.get("playtime"),
        }.items()
        if value not in (None, "", [])
    }
    hash_payload = {"source_id": source_id, "name": name, **evidence_fields}
    source_hash = stable_hash(hash_payload)
    fingerprint_payload = {
        "canonical_name": _canonical_name(name),
        "description": re.sub(r"\s+", " ", description.lower())[:4000],
        "genres": sorted(slug(item) for item in genres),
        "tags": sorted(slug(item) for item in tags),
    }
    return {
        "schema_version": RAWG_SOURCE_SCHEMA,
        "dataset": DATASET_ID,
        "source_id": source_id or slug(name) or f"line-{line_number}",
        "source_url": raw.get("sourceUrl") or f"https://rawg.io/games/{raw.get('slug') or slug(name)}",
        "source_file": str(source_file),
        "source_line": line_number,
        "pipeline_epoch": pipeline_epoch,
        "name": name or source_id or "Untitled game",
        "canonical_name": fingerprint_payload["canonical_name"],
        "description": description,
        "genres": genres,
        "tags": tags,
        "platforms": platforms,
        "developers": developers,
        "publishers": publishers,
        "evidence_fields": evidence_fields,
        "source_hash": source_hash,
        "evidence_fingerprint": stable_hash(fingerprint_payload),
        "has_mechanic_evidence": bool(description or genres or tags),
    }


def stream_rawg_records(
    source_root: Path,
    pipeline_epoch: str,
    start_file: Optional[str] = None,
    start_line: int = 0,
) -> Iterator[Tuple[Dict[str, Any], str, int]]:
    chunks = discover_chunks(source_root)
    for path in chunks:
        if start_file and path.name < start_file:
            continue
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if start_file == path.name and line_number <= start_line:
                    continue
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    if not isinstance(raw, dict):
                        raise ValueError("RAWG row must be a JSON object")
                    yield normalize_rawg_record(raw, path, line_number, pipeline_epoch), path.name, line_number
                except (json.JSONDecodeError, ValueError) as error:
                    yield {
                        "schema_version": RAWG_SOURCE_SCHEMA,
                        "dataset": DATASET_ID,
                        "source_id": f"malformed:{path.name}:{line_number}",
                        "source_file": str(path),
                        "source_line": line_number,
                        "pipeline_epoch": pipeline_epoch,
                        "source_hash": stable_hash(line),
                        "evidence_fingerprint": stable_hash([path.name, line_number, line]),
                        "has_mechanic_evidence": False,
                        "error": str(error),
                    }, path.name, line_number
