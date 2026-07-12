from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_BUCKET_ROOT = Path("buckets")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="bucket-service")
    subcommands = parser.add_subparsers(dest="command", required=True)

    create_parser = subcommands.add_parser("create")
    create_parser.add_argument("bucket")
    create_parser.add_argument("--root", default=str(DEFAULT_BUCKET_ROOT))

    intake_parser = subcommands.add_parser("intake")
    intake_parser.add_argument("bucket")
    intake_parser.add_argument("--content", required=True)
    intake_parser.add_argument("--source", default="cli")
    intake_parser.add_argument("--root", default=str(DEFAULT_BUCKET_ROOT))

    args = parser.parse_args(argv)
    service = BucketService(Path(args.root))
    if args.command == "create":
        report = service.create_bucket(args.bucket)
    elif args.command == "intake":
        report = service.intake(args.bucket, args.content, source=args.source)
    else:
        parser.error(f"unknown command: {args.command}")
        return 2

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


class BucketService:
    def __init__(self, root: Path = DEFAULT_BUCKET_ROOT) -> None:
        self.root = root

    def create_bucket(self, bucket: str) -> Dict[str, Any]:
        bucket_path = self._bucket_path(bucket)
        for child in [
            bucket_path / "intake",
            bucket_path / "heuristic-state",
            bucket_path / "llm-step",
            bucket_path / "final",
        ]:
            child.mkdir(parents=True, exist_ok=True)

        _write_json_if_missing(
            bucket_path / "bucket.json",
            {
                "bucket": bucket,
                "purpose": "Folder-backed bucket with intake service.",
                "created_at": _timestamp(),
            },
        )
        _write_json_if_missing(
            bucket_path / "heuristic-state" / "state.json",
            {
                "duplicate_policy": "reject normalized duplicates from final submissions",
                "fingerprints": [],
            },
        )
        _write_json_if_missing(
            bucket_path / "llm-step" / "state.json",
            {
                "enabled": False,
                "mode": "auto-pass",
                "note": "LLM gate is intentionally empty for now.",
            },
        )
        _write_json_if_missing(bucket_path / "final" / "submissions.json", [])

        return {
            "ok": True,
            "bucket": bucket,
            "bucket_path": str(bucket_path),
        }

    def intake(self, bucket: str, content: str, source: str = "cli") -> Dict[str, Any]:
        self.create_bucket(bucket)
        bucket_path = self._bucket_path(bucket)
        normalized = _normalize_content(content)
        fingerprint = _fingerprint(normalized)
        submission_id = f"{time.strftime('%Y%m%d-%H%M%S')}-{fingerprint[:12]}"

        intake_record = {
            "id": submission_id,
            "bucket": bucket,
            "source": source,
            "created_at": _timestamp(),
            "content": content,
            "normalized": normalized,
            "fingerprint": fingerprint,
        }
        _write_json(bucket_path / "intake" / f"{submission_id}.json", intake_record)

        heuristic = self._heuristic_step(bucket_path, fingerprint)
        _write_json(
            bucket_path / "heuristic-state" / f"{submission_id}.json",
            heuristic,
        )
        if not heuristic["ok"]:
            return {
                "ok": False,
                "bucket": bucket,
                "id": submission_id,
                "stage": "heuristic",
                "reason": heuristic["reason"],
                "intake_path": str(bucket_path / "intake" / f"{submission_id}.json"),
            }

        llm = self._llm_step(bucket_path, submission_id)
        _write_json(bucket_path / "llm-step" / f"{submission_id}.json", llm)
        if not llm["ok"]:
            return {
                "ok": False,
                "bucket": bucket,
                "id": submission_id,
                "stage": "llm",
                "reason": llm["reason"],
            }

        final_record = {
            **intake_record,
            "accepted_at": _timestamp(),
            "heuristic": heuristic,
            "llm": llm,
        }
        final_path = bucket_path / "final" / f"{submission_id}.json"
        _write_json(final_path, final_record)

        submissions_path = bucket_path / "final" / "submissions.json"
        submissions = _read_json_list(submissions_path)
        submissions.append(
            {
                "id": submission_id,
                "fingerprint": fingerprint,
                "source": source,
                "final_path": str(final_path),
                "accepted_at": final_record["accepted_at"],
            }
        )
        _write_json(submissions_path, submissions)
        self._update_heuristic_state(bucket_path, fingerprint)

        return {
            "ok": True,
            "bucket": bucket,
            "id": submission_id,
            "stage": "final",
            "final_path": str(final_path),
            "submissions_path": str(submissions_path),
        }

    def _heuristic_step(self, bucket_path: Path, fingerprint: str) -> Dict[str, Any]:
        submissions = _read_json_list(bucket_path / "final" / "submissions.json")
        duplicate = next(
            (
                item
                for item in submissions
                if isinstance(item, dict) and item.get("fingerprint") == fingerprint
            ),
            None,
        )
        if duplicate:
            return {
                "ok": False,
                "step": "heuristic",
                "reason": "duplicate final submission",
                "duplicate": duplicate,
                "fingerprint": fingerprint,
            }
        return {
            "ok": True,
            "step": "heuristic",
            "reason": "no duplicate final submission",
            "fingerprint": fingerprint,
        }

    def _llm_step(self, bucket_path: Path, submission_id: str) -> Dict[str, Any]:
        state = _read_json(bucket_path / "llm-step" / "state.json", default={})
        return {
            "ok": True,
            "step": "llm",
            "mode": state.get("mode", "auto-pass"),
            "enabled": False,
            "reason": "LLM gate not implemented; auto-pass placeholder.",
            "submission_id": submission_id,
        }

    def _update_heuristic_state(self, bucket_path: Path, fingerprint: str) -> None:
        state_path = bucket_path / "heuristic-state" / "state.json"
        state = _read_json(state_path, default={})
        fingerprints = state.get("fingerprints")
        if not isinstance(fingerprints, list):
            fingerprints = []
        if fingerprint not in fingerprints:
            fingerprints.append(fingerprint)
        state["fingerprints"] = fingerprints
        state["updated_at"] = _timestamp()
        _write_json(state_path, state)

    def _bucket_path(self, bucket: str) -> Path:
        safe_name = _safe_bucket_name(bucket)
        return self.root / safe_name


def _safe_bucket_name(bucket: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "-", bucket.strip()).strip("-")
    if not safe:
        raise ValueError("bucket name must contain at least one safe character")
    return safe


def _normalize_content(content: str) -> str:
    return " ".join(content.strip().lower().split())


def _fingerprint(normalized: str) -> str:
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _timestamp() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S %Z")


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _read_json_list(path: Path) -> List[Dict[str, Any]]:
    value = _read_json(path, default=[])
    if not isinstance(value, list):
        return []
    return value


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_json_if_missing(path: Path, data: Any) -> None:
    if not path.exists():
        _write_json(path, data)


if __name__ == "__main__":
    raise SystemExit(main())
