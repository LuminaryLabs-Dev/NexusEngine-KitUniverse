from __future__ import annotations

import hashlib
import gzip
import json
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


def now() -> str:
    return datetime.now().astimezone().isoformat()


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def append_jsonl(path: Path, values: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for value in values:
            handle.write(json.dumps(value, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    output = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            output.append(json.loads(line))
    return output


def code_epoch(repo_root: Path) -> Dict[str, str]:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo_root, capture_output=True, text=True, check=False
    ).stdout.strip() or "unversioned"
    digest = hashlib.sha256(head.encode("utf-8"))
    for relative in ["kituniverse_harness", "workflow_harnesses/rawg_capability_pipeline", "workflow_harnesses/kit_universe_batch"]:
        root = repo_root / relative
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.suffix in {".py", ".json", ".html", ".js"}:
                digest.update(str(path.relative_to(repo_root)).encode("utf-8"))
                digest.update(path.read_bytes())
    status = subprocess.run(
        ["git", "status", "--short"], cwd=repo_root, capture_output=True, text=True, check=False
    ).stdout
    digest.update(status.encode("utf-8"))
    return {"git_commit": head, "code_hash": digest.hexdigest(), "dirty_hash": hashlib.sha256(status.encode()).hexdigest()}


class Workspace:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.config_path = self.root / "config.json"
        self.state_path = self.root / "state.json"
        self.control_path = self.root / "control.json"
        self.events_path = self.root / "events.jsonl"
        self.sources_dir = self.root / "source-ledger"
        self.evidence_dir = self.root / "mechanic-evidence"
        self.clusters_path = self.root / "capability-clusters.json"
        self.gaps_path = self.root / "engine-gaps.json"
        self.requests_path = self.root / "build-requests.json"
        self.promotion_requests_path = self.root / "promotion-requests.json"
        self.inventory_path = self.root / "nexus-capability-inventory.json"
        self.epochs_path = self.root / "epochs.jsonl"

    def config(self) -> Dict[str, Any]:
        return read_json(self.config_path, {}) or {}

    def state(self) -> Dict[str, Any]:
        return read_json(self.state_path, {}) or {}

    def update_state(self, **changes: Any) -> Dict[str, Any]:
        state = {**self.state(), **changes, "updated_at": now()}
        write_json_atomic(self.state_path, state)
        return state

    def control(self) -> Dict[str, Any]:
        return read_json(self.control_path, {"action": "run"}) or {"action": "run"}

    def set_control(self, action: str) -> None:
        write_json_atomic(self.control_path, {"action": action, "updated_at": now()})

    def event(self, action: str, data: Dict[str, Any] | None = None) -> None:
        append_jsonl(self.events_path, [{"timestamp": now(), "action": action, "data": data or {}}])

    def free_gib(self) -> float:
        return shutil.disk_usage(self.root).free / (1024 ** 3)

    def read_sharded(self, kind: str) -> List[Dict[str, Any]]:
        directory = self.sources_dir if kind == "source" else self.evidence_dir
        output: List[Dict[str, Any]] = []
        for path in sorted(directory.glob("*.jsonl*")):
            opener = gzip.open if path.suffix == ".gz" else open
            with opener(path, "rt", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        output.append(json.loads(line))
        return output

    def append_sharded(
        self, kind: str, values: Iterable[Dict[str, Any]], record_offset: int, shard_size: int = 10000
    ) -> Path:
        directory = self.sources_dir if kind == "source" else self.evidence_dir
        prefix = "sources" if kind == "source" else "evidence"
        shard_index = record_offset // shard_size
        path = directory / f"{prefix}-{shard_index:06d}.jsonl"
        append_jsonl(path, values)
        if record_offset and record_offset % shard_size == 0:
            previous = directory / f"{prefix}-{shard_index - 1:06d}.jsonl"
            if previous.exists():
                compressed = previous.with_suffix(previous.suffix + ".gz")
                with previous.open("rb") as source, gzip.open(compressed, "wb") as target:
                    shutil.copyfileobj(source, target)
                previous.unlink()
        return path
