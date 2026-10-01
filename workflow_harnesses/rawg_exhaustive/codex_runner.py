from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable


APP_CODEX_BINARY = Path("/Applications/ChatGPT.app/Contents/Resources/codex")
DEFAULT_CODEX_MODEL = "gpt-5.6-luna"
DEFAULT_REASONING_EFFORT = "low"


def resolve_codex_binary() -> Path | None:
    binary = shutil.which("codex")
    if binary:
        return Path(binary)
    return APP_CODEX_BINARY if APP_CODEX_BINARY.exists() else None


def run_codex_lane(
    *,
    repo_root: Path,
    artifact_root: Path,
    scratch_root: Path,
    job_id: str,
    prompt: str,
    model: str = DEFAULT_CODEX_MODEL,
    reasoning_effort: str = DEFAULT_REASONING_EFFORT,
    timeout_seconds: int = 900,
    attempt: int = 0,
) -> Dict[str, Any]:
    safe_job_id = _safe_job_id(job_id)
    response_root = artifact_root / "responses"
    execution_root = artifact_root / "executions"
    response_root.mkdir(parents=True, exist_ok=True)
    execution_root.mkdir(parents=True, exist_ok=True)
    scratch = scratch_root / safe_job_id / f"attempt-{attempt}"
    stale_scratch_removed = False
    if scratch.exists():
        shutil.rmtree(scratch)
        stale_scratch_removed = True
    scratch.mkdir(parents=True, exist_ok=False)
    output_path = response_root / f"{safe_job_id}.attempt-{attempt}.txt"
    execution_path = execution_root / f"{safe_job_id}.attempt-{attempt}.json"
    binary = resolve_codex_binary()
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    process: subprocess.CompletedProcess[str] | None = None
    error: str | None = None
    try:
        if binary is None:
            error = "Codex CLI binary was not found"
        else:
            command = [
                str(binary),
                "exec",
                "--ephemeral",
                "--color",
                "never",
                "-C",
                str(repo_root.resolve()),
                "-s",
                "read-only",
                "-m",
                model,
                "-c",
                f'model_reasoning_effort="{reasoning_effort}"',
                "-o",
                str(output_path.resolve()),
                "-",
            ]
            environment = dict(os.environ)
            environment["TMPDIR"] = str(scratch.resolve())
            process = subprocess.run(
                command,
                input=prompt,
                cwd=repo_root,
                env=environment,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
    except subprocess.TimeoutExpired:
        error = f"Codex CLI timed out after {timeout_seconds} seconds"
    except OSError as exc:
        error = str(exc)
    finished = time.monotonic()
    try:
        shutil.rmtree(scratch)
        scratch_cleaned = True
    except OSError:
        scratch_cleaned = False
    if scratch_cleaned:
        for empty_root in (scratch.parent, scratch_root, scratch_root.parent):
            try:
                empty_root.rmdir()
            except OSError:
                pass
    response_exists = output_path.is_file() and bool(output_path.read_text(encoding="utf-8").strip())
    ok = process is not None and process.returncode == 0 and response_exists and scratch_cleaned
    report = {
        "schema_version": "kituniverse.codex-lane-execution.v1",
        "job_id": job_id,
        "attempt": attempt,
        "ok": ok,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "sandbox": "read-only",
        "started_at": started_at,
        "elapsed_seconds": round(finished - started, 3),
        "returncode": process.returncode if process is not None else None,
        "response": str(output_path) if response_exists else None,
        "error": error,
        "stderr_tail": process.stderr[-2000:] if process is not None and process.returncode != 0 else "",
        "scratch_cleaned": scratch_cleaned,
        "stale_scratch_removed": stale_scratch_removed,
    }
    _write_json(execution_path, report)
    return {
        **report,
        "execution_report": str(execution_path),
        "_started_monotonic": started,
        "_finished_monotonic": finished,
    }


def peak_overlap(reports: Iterable[Dict[str, Any]]) -> int:
    events: list[tuple[float, int]] = []
    for report in reports:
        start = report.get("_started_monotonic")
        finish = report.get("_finished_monotonic")
        if isinstance(start, (int, float)) and isinstance(finish, (int, float)):
            events.extend(((float(start), 1), (float(finish), -1)))
    active = peak = 0
    for _, delta in sorted(events, key=lambda event: (event[0], -event[1])):
        active += delta
        peak = max(peak, active)
    return peak


def cleanup_scratch(workspace: Path) -> Dict[str, Any]:
    root = workspace / ".codex-scratch"
    removed: list[str] = []
    failures: list[Dict[str, str]] = []
    if root.exists():
        for path in sorted(root.iterdir()):
            try:
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
                removed.append(str(path.relative_to(workspace)))
            except OSError as exc:
                failures.append({"path": str(path), "error": str(exc)})
        try:
            root.rmdir()
        except OSError:
            pass
    return {
        "ok": not failures,
        "scratch_root": str(root),
        "removed": removed,
        "failures": failures,
        "scratch_remaining": sum(1 for _ in root.rglob("*")) if root.exists() else 0,
    }


def _safe_job_id(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-.")
    return normalized[:160] or "job"


def _write_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)
