from __future__ import annotations

import json
import hashlib
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional


def _simulator_source_hash(cli: List[str], tool_source: str) -> Optional[str]:
    executable_path = Path(cli[-1]) if cli else None
    if not executable_path or not executable_path.exists() or not executable_path.is_file():
        return None
    digest = hashlib.sha256()
    candidates = [executable_path]
    if executable_path.suffix == ".js":
        candidates.extend(
            executable_path.parent / name
            for name in ["actions.js", "tool-catalog.js", tool_source]
        )
    for path in candidates:
        if path.exists() and path.is_file():
            digest.update(path.name.encode("utf-8"))
            digest.update(path.read_bytes())
    return digest.hexdigest()


def resolve_simulator_cli(value: Optional[str]) -> List[str]:
    configured = value or os.environ.get("NEXUS_SIMULATOR_CLI")
    if configured:
        path = Path(configured).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"NexusSimulator CLI not found: {path}")
        return ["node", str(path)] if path.suffix == ".js" else [str(path)]
    executable = shutil.which("nexus-sim")
    if not executable:
        raise FileNotFoundError(
            "NexusSimulator is required; pass --simulator-cli or set NEXUS_SIMULATOR_CLI"
        )
    return [executable]


def run_simulator(
    cli: List[str], input_path: Path, output_path: Path, run_id: str, timeout_seconds: int = 300
) -> Dict[str, Any]:
    command = [
        *cli,
        "tools",
        "run",
        "kit.contract-proof",
        "--input",
        str(input_path.resolve()),
        "--output",
        str(output_path.resolve()),
        "--run-id",
        run_id,
    ]
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout_seconds, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"ok": False, "error": str(error), "command": command}
    try:
        report = json.loads(output_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return {
            "ok": False,
            "error": f"NexusSimulator report unavailable: {error}",
            "command": command,
            "stdout_tail": result.stdout[-2000:],
            "stderr_tail": result.stderr[-2000:],
        }
    report["ok"] = result.returncode == 0 and report.get("status") == "passed"
    report["command"] = command
    report["returncode"] = result.returncode
    report["stderr_tail"] = result.stderr[-2000:]
    source_hash = _simulator_source_hash(cli, "kit-contract-proof.js")
    if source_hash:
        report["simulator_source_hash"] = source_hash
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def run_runtime_proof(
    cli: List[str], manifest_path: Path, output_path: Path, run_id: str, timeout_seconds: int = 300
) -> Dict[str, Any]:
    command = [
        *cli,
        "tools",
        "run",
        "kit.runtime-proof",
        "--manifest",
        str(manifest_path.resolve()),
        "--output",
        str(output_path.resolve()),
        "--run-id",
        run_id,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout_seconds, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"ok": False, "error": str(error), "command": command}
    try:
        report = json.loads(output_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return {"ok": False, "error": f"NexusSimulator runtime report unavailable: {error}", "command": command, "stdout_tail": result.stdout[-2000:], "stderr_tail": result.stderr[-2000:]}
    report.update({"ok": result.returncode == 0 and report.get("status") == "passed", "command": command, "returncode": result.returncode, "stderr_tail": result.stderr[-2000:]})
    source_hash = _simulator_source_hash(cli, "kit-runtime-proof.js")
    if source_hash:
        report["simulator_source_hash"] = source_hash
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report
