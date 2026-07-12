from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Any, Dict


CODEX_BINARY = Path("/Applications/ChatGPT.app/Contents/Resources/codex")
CODEX_MODEL = "gpt-5.6-sol"


def run_codex_cli_review(repo_root: Path, run_dir: Path, timeout_seconds: int = 900) -> Dict[str, Any]:
    output_path = run_dir / "codex-cli-review.md"
    prompt = f"""
Review and self-correct the KitUniverse one-shot CLI after exactly one attempted kit turn.

READ FIRST:
- {run_dir / 'report.json'}
- {run_dir / 'final-kits.jsonl'}
- workflow_harnesses/guided_kit_builder/
- kituniverse_harness/cli.py

GOAL:
- The hero command accepts one rough --idea and produces exactly one validated kit.
- LM Studio 350M remains the generator.
- The cumulative universe rejects duplicates and tracks progress toward 1000 kits.
- Find the highest-value actual CLI defect exposed by this turn.
- If a small safe correction is needed, implement it and run compile plus CLI help validation.
- If no correction is justified, do not churn code; explain the next review focus.

BOUNDARIES:
- Do not generate another kit or invoke --universe-turn; a failed attempt must remain uncommitted.
- Do not edit runs/, buckets/, cumulative JSONL ledgers, goal files, or memory files.
- Preserve unrelated user changes.
- Make at most one coherent CLI improvement.

FINAL RESPONSE:
State the observed issue, files changed, validation, and next review focus concisely.
""".strip()
    command = [
        str(CODEX_BINARY),
        "exec",
        "--ephemeral",
        "--color",
        "never",
        "-C",
        str(repo_root),
        "-s",
        "workspace-write",
        "-m",
        CODEX_MODEL,
        "-c",
        'model_reasoning_effort="medium"',
        "-o",
        str(output_path),
        prompt,
    ]
    started = time.time()
    try:
        result = subprocess.run(
            command,
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return {
            "ok": False,
            "model": CODEX_MODEL,
            "binary": str(CODEX_BINARY),
            "reasoning_effort": "medium",
            "error": str(error),
            "output": str(output_path),
            "elapsed_seconds": round(time.time() - started, 3),
        }
    return {
        "ok": result.returncode == 0 and output_path.exists(),
        "model": CODEX_MODEL,
        "binary": str(CODEX_BINARY),
        "reasoning_effort": "medium",
        "returncode": result.returncode,
        "output": str(output_path),
        "elapsed_seconds": round(time.time() - started, 3),
        "stderr_tail": result.stderr[-2000:],
    }
