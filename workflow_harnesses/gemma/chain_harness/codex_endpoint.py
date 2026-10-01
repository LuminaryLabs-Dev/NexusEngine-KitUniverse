from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_MODEL = "gpt-5.6-luna"
DEFAULT_REASONING_EFFORT = "xhigh"
DEFAULT_RUN_ROOT = Path("runs/gemma/codex-luna")
APP_CODEX_BINARY = Path("/Applications/ChatGPT.app/Contents/Resources/codex")


@dataclass(frozen=True)
class CodexEndpointConfig:
    prompt: str
    repo_root: Path
    run_root: Path = DEFAULT_RUN_ROOT
    model: str = DEFAULT_MODEL
    reasoning_effort: str = DEFAULT_REASONING_EFFORT
    timeout_seconds: int = 900


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _resolve_binary() -> Optional[Path]:
    path_binary = shutil.which("codex")
    if path_binary:
        return Path(path_binary)
    if APP_CODEX_BINARY.exists():
        return APP_CODEX_BINARY
    return None


def ask_codex(config: CodexEndpointConfig) -> Dict[str, Any]:
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
    run_dir = config.run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    input_record = {
        "schema_version": "gemma.codex-request.v1",
        "prompt": config.prompt,
        "model": config.model,
        "reasoning_effort": config.reasoning_effort,
        "sandbox": "read-only",
        "repo_root": str(config.repo_root.resolve()),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(run_dir / "request.json", input_record)
    binary = _resolve_binary()
    output_path = run_dir / "response.md"
    if binary is None:
        report = {
            "ok": False,
            "run_dir": str(run_dir),
            "model": config.model,
            "reasoning_effort": config.reasoning_effort,
            "error": "Codex CLI binary was not found",
        }
        _write_json(run_dir / "report.json", report)
        return report

    command = [
        str(binary),
        "exec",
        "--ephemeral",
        "--color",
        "never",
        "-C",
        str(config.repo_root.resolve()),
        "-s",
        "read-only",
        "-m",
        config.model,
        "-c",
        f'model_reasoning_effort="{config.reasoning_effort}"',
        "-o",
        str(output_path.resolve()),
        "-",
    ]
    started = time.monotonic()
    try:
        result = subprocess.run(
            command,
            input=config.prompt,
            cwd=config.repo_root,
            capture_output=True,
            text=True,
            timeout=config.timeout_seconds,
            check=False,
        )
        error = None
    except subprocess.TimeoutExpired as exc:
        result = None
        error = f"Codex CLI timed out after {config.timeout_seconds} seconds: {exc}"
    except OSError as exc:
        result = None
        error = str(exc)

    elapsed_seconds = round(time.monotonic() - started, 3)
    response_exists = output_path.exists() and bool(output_path.read_text(encoding="utf-8").strip())
    ok = result is not None and result.returncode == 0 and response_exists
    report = {
        "ok": ok,
        "run_dir": str(run_dir),
        "response": str(output_path) if response_exists else None,
        "model": config.model,
        "reasoning_effort": config.reasoning_effort,
        "sandbox": "read-only",
        "binary": str(binary),
        "returncode": result.returncode if result is not None else None,
        "elapsed_seconds": elapsed_seconds,
        "error": error,
        "stderr_tail": result.stderr[-2000:] if result is not None else "",
    }
    _write_json(run_dir / "report.json", report)
    return report


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("prompt", help="Question or bounded read-only task for Codex CLI")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument(
        "--reasoning-effort",
        choices=("low", "medium", "high", "xhigh"),
        default=DEFAULT_REASONING_EFFORT,
    )
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--text", action="store_true", help="Print only the final Codex response")


def run_from_namespace(args: argparse.Namespace) -> int:
    report = ask_codex(
        CodexEndpointConfig(
            prompt=args.prompt,
            repo_root=args.repo_root,
            run_root=args.run_root,
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    if args.text and report["ok"]:
        print(Path(report["response"]).read_text(encoding="utf-8").strip())
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="ask-codex-luna")
    configure_parser(parser)
    return run_from_namespace(parser.parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
