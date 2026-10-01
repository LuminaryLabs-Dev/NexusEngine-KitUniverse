from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional

from workflow_harnesses.rawg_capability_pipeline.contracts import stable_hash

from .ast import load_ast
from .codex_runner import resolve_codex_binary
from .domain_architecture import is_architectural_domain
from .workflow import _git_state


TARGET_KITS = 100
TARGET_DOMAINS = 100


def capture_baseline(repo_root: Path, workspace: Path, ast_path: Path, run_root: Path) -> Path:
    ast = load_ast(ast_path)
    inventory = collect_inventory(workspace)
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    manifest = _read_json(workspace / "manifest.json")
    baseline = {
        "schema_version": "kit-universe-it.baseline.v2",
        "run_id": run_id,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "goal": {
            "new_runtime_proven_kits": TARGET_KITS,
            "new_validated_top_level_domains": TARGET_DOMAINS,
        },
        "repo": _git_state(repo_root),
        "workflow": {
            "ast_path": str(ast_path.resolve()),
            "ast_hash": stable_hash(ast),
            "workflow_id": ast["workflow_id"],
            "workspace": str(workspace.resolve()),
            "prior_pipeline_epoch": manifest.get("pipeline_epoch"),
            "codex_review": _codex_node(ast, "codex.review-master-kits"),
            "codex_author": _codex_node(ast, "codex.author-runtime-kits"),
        },
        "corpus": {
            "expected": 881_069,
            "game_evidence_maps": _line_count(workspace / "shards", "game-evidence-maps"),
            "game_pointer_maps": _line_count(workspace / "shards", "game-kit-pointer-maps"),
            "completed_extraction_pages": _line_count(workspace / "shards", "interaction-page-ledger"),
            "completed_local_refinements": _line_count(workspace / "shards", "master-refine-ledger"),
        },
        "inventory": inventory,
        "provider": _provider_status(ast),
        "safety": {
            "free_gib": round(shutil.disk_usage(workspace).free / (1024 ** 3), 3),
            "min_free_gib": float((ast.get("controls") or {}).get("min_free_gib", 10.0)),
        },
        "deltas": {"runtime_proven_kits": 0, "validated_top_level_domains": 0},
    }
    _write_json(run_dir / "baseline.json", baseline)
    return run_dir / "baseline.json"


def reconcile_baseline(baseline_path: Path, workspace: Path) -> Dict[str, Any]:
    baseline = _read_json(baseline_path)
    current = collect_inventory(workspace)
    baseline_kits = set(baseline.get("inventory", {}).get("runtime_proven_master_ids") or [])
    baseline_domains = set(baseline.get("inventory", {}).get("validated_top_level_domain_ids") or [])
    current_kits = set(current["runtime_proven_master_ids"])
    current_domains = set(current["validated_top_level_domain_ids"])
    new_kits = sorted(current_kits - baseline_kits)
    new_domains = sorted(current_domains - baseline_domains)
    success = len(new_kits) >= TARGET_KITS and len(new_domains) >= TARGET_DOMAINS
    status = {
        "schema_version": "kit-universe-it.status.v2",
        "baseline": str(baseline_path.resolve()),
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "success": success,
        "status": "complete" if success else "running",
        "kit_delta": len(new_kits),
        "domain_delta": len(new_domains),
        "new_runtime_proven_master_ids": new_kits,
        "new_validated_top_level_domain_ids": new_domains,
        "current_inventory": current,
        "free_gib": round(shutil.disk_usage(workspace).free / (1024 ** 3), 3),
    }
    _write_json(baseline_path.parent / "status.json", status)
    return status


def collect_inventory(workspace: Path) -> Dict[str, Any]:
    shards = workspace / "shards"
    accepted = {
        str(item["master_kit_id"])
        for item in _read_jsonl(shards.glob("master-codex-decisions-*.jsonl"))
        if item.get("accepted") is True and item.get("master_kit_id")
    }
    runtime_proven = {
        str(item["master_kit_id"])
        for item in _read_jsonl(shards.glob("runtime-build-ledger-*.jsonl"))
        if item.get("status") == "runtime-proven" and item.get("master_kit_id")
    }
    package_ids = []
    for descriptor_path in sorted((workspace / "kits").glob("*/kit.json")):
        descriptor = _read_json(descriptor_path)
        if descriptor.get("kit_id"):
            package_ids.append(str(descriptor["kit_id"]))
    memberships = []
    for item in _read_jsonl(shards.glob("domain-placement-ledger-*.jsonl")):
        if item.get("master_kit_id") in runtime_proven:
            memberships.append({
                "identity": item.get("identity"),
                "master_kit_id": item.get("master_kit_id"),
                "kit_id": item.get("kit_id"),
                "domain": item.get("domain"),
                "subdomain": item.get("subdomain"),
            })
    audit = _read_json(workspace / "domain-architecture-report.json")
    validated_domains = (
        sorted(
            domain
            for domain in (audit.get("domain_counts") or {})
            if is_architectural_domain(domain)
        )
        if audit.get("ok") is True
        else []
    )
    return {
        "codex_accepted_master_ids": sorted(accepted),
        "runtime_proven_master_ids": sorted(runtime_proven),
        "kit_package_ids": sorted(set(package_ids)),
        "validated_top_level_domain_ids": validated_domains,
        "domain_memberships": sorted(memberships, key=lambda item: str(item.get("identity") or "")),
        "architecture_audit_ok": audit.get("ok") is True,
        "architecture_report": str(workspace / "domain-architecture-report.json") if audit else None,
    }


def _provider_status(ast: Dict[str, Any]) -> Dict[str, Any]:
    controls = ast.get("controls") or {}
    base_url = str(controls.get("base_url") or "").rstrip("/")
    models = []
    endpoint_error = None
    if base_url:
        try:
            with urllib.request.urlopen(f"{base_url}/models", timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
            models = sorted(str(item.get("id")) for item in payload.get("data") or [] if item.get("id"))
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            endpoint_error = str(exc)
    binary = resolve_codex_binary()
    codex_version = None
    if binary is not None:
        process = subprocess.run([str(binary), "--version"], capture_output=True, text=True, timeout=10, check=False)
        codex_version = process.stdout.strip() if process.returncode == 0 else None
    return {
        "local": {"base_url": base_url, "ok": endpoint_error is None and bool(models), "models": models, "error": endpoint_error},
        "codex": {"ok": binary is not None and codex_version is not None, "binary": str(binary) if binary else None, "version": codex_version},
    }


def _codex_node(ast: Dict[str, Any], operation: str) -> Dict[str, Any] | None:
    for node in ast.get("nodes") or []:
        if node.get("op") == operation:
            return dict(node.get("config") or {})
    return None


def _line_count(shards: Path, stem: str) -> int:
    total = 0
    for path in sorted(shards.glob(f"{stem}-*.jsonl")):
        with path.open(encoding="utf-8") as handle:
            total += sum(1 for _ in handle)
    return total


def _read_jsonl(paths: Iterable[Path]) -> Iterator[Dict[str, Any]]:
    for path in sorted(paths):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def configure_parser(parser: argparse.ArgumentParser) -> None:
    subcommands = parser.add_subparsers(dest="command", required=True)
    baseline = subcommands.add_parser("baseline")
    baseline.add_argument("--repo-root", type=Path, default=Path.cwd())
    baseline.add_argument("--workspace", type=Path, required=True)
    baseline.add_argument("--ast", type=Path, required=True)
    baseline.add_argument("--run-root", type=Path, default=Path("runs/rawg-881k/kit-universe-it"))
    status = subcommands.add_parser("status")
    status.add_argument("--baseline", type=Path, required=True)
    status.add_argument("--workspace", type=Path, required=True)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="rawg-exhaustive-campaign")
    configure_parser(parser)
    args = parser.parse_args(argv)
    if args.command == "baseline":
        path = capture_baseline(args.repo_root.resolve(), args.workspace.resolve(), args.ast.resolve(), args.run_root.resolve())
        print(json.dumps({"ok": True, "baseline": str(path)}, indent=2, sort_keys=True))
        return 0
    status = reconcile_baseline(args.baseline.resolve(), args.workspace.resolve())
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0 if status.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
