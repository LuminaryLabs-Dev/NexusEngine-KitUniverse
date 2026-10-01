from __future__ import annotations

import json
import os
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List

from workflow_harnesses.rawg_capability_pipeline.contracts import slug, stable_hash
from workflow_harnesses.rawg_matrix_optimizer.workflow_rawg_matrix_optimizer import ShardedJsonlWriter

from .contracts import (
    DOMAIN_ARCHITECTURE_REPORT_SCHEMA,
    DOMAIN_DESCRIPTOR_SCHEMA,
    DOMAIN_MEMBERSHIP_SCHEMA,
    SUBDOMAIN_DESCRIPTOR_SCHEMA,
    SUPPORTED_KIT_DESCRIPTOR_SCHEMAS,
    validate_kit_descriptor,
)


CATCH_ALL_DOMAINS = {
    "general", "generic", "mechanics", "misc", "miscellaneous", "other", "unknown", "unclassified"
}


def is_architectural_domain(value: Any) -> bool:
    normalized = slug(value)
    return bool(normalized) and normalized not in CATCH_ALL_DOMAINS


def place_domain_architecture(workspace: Path, controls: Dict[str, Any]) -> Dict[str, Any]:
    shards = workspace / "shards"
    proven = _latest_runtime_proven(shards)
    prior = _load_values(shards.glob("domain-placement-ledger-*.jsonl"), "identity")
    writer = ShardedJsonlWriter(shards, "domain-placement-ledger", int(controls["shard_max_bytes"]))
    placed = skipped = 0
    errors: List[Dict[str, str]] = []

    for master_kit_id, build in sorted(proven.items()):
        package_root = Path(build["package_root"])
        descriptor_path = package_root / "kit.json"
        try:
            descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            errors.append({"master_kit_id": master_kit_id, "reason": f"unreadable-kit-descriptor:{exc}"})
            continue
        domain = slug(descriptor.get("domain"))
        subdomain = slug(descriptor.get("subdomain"))
        issue = _placement_issue(descriptor, domain, subdomain)
        if issue:
            errors.append({"master_kit_id": master_kit_id, "reason": issue})
            continue
        identity = stable_hash([
            master_kit_id,
            descriptor["kit_id"],
            descriptor["contract_hash"],
            domain,
            subdomain,
            build["proof_path"],
        ])
        membership = {
            "schema_version": DOMAIN_MEMBERSHIP_SCHEMA,
            "identity": identity,
            "pipeline_epoch": controls.get("pipeline_epoch"),
            "master_kit_id": master_kit_id,
            "kit_id": descriptor["kit_id"],
            "domain": domain,
            "subdomain": subdomain,
            "package_root": str(package_root),
            "kit_descriptor": str(descriptor_path),
            "runtime_proof": build["proof_path"],
            "contract_hash": descriptor["contract_hash"],
            "kit_role": descriptor.get("kit_role", "atomic"),
            "promotion_tier": descriptor.get("promotion_tier", "protokit-candidate"),
            "parent_kit_id": descriptor.get("parent_kit_id"),
            "child_kit_ids": descriptor.get("child_kit_ids", []),
        }
        domain_root = workspace / "domains" / domain
        subdomain_root = domain_root / subdomain
        domain_descriptor = {
            "schema_version": DOMAIN_DESCRIPTOR_SCHEMA,
            "domain": domain,
            "purpose": f"Own reusable {domain.replace('-', ' ')} capabilities without host or renderer glue.",
        }
        subdomain_descriptor = {
            "schema_version": SUBDOMAIN_DESCRIPTOR_SCHEMA,
            "domain": domain,
            "subdomain": subdomain,
            "purpose": f"Own reusable {subdomain.replace('-', ' ')} concerns inside {domain.replace('-', ' ')}.",
        }
        membership_path = subdomain_root / f"{descriptor['kit_id']}.json"
        if identity in prior:
            issue = _existing_membership_issue(membership_path, membership)
            if issue:
                errors.append({"master_kit_id": master_kit_id, "reason": issue})
            else:
                skipped += 1
            continue
        conflict = _write_compatible_descriptor(domain_root / "domain.json", domain_descriptor, ("domain",))
        conflict = conflict or _write_compatible_descriptor(
            subdomain_root / "subdomain.json", subdomain_descriptor, ("domain", "subdomain")
        )
        conflict = conflict or _write_immutable_json(membership_path, membership)
        if conflict:
            errors.append({"master_kit_id": master_kit_id, "reason": conflict})
            continue
        writer.append(membership)
        prior.add(identity)
        placed += 1

    return {
        "ok": not errors,
        "status": "complete" if not errors else "hold",
        "reason": None if not errors else "domain-placement-errors",
        "runtime_proven": len(proven),
        "new_placements": placed,
        "existing_placements": skipped,
        "errors": errors[:100],
    }


def audit_domain_architecture(workspace: Path, controls: Dict[str, Any]) -> Dict[str, Any]:
    shards = workspace / "shards"
    proven = _latest_runtime_proven(shards)
    accepted = {
        str(item["master_kit_id"])
        for item in _read_jsonl(shards.glob("master-codex-decisions-*.jsonl"))
        if item.get("accepted") is True and item.get("master_kit_id")
    }
    build_requests = {
        str(item.get("source_context", {}).get("master_kit_id")): item
        for item in _read_jsonl(shards.glob("exhaustive-build-requests-*.jsonl"))
        if item.get("source_context", {}).get("master_kit_id")
    }
    placements = list(_read_jsonl(shards.glob("domain-placement-ledger-*.jsonl")))
    by_master: Dict[str, List[Dict[str, Any]]] = {}
    for item in placements:
        by_master.setdefault(str(item.get("master_kit_id")), []).append(item)
    errors: List[Dict[str, str]] = []
    kit_ids: Counter[str] = Counter()
    domain_counts: Counter[str] = Counter()
    subdomain_counts: Counter[str] = Counter()

    for master_kit_id in sorted(accepted - set(build_requests)):
        errors.append({"master_kit_id": master_kit_id, "reason": "accepted-without-build-request"})
    for master_kit_id, request in sorted(build_requests.items()):
        if master_kit_id in accepted and request.get("build_status") != "queued":
            errors.append({"master_kit_id": master_kit_id, "reason": "accepted-contract-requires-domain-repair"})
    for master_kit_id in sorted(accepted - set(proven)):
        errors.append({"master_kit_id": master_kit_id, "reason": "accepted-without-runtime-proof"})
    for master_kit_id in sorted(set(proven) - accepted):
        errors.append({"master_kit_id": master_kit_id, "reason": "runtime-proven-without-codex-acceptance"})

    for master_kit_id, build in sorted(proven.items()):
        package_root = Path(build["package_root"])
        descriptor_path = package_root / "kit.json"
        proof_path = Path(build["proof_path"])
        if package_root.parent != workspace / "kits":
            errors.append({"master_kit_id": master_kit_id, "reason": "runtime-proven-kit-outside-kits-folder"})
        try:
            descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            errors.append({"master_kit_id": master_kit_id, "reason": "unreadable-kit-descriptor"})
            continue
        rows = [
            row for row in by_master.get(master_kit_id, [])
            if row.get("contract_hash") == descriptor.get("contract_hash")
            and row.get("runtime_proof") == build.get("proof_path")
        ]
        if len(rows) != 1:
            errors.append({"master_kit_id": master_kit_id, "reason": f"current-placement-count:{len(rows)}"})
            continue
        row = rows[0]
        try:
            proof = json.loads(proof_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            errors.append({"master_kit_id": master_kit_id, "reason": "unreadable-runtime-proof"})
            continue
        if proof.get("ok") is not True:
            errors.append({"master_kit_id": master_kit_id, "reason": "runtime-proof-not-passing"})
        domain = slug(descriptor.get("domain"))
        subdomain = slug(descriptor.get("subdomain"))
        issue = _placement_issue(descriptor, domain, subdomain)
        if issue:
            errors.append({"master_kit_id": master_kit_id, "reason": issue})
        if row.get("domain") != domain or row.get("subdomain") != subdomain:
            errors.append({"master_kit_id": master_kit_id, "reason": "placement-contract-mismatch"})
        membership_path = workspace / "domains" / domain / subdomain / f"{descriptor['kit_id']}.json"
        membership_issue = _existing_membership_issue(membership_path, row)
        if membership_issue:
            errors.append({"master_kit_id": master_kit_id, "reason": membership_issue})
        domain_issue = _descriptor_file_issue(
            workspace / "domains" / domain / "domain.json",
            DOMAIN_DESCRIPTOR_SCHEMA,
            {"domain": domain},
        )
        if domain_issue:
            errors.append({"master_kit_id": master_kit_id, "reason": domain_issue})
        subdomain_issue = _descriptor_file_issue(
            workspace / "domains" / domain / subdomain / "subdomain.json",
            SUBDOMAIN_DESCRIPTOR_SCHEMA,
            {"domain": domain, "subdomain": subdomain},
        )
        if subdomain_issue:
            errors.append({"master_kit_id": master_kit_id, "reason": subdomain_issue})
        kit_ids[descriptor["kit_id"]] += 1
        domain_counts[domain] += 1
        subdomain_counts[f"{domain}/{subdomain}"] += 1

    for kit_id, count in kit_ids.items():
        if count > 1:
            errors.append({"kit_id": kit_id, "reason": f"duplicate-kit-id:{count}"})
    known_packages = {Path(item["package_root"]).resolve() for item in proven.values()}
    for package_root in sorted((workspace / "kits").glob("*")) if (workspace / "kits").exists() else []:
        if package_root.is_dir() and package_root.resolve() not in known_packages:
            errors.append({"kit_id": package_root.name, "reason": "orphan-kit-package"})

    report = {
        "schema_version": DOMAIN_ARCHITECTURE_REPORT_SCHEMA,
        "pipeline_epoch": controls.get("pipeline_epoch"),
        "ok": bool(proven) and not errors,
        "status": "complete" if proven and not errors else "hold",
        "reason": None if proven and not errors else ("no-runtime-proven-kits" if not proven else "domain-architecture-errors"),
        "runtime_proven_kits": len(proven),
        "codex_accepted_kits": len(accepted),
        "build_requests": len(build_requests),
        "placed_kits": len(proven) - sum(
            1 for item in errors if item.get("reason", "").startswith("current-placement-count:")
        ),
        "domains": len(domain_counts),
        "subdomains": len(subdomain_counts),
        "domain_counts": dict(sorted(domain_counts.items())),
        "subdomain_counts": dict(sorted(subdomain_counts.items())),
        "errors": errors[:1000],
    }
    reports_root = workspace / "domain-architecture-reports"
    reports_root.mkdir(parents=True, exist_ok=True)
    epoch = str(controls.get("pipeline_epoch") or "unknown")
    _write_json(reports_root / f"{epoch}.json", report)
    _write_json(workspace / "domain-architecture-report.json", report)
    return report


def _placement_issue(descriptor: Dict[str, Any], domain: str, subdomain: str) -> str | None:
    if descriptor.get("schema_version") not in SUPPORTED_KIT_DESCRIPTOR_SCHEMAS:
        return "invalid-kit-descriptor-schema"
    descriptor_errors = validate_kit_descriptor(descriptor, allow_legacy=True)
    if descriptor_errors:
        return descriptor_errors[0]
    if not domain or not subdomain:
        return "missing-domain-or-subdomain"
    if not is_architectural_domain(domain) or not is_architectural_domain(subdomain):
        return "catch-all-domain-boundary"
    contract = descriptor.get("contract") or {}
    if slug(contract.get("domain")) != domain or slug(contract.get("subdomain")) != subdomain:
        return "descriptor-contract-domain-mismatch"
    if not str(contract.get("owns") or "").strip():
        return "missing-domain-purpose"
    return None


def _latest_runtime_proven(shards: Path) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for item in _read_jsonl(shards.glob("runtime-build-ledger-*.jsonl")):
        if item.get("status") == "runtime-proven" and item.get("master_kit_id"):
            latest[str(item["master_kit_id"])] = item
    return latest


def _read_jsonl(paths: Iterable[Path]) -> Iterator[Dict[str, Any]]:
    for path in sorted(paths):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def _load_values(paths: Iterable[Path], key: str) -> set[str]:
    return {str(item[key]) for item in _read_jsonl(paths) if item.get(key) is not None}


def _write_immutable_json(path: Path, value: Dict[str, Any]) -> str | None:
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists():
        try:
            return None if path.read_text(encoding="utf-8") == rendered else f"immutable-path-conflict:{path}"
        except OSError as exc:
            return f"unreadable-immutable-path:{path}:{exc}"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    os.replace(temporary, path)
    return None


def _write_compatible_descriptor(
    path: Path, value: Dict[str, Any], identity_keys: tuple[str, ...]
) -> str | None:
    if not path.exists():
        return _write_immutable_json(path, value)
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return f"unreadable-immutable-path:{path}:{exc}"
    if existing.get("schema_version") != value.get("schema_version"):
        return f"immutable-descriptor-schema-conflict:{path}"
    if any(existing.get(key) != value.get(key) for key in identity_keys):
        return f"immutable-descriptor-identity-conflict:{path}"
    if not str(existing.get("purpose") or "").strip():
        return f"immutable-descriptor-missing-purpose:{path}"
    return None


def _existing_membership_issue(path: Path, expected: Dict[str, Any]) -> str | None:
    if not path.is_file():
        return f"missing-domain-membership-file:{path}"
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return f"unreadable-domain-membership-file:{path}:{exc}"
    if existing.get("schema_version") != DOMAIN_MEMBERSHIP_SCHEMA:
        return f"domain-membership-schema-mismatch:{path}"
    for key in (
        "identity",
        "master_kit_id",
        "kit_id",
        "domain",
        "subdomain",
        "contract_hash",
        "runtime_proof",
    ):
        if existing.get(key) != expected.get(key):
            return f"domain-membership-{key.replace('_', '-')}-mismatch:{path}"
    return None


def _descriptor_file_issue(
    path: Path, schema_version: str, expected_identity: Dict[str, str]
) -> str | None:
    if not path.is_file():
        return f"missing-domain-descriptor:{path}"
    try:
        descriptor = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return f"unreadable-domain-descriptor:{path}:{exc}"
    if descriptor.get("schema_version") != schema_version:
        return f"domain-descriptor-schema-mismatch:{path}"
    if any(descriptor.get(key) != value for key, value in expected_identity.items()):
        return f"domain-descriptor-identity-mismatch:{path}"
    if not str(descriptor.get("purpose") or "").strip():
        return f"domain-descriptor-missing-purpose:{path}"
    return None


def _write_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)
