from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REPORT_SCHEMA = "kituniverse.kit-organization-report.v1"
VALID_ROLES = {"atomic", "policy", "adapter", "assembly", "app"}
VALID_TIERS = {"core", "protokit-candidate", "application"}
CATCH_ALLS = {"general", "generic", "mechanics", "misc", "miscellaneous", "other", "unknown", "unclassified"}


def _slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def _as_strings(value: Any) -> List[str]:
    if value is None:
        return []
    values = value if isinstance(value, list) else [value]
    return sorted({str(item).strip() for item in values if str(item).strip()})


def _read_json(path: Path) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, str(exc)
    if not isinstance(value, dict):
        return None, "expected a JSON object"
    return value, None


def _explicit_value(descriptor: Dict[str, Any], names: Iterable[str]) -> Any:
    metadata = descriptor.get("metadata") if isinstance(descriptor.get("metadata"), dict) else {}
    contract = descriptor.get("contract") if isinstance(descriptor.get("contract"), dict) else {}
    for name in names:
        for source in (descriptor, metadata, contract):
            if source.get(name) not in (None, ""):
                return source[name]
    return None


def _classify_role(descriptor: Dict[str, Any]) -> tuple[str, str]:
    explicit = _slug(_explicit_value(descriptor, ("kit_role", "role")))
    if explicit in VALID_ROLES:
        return explicit, "explicit"
    promotion = _slug(_explicit_value(descriptor, ("promotion_tier", "tier")))
    if promotion == "application":
        return "app", "promotion-tier"
    children = _as_strings(
        _explicit_value(descriptor, ("child_kit_ids", "children", "internal_kits", "composes"))
    )
    if children:
        return "assembly", "declared-children"
    terms = " ".join(
        _slug(value)
        for value in (
            descriptor.get("kit_id"),
            descriptor.get("domain"),
            descriptor.get("subdomain"),
            (descriptor.get("contract") or {}).get("name"),
        )
    )
    if "adapter" in terms:
        return "adapter", "name-signal"
    if "policy" in terms:
        return "policy", "name-signal"
    return "atomic", "conservative-default"


def _classify_tier(descriptor: Dict[str, Any], role: str) -> tuple[str, str]:
    explicit = _slug(_explicit_value(descriptor, ("promotion_tier", "tier")))
    aliases = {
        "core-candidate": "core",
        "protokit": "protokit-candidate",
        "proto-kit": "protokit-candidate",
        "app": "application",
    }
    explicit = aliases.get(explicit, explicit)
    if explicit in VALID_TIERS:
        return explicit, "explicit"
    if role == "app":
        return "application", "kit-role"
    domain = _slug(descriptor.get("domain"))
    if domain == "core" or domain.startswith("core-"):
        return "core", "core-domain-prefix"
    return "protokit-candidate", "safe-default"


def _proof_status(package_root: Path, descriptor: Dict[str, Any]) -> tuple[str, Optional[str]]:
    value = descriptor.get("runtime_proof")
    if not value:
        return "missing", None
    path = Path(str(value))
    if not path.is_absolute():
        path = package_root / path
    proof, error = _read_json(path)
    if error:
        return "unreadable", str(path)
    return ("passing" if proof.get("ok") is True else "failing"), str(path)


def _membership_records(domains_root: Path) -> tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    records: List[Dict[str, Any]] = []
    findings: List[Dict[str, str]] = []
    if not domains_root.exists():
        return records, findings
    for path in sorted(domains_root.glob("**/*.json")):
        value, error = _read_json(path)
        if error:
            findings.append({"severity": "error", "code": "unreadable-domain-json", "path": str(path), "detail": error})
            continue
        if value.get("kit_id"):
            records.append({**value, "membership_path": str(path)})
    return records, findings


def organize_dry_run(workspace: Path, run_root: Path) -> Dict[str, Any]:
    workspace = workspace.resolve()
    kits_root = workspace / "kits"
    domains_root = workspace / "domains"
    memberships, findings = _membership_records(domains_root)
    memberships_by_kit: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for membership in memberships:
        memberships_by_kit[str(membership["kit_id"])].append(membership)

    classifications: List[Dict[str, Any]] = []
    seen_ids: Counter[str] = Counter()
    package_roots = sorted(path for path in kits_root.glob("*") if path.is_dir()) if kits_root.exists() else []
    for package_root in package_roots:
        descriptor_path = package_root / "kit.json"
        descriptor, error = _read_json(descriptor_path)
        if error:
            findings.append({"severity": "error", "code": "unreadable-kit-descriptor", "path": str(descriptor_path), "detail": error})
            continue
        kit_id = str(descriptor.get("kit_id") or descriptor.get("id") or package_root.name)
        seen_ids[kit_id] += 1
        domain = _slug(descriptor.get("domain") or (descriptor.get("contract") or {}).get("domain"))
        subdomain = _slug(descriptor.get("subdomain") or (descriptor.get("contract") or {}).get("subdomain"))
        expected_domain_path = f"n:{domain}:{subdomain}" if domain and subdomain else None
        domain_path = descriptor.get("domain_path") or expected_domain_path
        role, role_basis = _classify_role(descriptor)
        tier, tier_basis = _classify_tier(descriptor, role)
        requires = _as_strings(_explicit_value(descriptor, ("requires",)))
        provides = _as_strings(_explicit_value(descriptor, ("provides",)))
        child_ids = _as_strings(
            _explicit_value(descriptor, ("child_kit_ids", "children", "internal_kits", "composes"))
        )
        parent_id = _explicit_value(descriptor, ("parent_kit_id", "parent"))
        contract = descriptor.get("contract") if isinstance(descriptor.get("contract"), dict) else {}
        idempotency = bool(str(contract.get("idempotency") or descriptor.get("idempotency") or "").strip())
        snapshot_reset = bool(str(contract.get("reset_snapshot") or descriptor.get("reset_snapshot") or "").strip())
        proof_status, proof_path = _proof_status(package_root, descriptor)
        current_memberships = memberships_by_kit.get(kit_id, [])
        provenance = bool(descriptor.get("source_id") or descriptor.get("source_context"))
        source_context = descriptor.get("source_context") if isinstance(descriptor.get("source_context"), dict) else {}
        core_kits_reused = sorted({
            *[token for token in requires if token.startswith("n:core-")],
            *_as_strings(_explicit_value(descriptor, ("core_kits_reused",))),
        })

        if not domain or not subdomain:
            findings.append({"severity": "error", "code": "missing-domain-boundary", "kit_id": kit_id})
        if domain in CATCH_ALLS or subdomain in CATCH_ALLS:
            findings.append({"severity": "error", "code": "catch-all-domain-boundary", "kit_id": kit_id})
        if domain_path != expected_domain_path:
            findings.append({"severity": "error", "code": "descriptor-domain-path-mismatch", "kit_id": kit_id})
        if descriptor.get("schema_version") == "kituniverse.kit-descriptor.v2":
            for field in (
                "kit_role", "promotion_tier", "visibility", "parent_kit_id", "child_kit_ids",
                "core_kits_reused", "requires", "provides", "child_idempotency_proof",
            ):
                if field not in descriptor:
                    findings.append({"severity": "error", "code": "missing-explicit-architecture-field", "kit_id": kit_id, "detail": field})
        if len(current_memberships) != 1:
            findings.append({"severity": "error", "code": "membership-count", "kit_id": kit_id, "detail": str(len(current_memberships))})
        elif _slug(current_memberships[0].get("domain")) != domain or _slug(current_memberships[0].get("subdomain")) != subdomain:
            findings.append({"severity": "error", "code": "membership-domain-mismatch", "kit_id": kit_id})
        if proof_status != "passing":
            findings.append({"severity": "error", "code": "runtime-proof-not-passing", "kit_id": kit_id, "detail": proof_status})
        if not idempotency:
            findings.append({"severity": "error", "code": "missing-idempotency-contract", "kit_id": kit_id})
        if not snapshot_reset:
            findings.append({"severity": "error", "code": "missing-snapshot-reset-contract", "kit_id": kit_id})
        if not provenance:
            findings.append({"severity": "error", "code": "missing-source-provenance", "kit_id": kit_id})
        if role == "assembly" and not child_ids:
            findings.append({"severity": "error", "code": "assembly-without-declared-children", "kit_id": kit_id})
        if role != "assembly" and child_ids:
            findings.append({"severity": "error", "code": "non-assembly-with-children", "kit_id": kit_id})
        if role == "assembly" and not bool(descriptor.get("child_idempotency_proof")):
            findings.append({"severity": "warning", "code": "assembly-child-idempotency-unproven", "kit_id": kit_id})

        classifications.append(
            {
                "kit_id": kit_id,
                "package_root": str(package_root),
                "descriptor": str(descriptor_path),
                "descriptor_schema": descriptor.get("schema_version"),
                "domain": domain,
                "subdomain": subdomain,
                "domain_path": domain_path,
                "parent_domain_path": descriptor.get("parent_domain_path"),
                "kit_role": role,
                "kit_role_basis": role_basis,
                "promotion_tier": tier,
                "promotion_tier_basis": tier_basis,
                "public_or_internal": _slug(_explicit_value(descriptor, ("visibility",))) or "public",
                "parent_kit_id": str(parent_id) if parent_id else None,
                "child_kit_ids": child_ids,
                "requires": requires,
                "provides": provides,
                "idempotency_declared": idempotency,
                "snapshot_reset_declared": snapshot_reset,
                "runtime_proof_status": proof_status,
                "runtime_proof": proof_path,
                "source_provenance": provenance,
                "source_evidence": {
                    "source_id": descriptor.get("source_id") or source_context.get("source_id"),
                    "evidence_id": source_context.get("evidence_id"),
                    "evidence_field": source_context.get("evidence_field"),
                    "support_count": source_context.get("support_count"),
                },
                "core_kits_reused": core_kits_reused,
                "membership_count": len(current_memberships),
                "membership_paths": [item["membership_path"] for item in current_memberships],
            }
        )

    for kit_id, count in seen_ids.items():
        if count > 1:
            findings.append({"severity": "error", "code": "duplicate-kit-id", "kit_id": kit_id, "detail": str(count)})
    known_ids = set(seen_ids)
    for membership in memberships:
        if str(membership["kit_id"]) not in known_ids:
            findings.append({"severity": "error", "code": "membership-without-kit", "kit_id": str(membership["kit_id"]), "path": membership["membership_path"]})
    for item in classifications:
        for child_id in item["child_kit_ids"]:
            if child_id not in known_ids:
                findings.append({"severity": "error", "code": "missing-child-kit", "kit_id": item["kit_id"], "detail": child_id})
        if item["parent_kit_id"] and item["parent_kit_id"] not in known_ids:
            findings.append({"severity": "error", "code": "missing-parent-kit", "kit_id": item["kit_id"], "detail": item["parent_kit_id"]})

    domain_tree: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    for item in classifications:
        if item["domain"] and item["subdomain"]:
            domain_tree[item["domain"]][item["subdomain"]].append(item["kit_id"])
    rendered_tree = {
        domain: {subdomain: sorted(ids) for subdomain, ids in sorted(subdomains.items())}
        for domain, subdomains in sorted(domain_tree.items())
    }
    domain_purposes: Dict[str, Any] = {}
    for domain, subdomains in rendered_tree.items():
        domain_descriptor, _ = _read_json(domains_root / domain / "domain.json")
        domain_purposes[domain] = {
            "purpose": (domain_descriptor or {}).get("purpose"),
            "subdomains": {},
        }
        for subdomain in subdomains:
            subdomain_descriptor, _ = _read_json(domains_root / domain / subdomain / "subdomain.json")
            domain_purposes[domain]["subdomains"][subdomain] = (subdomain_descriptor or {}).get("purpose")
    edges = []
    for item in classifications:
        edges.extend({"from": item["kit_id"], "to": child, "kind": "composes"} for child in item["child_kit_ids"])
        edges.extend({"from": item["kit_id"], "to": token, "kind": "requires"} for token in item["requires"])

    error_count = sum(item["severity"] == "error" for item in findings)
    warning_count = sum(item["severity"] == "warning" for item in findings)
    report = {
        "schema_version": REPORT_SCHEMA,
        "mode": "dry-run",
        "ok": bool(classifications) and error_count == 0,
        "workspace": str(workspace),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "kits": len(classifications),
            "domains": len(domain_tree),
            "subdomains": sum(len(value) for value in domain_tree.values()),
            "memberships": len(memberships),
            "roles": dict(sorted(Counter(item["kit_role"] for item in classifications).items())),
            "promotion_tiers": dict(sorted(Counter(item["promotion_tier"] for item in classifications).items())),
            "errors": error_count,
            "warnings": warning_count,
        },
        "domain_list": sorted(rendered_tree),
        "subdomain_list": [
            f"{domain}/{subdomain}"
            for domain, subdomains in rendered_tree.items()
            for subdomain in subdomains
        ],
        "domain_tree": rendered_tree,
        "domain_purposes": domain_purposes,
        "core_kits_reused": sorted({
            token for item in classifications for token in item["core_kits_reused"]
        }),
        "kits": classifications,
        "composition_edges": sorted(edges, key=lambda item: (item["from"], item["kind"], item["to"])),
        "duplicate_and_merge_ledger": [item for item in findings if item["code"] in {"duplicate-kit-id", "non-assembly-with-children"}],
        "findings": sorted(findings, key=lambda item: (item["severity"], item["code"], item.get("kit_id", ""))),
        "mutation_count": 0,
    }
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    report["run_dir"] = str(run_dir)
    (run_dir / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dry-run", action="store_true", required=True, help="Inspect and report without moving or rewriting kits")
    parser.add_argument("--workspace", type=Path, default=Path.cwd(), help="Root containing kits/ and domains/")
    parser.add_argument("--run-root", type=Path, default=Path("runs/kit-organize"))
    parser.add_argument("--output", choices=("summary", "json"), default="summary")


def run_from_namespace(args: argparse.Namespace) -> int:
    report = organize_dry_run(args.workspace, args.run_root)
    if args.output == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(json.dumps({"ok": report["ok"], "run_dir": report["run_dir"], **report["summary"]}, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="kit-organize")
    configure_parser(parser)
    return run_from_namespace(parser.parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
