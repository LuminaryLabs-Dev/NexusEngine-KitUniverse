from __future__ import annotations

from pathlib import Path
from typing import Any, Dict


def build_handoff_manifest(
    run_dir: Path,
    final_bucket_run_dir: str,
    target_count: int,
    build_batch_manifest: Dict[str, Any],
    build_work_orders_report: Dict[str, Any],
    build_batch_packets_report: Dict[str, Any],
    build_promotion_index: Dict[str, Any],
    promoted_batches_report: Dict[str, Any],
    downstream_build_chain_integrity: Dict[str, Any],
    router_stats: Dict[str, Any],
) -> Dict[str, Any]:
    artifacts = {
        "manifest": "manifest.json",
        "research_pack": "research-pack.json",
        "stage_contracts": "stage-contracts.json",
        "first_stage_breadth_audit": "first-stage-breadth-audit.json",
        "revealed_reduced_audit": "revealed-reduced-audit.json",
        "idea_matrix_audit": "idea-matrix-audit.json",
        "idea_matrix": "idea-matrix.jsonl",
        "filtered_candidates": "filtered-candidates.jsonl",
        "atomic_filter_audit": "atomic-filter-audit.json",
        "domain_merge_input_audit": "domain-merge-input-audit.json",
        "domain_canonicalization_audit": "domain-canonicalization-audit.json",
        "selected_final_records": "selected-final-records.jsonl",
        "source_shape_audit": "source-shape-audit.json",
        "final_jsonl": "final-kits.jsonl",
        "final_bucket": final_bucket_run_dir,
        "final_bucket_reconciliation": "final-bucket-reconciliation.json",
        "feed_forward_artifact_integrity": "feed-forward-artifact-integrity.json",
        "stage_contract_integrity": "stage-contract-integrity.json",
        "final_lineage_integrity": "final-lineage-integrity.json",
        "merge_review_coverage": "merge-review-coverage.json",
        "prompt_control_indirection": "prompt-control-indirection.json",
        "slot_decision_trace_integrity": "slot-decision-trace-integrity.json",
        "stage_resume_plan": "stage-resume-plan.json",
        "artifact_schema_index": "artifact-schema-index.json",
        "build_batches": "build-batches.json",
        "build_work_orders": "build-work-orders.jsonl",
        "build_inputs_index": "build-inputs/index.json",
        "batch_results_index": "batch-results/index.json",
        "promotion_index": "build-promotion-index.json",
        "promoted_batches_index": "promoted-batches/index.json",
        "downstream_build_chain_integrity": "downstream-build-chain-integrity.json",
        "lfm_slot_decisions": "lfm-slot-decisions.json",
        "simulator_smoke": "simulator-slot-smoke.json",
        "objective_audit": "objective-audit.json",
        "stage_ledger": "stage-ledger.json",
        "report": "report.json",
        "report_markdown": "report.md",
    }
    deferred_artifacts = {
        "lfm_slot_decisions",
        "objective_audit",
        "report",
        "report_markdown",
        "simulator_smoke",
        "stage_ledger",
    }
    missing_artifacts = [
        name
        for name, relative_path in artifacts.items()
        if name not in deferred_artifacts and name != "final_bucket" and not (run_dir / relative_path).exists()
    ]
    checks = [
        _check(
            "final-count",
            "handoff manifest covers the configured final record count",
            target_count > 0,
            {"target_count": target_count},
        ),
        _check(
            "batch-chain-ready",
            "batch, work order, packet, promotion, and promoted counts align",
            build_batch_manifest.get("batch_count", 0)
            == build_work_orders_report.get("work_order_count", -1)
            == build_batch_packets_report.get("packet_count", -2)
            == build_promotion_index.get("ready_count", -3)
            == promoted_batches_report.get("promoted_count", -4),
            {
                "batch_count": build_batch_manifest.get("batch_count", 0),
                "work_order_count": build_work_orders_report.get("work_order_count", 0),
                "packet_count": build_batch_packets_report.get("packet_count", 0),
                "ready_count": build_promotion_index.get("ready_count", 0),
                "promoted_count": promoted_batches_report.get("promoted_count", 0),
            },
        ),
        _check(
            "downstream-chain-integrity",
            "downstream build-chain integrity proves exact final-record coverage through every handoff",
            downstream_build_chain_integrity.get("ok"),
            {
                "counts": downstream_build_chain_integrity.get("counts"),
                "failed": downstream_build_chain_integrity.get("failed"),
            },
        ),
        _check(
            "record-coverage",
            "promoted batches cover every final record",
            promoted_batches_report.get("record_count", 0) == target_count
            and downstream_build_chain_integrity.get("counts", {}).get("promoted_records") == target_count,
            {
                "promoted_records": promoted_batches_report.get("record_count", 0),
                "integrity_promoted_records": downstream_build_chain_integrity.get("counts", {}).get(
                    "promoted_records"
                ),
                "target_count": target_count,
            },
        ),
        _check(
            "controls",
            "router controls stayed within 100 context tokens and 128 max predictions",
            router_stats.get("max_context_tokens", 999) <= 100 and router_stats.get("max_predictions") == 128,
            {
                "max_context_tokens": router_stats.get("max_context_tokens"),
                "max_predictions": router_stats.get("max_predictions"),
            },
        ),
        _check(
            "artifacts-present",
            "handoff references exist in the run directory",
            not missing_artifacts,
            {"missing_artifacts": missing_artifacts},
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "handoff-manifest",
        "run_dir": str(run_dir),
        "target_count": target_count,
        "artifact_map": artifacts,
        "summary": {
            "batch_count": build_batch_manifest.get("batch_count", 0),
            "work_order_count": build_work_orders_report.get("work_order_count", 0),
            "packet_count": build_batch_packets_report.get("packet_count", 0),
            "promotion_ready_count": build_promotion_index.get("ready_count", 0),
            "promoted_count": promoted_batches_report.get("promoted_count", 0),
            "promoted_records": promoted_batches_report.get("record_count", 0),
            "downstream_chain_integrity": downstream_build_chain_integrity.get("ok"),
            "router_calls_completed": router_stats.get("calls_completed", 0),
            "max_context_tokens": router_stats.get("max_context_tokens"),
            "max_predictions": router_stats.get("max_predictions"),
        },
        "deferred_artifacts": sorted(deferred_artifacts),
        "resume_order": [
            "Read report.md for top-level proof.",
            "Read stage-resume-plan.json for deterministic stage resume entries.",
            "Read artifact-schema-index.json before writing consumers for run artifacts.",
            "Read stage-ledger.json for stage-by-stage gates.",
            "Read build-promotion-index.json or promoted-batches/index.json for downstream batch work.",
            "Load one build-inputs/<batch-id>/kit-records.jsonl at a time for bounded builders.",
        ],
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
