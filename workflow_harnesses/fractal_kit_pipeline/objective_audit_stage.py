from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.fractal_kit_pipeline.run_artifacts import count_jsonl


def build_objective_audit(
    run_dir: Path,
    final_bucket_run_dir: str,
    report: Dict[str, Any],
    stage_ledger: Dict[str, Any],
) -> Dict[str, Any]:
    manifest = _read_json(run_dir / "manifest.json")
    configured_target = manifest.get("target_count", 0)
    final_jsonl = Path(report["final_jsonl"])
    line_count, malformed = count_jsonl(final_jsonl)
    final_records = _read_jsonl(final_jsonl)
    checks = [
        _check(
            "target-count",
            "final JSONL matches the configured target count",
            line_count == configured_target and report.get("final_records", 0) == configured_target,
            {
                "configured_target": configured_target,
                "line_count": line_count,
                "final_records": report.get("final_records", 0),
            },
        ),
        _check(
            "jsonl-safety",
            "final JSONL has zero malformed lines",
            malformed == 0 and report.get("final_jsonl_malformed") == 0,
            {"malformed": malformed, "reported_malformed": report.get("final_jsonl_malformed")},
        ),
        _check(
            "unique-record-ids",
            "final records have no duplicate record ids",
            report.get("duplicate_record_ids") == 0
            and len(final_records) == len({record.get("record_id") for record in final_records}),
            {"duplicate_record_ids": report.get("duplicate_record_ids")},
        ),
        _check(
            "final-bucket",
            "final records were ingested into the sharded final bucket",
            report.get("final_bucket_report", {}).get("ok")
            and report.get("final_bucket_report", {}).get("total_records") == report.get("final_records")
            and not report.get("failed_ingests"),
            {
                "final_bucket_run_dir": final_bucket_run_dir,
                "bucket_total_records": report.get("final_bucket_report", {}).get("total_records"),
                "failed_ingests": len(report.get("failed_ingests", [])),
            },
        ),
        _check(
            "final-bucket-reconciliation",
            "sharded final bucket contains exactly the same record ids as final-kits.jsonl",
            report.get("final_bucket_reconciliation", {}).get("ok"),
            {
                "final_records": report.get("final_bucket_reconciliation", {}).get("final_records"),
                "bucket_records": report.get("final_bucket_reconciliation", {}).get("bucket_records"),
                "missing_from_bucket": report.get("final_bucket_reconciliation", {}).get("missing_from_bucket"),
                "extra_in_bucket": report.get("final_bucket_reconciliation", {}).get("extra_in_bucket"),
                "failed": report.get("final_bucket_reconciliation", {}).get("failed"),
            },
        ),
        _check(
            "feed-forward-artifact-integrity",
            "persisted feed-forward JSONL artifacts are present, parseable, and count-aligned",
            report.get("feed_forward_artifact_integrity", {}).get("ok"),
            {
                "artifact_count": report.get("feed_forward_artifact_integrity", {}).get("artifact_count"),
                "failed": report.get("feed_forward_artifact_integrity", {}).get("failed"),
            },
        ),
        _check(
            "first-stage-breadth-audit",
            "first expansion stage stays broad, exact-unique, loosely relevant, and defers semantic cleanup",
            report.get("first_stage_breadth_audit", {}).get("ok"),
            {
                "accepted_count": report.get("first_stage_breadth_audit", {}).get("accepted_count"),
                "candidate_count": report.get("first_stage_breadth_audit", {}).get("candidate_count"),
                "acceptance_ratio": report.get("first_stage_breadth_audit", {}).get("acceptance_ratio"),
                "rejection_reasons": report.get("first_stage_breadth_audit", {}).get("rejection_reasons"),
                "failed": report.get("first_stage_breadth_audit", {}).get("failed"),
            },
        ),
        _check(
            "revealed-reduced-audit",
            "expansion points feed forward into compact reusable revealed and reduced capability phrases",
            report.get("revealed_reduced_audit", {}).get("ok"),
            {
                "expansion_points": report.get("revealed_reduced_audit", {}).get("expansion_points"),
                "reveal_records": report.get("revealed_reduced_audit", {}).get("reveal_records"),
                "unique_reduced": report.get("revealed_reduced_audit", {}).get("unique_reduced"),
                "generic_reduced": report.get("revealed_reduced_audit", {}).get("generic_reduced"),
                "failed": report.get("revealed_reduced_audit", {}).get("failed"),
            },
        ),
        _check(
            "idea-matrix-audit",
            "deterministic matrix expansion preserves reduced evidence and produces broad unique candidate records",
            report.get("idea_matrix_audit", {}).get("ok"),
            {
                "record_count": report.get("idea_matrix_audit", {}).get("record_count"),
                "unique_semantic_keys": report.get("idea_matrix_audit", {}).get("unique_semantic_keys"),
                "unique_domains": report.get("idea_matrix_audit", {}).get("unique_domains"),
                "unique_reveal_signals": report.get("idea_matrix_audit", {}).get("unique_reveal_signals"),
                "failed": report.get("idea_matrix_audit", {}).get("failed"),
            },
        ),
        _check(
            "atomic-filter-audit",
            "atomic/idempotent filtering preserves a semantic-unique target-sized subset from the matrix",
            report.get("atomic_filter_audit", {}).get("ok"),
            {
                "matrix_records": report.get("atomic_filter_audit", {}).get("matrix_records"),
                "filtered_records": report.get("atomic_filter_audit", {}).get("filtered_records"),
                "unique_semantic_keys": report.get("atomic_filter_audit", {}).get("unique_semantic_keys"),
                "rejected_count": report.get("atomic_filter_audit", {}).get("rejected_count"),
                "failed": report.get("atomic_filter_audit", {}).get("failed"),
            },
        ),
        _check(
            "domain-merge-input-audit",
            "filtered broad candidates form valid domain-index input for recursive merge review",
            report.get("domain_merge_input_audit", {}).get("ok"),
            {
                "filtered_records": report.get("domain_merge_input_audit", {}).get("filtered_records"),
                "unique_domains": report.get("domain_merge_input_audit", {}).get("unique_domains"),
                "unique_parent_domains": report.get("domain_merge_input_audit", {}).get("unique_parent_domains"),
                "pairs_reviewed": report.get("domain_merge_input_audit", {}).get("pairs_reviewed"),
                "failed": report.get("domain_merge_input_audit", {}).get("failed"),
            },
        ),
        _check(
            "domain-canonicalization-audit",
            "reviewed canonical domain metadata is applied while preserving source payload and evidence",
            report.get("domain_canonicalization_audit", {}).get("ok"),
            {
                "filtered_records": report.get("domain_canonicalization_audit", {}).get("filtered_records"),
                "canonicalized_records": report.get("domain_canonicalization_audit", {}).get("canonicalized_records"),
                "canonicalized_count": report.get("domain_canonicalization_audit", {}).get("canonicalized_count"),
                "preserved_count": report.get("domain_canonicalization_audit", {}).get("preserved_count"),
                "failed": report.get("domain_canonicalization_audit", {}).get("failed"),
            },
        ),
        _check(
            "stage-contract-integrity",
            "manifest stage graph, stage-contract artifact, and code contracts agree",
            report.get("stage_contract_integrity", {}).get("ok"),
            {
                "contract_count": report.get("stage_contract_integrity", {}).get("contract_count"),
                "manifest_stage_count": report.get("stage_contract_integrity", {}).get("manifest_stage_count"),
                "artifact_stage_count": report.get("stage_contract_integrity", {}).get("artifact_stage_count"),
                "failed": report.get("stage_contract_integrity", {}).get("failed"),
            },
        ),
        _check(
            "final-lineage-integrity",
            "final records trace back through persisted feed-forward artifacts and reviewed references",
            report.get("final_lineage_integrity", {}).get("ok"),
            {
                "counts": report.get("final_lineage_integrity", {}).get("counts"),
                "failed": report.get("final_lineage_integrity", {}).get("failed"),
            },
        ),
        _check(
            "merge-review-coverage",
            "recursive Y/N domain and kit merge reviews ran cleanly and final records carry canonical metadata",
            report.get("merge_review_coverage", {}).get("ok"),
            {
                "counts": report.get("merge_review_coverage", {}).get("counts"),
                "failed": report.get("merge_review_coverage", {}).get("failed"),
            },
        ),
        _check(
            "prompt-control-indirection",
            "generation prompts stay indirect while router controls and temperature schedule are enforced",
            report.get("prompt_control_indirection", {}).get("ok"),
            {
                "prompt_count": report.get("prompt_control_indirection", {}).get("prompt_count"),
                "failed": report.get("prompt_control_indirection", {}).get("failed"),
            },
        ),
        _check(
            "slot-decision-trace-integrity",
            "sampled LFM slot decision traces cover required slots and agree with simulator smoke",
            report.get("slot_decision_trace_integrity", {}).get("ok"),
            {
                "counts": report.get("slot_decision_trace_integrity", {}).get("counts"),
                "failed": report.get("slot_decision_trace_integrity", {}).get("failed"),
            },
        ),
        _check(
            "stage-resume-plan",
            "every artifact-backed stage has a deterministic resume plan entry",
            report.get("stage_resume_plan", {}).get("ok"),
            {
                "stage_count": report.get("stage_resume_plan", {}).get("stage_count"),
                "deferred_while_building": report.get("stage_resume_plan", {}).get("deferred_while_building"),
                "failed": report.get("stage_resume_plan", {}).get("failed"),
            },
        ),
        _check(
            "artifact-schema-index",
            "major JSON and JSONL artifacts have parseable schema summaries and required fields",
            report.get("artifact_schema_index", {}).get("ok"),
            {
                "jsonl_artifact_count": report.get("artifact_schema_index", {}).get("jsonl_artifact_count"),
                "json_artifact_count": report.get("artifact_schema_index", {}).get("json_artifact_count"),
                "failed": report.get("artifact_schema_index", {}).get("failed"),
            },
        ),
        _check(
            "provider-controls",
            "LM Studio calls stay within 100 context tokens and 128 active predictions",
            manifest.get("max_context_tokens", 999) <= 100
            and manifest.get("max_predictions") == 128
            and report.get("router_stats", {}).get("max_context_tokens", 999) <= 100
            and report.get("router_stats", {}).get("max_predictions") == 128,
            {
                "manifest_max_context_tokens": manifest.get("max_context_tokens"),
                "manifest_max_predictions": manifest.get("max_predictions"),
                "router_stats": report.get("router_stats"),
            },
        ),
        _check(
            "workflow-concurrency",
            "workflow concurrency is fixed at 128",
            manifest.get("concurrency") == 128,
            {"concurrency": manifest.get("concurrency")},
        ),
        _check(
            "temperature-schedule",
            "temperature schedule exists and scales from high expansion to low gates",
            _temperature_schedule_ok(manifest.get("temperature_schedule", {})),
            {"temperature_schedule": manifest.get("temperature_schedule", {})},
        ),
        _check(
            "indirect-generation",
            "model-facing setup keeps the final Nexus kit target out of generation prompts",
            "Nexus" not in manifest.get("list_goal", "") and "kit" not in manifest.get("list_goal", "").lower(),
            {"list_goal": manifest.get("list_goal", "")},
        ),
        _check(
            "atomic-idempotent-final-quality",
            "final quality gate proves atomic/idempotent slot-filled records",
            report.get("final_quality_report", {}).get("ok")
            and report.get("final_quality_report", {}).get("kept_records") == report.get("final_records")
            and report.get("final_quality_report", {}).get("rejected_records") == 0,
            {
                "final_quality_ok": report.get("final_quality_report", {}).get("ok"),
                "kept_records": report.get("final_quality_report", {}).get("kept_records"),
                "rejected_records": report.get("final_quality_report", {}).get("rejected_records"),
            },
        ),
        _check(
            "persisted-idea-matrix",
            "idea matrix stage persists candidate records before filtering",
            (run_dir / "idea-matrix.jsonl").exists()
            and report.get("idea_matrix_records", 0) >= configured_target
            and report.get("filter_report", {}).get("raw_count") == report.get("idea_matrix_records"),
            {
                "idea_matrix_path": str(run_dir / "idea-matrix.jsonl"),
                "idea_matrix_records": report.get("idea_matrix_records"),
                "filter_raw_count": report.get("filter_report", {}).get("raw_count"),
                "configured_target": configured_target,
            },
        ),
        _check(
            "persisted-filtered-candidates",
            "atomic/idempotent filter persists kept candidates before domain merge review",
            (run_dir / "filtered-candidates.jsonl").exists()
            and report.get("filtered_candidate_records", 0) >= configured_target
            and report.get("filter_report", {}).get("kept_count") == report.get("filtered_candidate_records"),
            {
                "filtered_candidates_path": str(run_dir / "filtered-candidates.jsonl"),
                "filtered_candidate_records": report.get("filtered_candidate_records"),
                "filter_kept_count": report.get("filter_report", {}).get("kept_count"),
                "configured_target": configured_target,
            },
        ),
        _check(
            "persisted-final-selection",
            "final selection persists selected records before quality gates",
            (run_dir / "selected-final-records.jsonl").exists()
            and report.get("selected_final_records", 0) == configured_target,
            {
                "selected_final_path": str(run_dir / "selected-final-records.jsonl"),
                "selected_final_records": report.get("selected_final_records"),
                "configured_target": configured_target,
            },
        ),
        _check(
            "source-shape-audit",
            "final records match source-reviewed ProtoKit/domain-kit shape",
            report.get("source_shape_audit", {}).get("ok"),
            {
                "records_checked": report.get("source_shape_audit", {}).get("records_checked"),
                "record_failure_count": report.get("source_shape_audit", {}).get("record_failure_count"),
                "public_sources_reviewed": report.get("source_shape_audit", {}).get("public_sources_reviewed"),
                "failed": report.get("source_shape_audit", {}).get("failed"),
            },
        ),
        _check(
            "diversity-audit",
            "final records are broad and signature-unique enough to build as separate kits",
            report.get("diversity_audit", {}).get("ok"),
            {
                "unique_canonical_domains": report.get("diversity_audit", {}).get("unique_canonical_domains"),
                "unique_categories": report.get("diversity_audit", {}).get("unique_categories"),
                "unique_signatures": report.get("diversity_audit", {}).get("unique_signatures"),
                "max_duplicate_signature_count": report.get("diversity_audit", {}).get(
                    "max_duplicate_signature_count"
                ),
                "failed": report.get("diversity_audit", {}).get("failed"),
            },
        ),
        _check(
            "dependency-graph-audit",
            "final records have coherent requires/provides dependency graph data",
            report.get("dependency_graph_audit", {}).get("ok"),
            {
                "unique_required_tokens": report.get("dependency_graph_audit", {}).get("unique_required_tokens"),
                "unique_provided_tokens": report.get("dependency_graph_audit", {}).get("unique_provided_tokens"),
                "unique_primary_dependencies": report.get("dependency_graph_audit", {}).get(
                    "unique_primary_dependencies"
                ),
                "direct_self_edges": report.get("dependency_graph_audit", {}).get("direct_self_edges"),
                "malformed_tokens": report.get("dependency_graph_audit", {}).get("malformed_tokens"),
                "failed": report.get("dependency_graph_audit", {}).get("failed"),
            },
        ),
        _check(
            "build-batch-manifest",
            "final records are split into bounded build batches with exact-once assignment",
            report.get("build_batch_manifest", {}).get("ok"),
            {
                "batch_count": report.get("build_batch_manifest", {}).get("batch_count"),
                "group_count": report.get("build_batch_manifest", {}).get("group_count"),
                "max_batch_size": report.get("build_batch_manifest", {}).get("max_batch_size"),
                "duplicate_assignments": report.get("build_batch_manifest", {}).get("duplicate_assignments"),
                "missing_assignments": report.get("build_batch_manifest", {}).get("missing_assignments"),
                "failed": report.get("build_batch_manifest", {}).get("failed"),
            },
        ),
        _check(
            "build-batch-replay-smoke",
            "all bounded build batches can be reconstructed independently from final records",
            report.get("build_batch_replay_smoke", {}).get("ok"),
            {
                "batches_replayed": report.get("build_batch_replay_smoke", {}).get("batches_replayed"),
                "records_replayed": report.get("build_batch_replay_smoke", {}).get("records_replayed"),
                "failed_batch_count": report.get("build_batch_replay_smoke", {}).get("failed_batch_count"),
                "failed": report.get("build_batch_replay_smoke", {}).get("failed"),
            },
        ),
        _check(
            "build-work-orders",
            "one bounded downstream work order exists for each build batch",
            report.get("build_work_orders_report", {}).get("ok"),
            {
                "work_order_count": report.get("build_work_orders_report", {}).get("work_order_count"),
                "batch_count": report.get("build_work_orders_report", {}).get("batch_count"),
                "assigned_records": report.get("build_work_orders_report", {}).get("assigned_records"),
                "failed": report.get("build_work_orders_report", {}).get("failed"),
            },
        ),
        _check(
            "build-batch-packets",
            "each bounded work order is materialized as an isolated downstream input packet",
            report.get("build_batch_packets_report", {}).get("ok"),
            {
                "packet_count": report.get("build_batch_packets_report", {}).get("packet_count"),
                "work_order_count": report.get("build_batch_packets_report", {}).get("work_order_count"),
                "packet_records": report.get("build_batch_packets_report", {}).get("packet_records"),
                "missing_record_refs": len(
                    report.get("build_batch_packets_report", {}).get("missing_record_refs", [])
                ),
                "failed": report.get("build_batch_packets_report", {}).get("failed"),
            },
        ),
        _check(
            "build-batch-dry-run",
            "each isolated build input packet can be consumed by a deterministic batch builder dry run",
            report.get("build_batch_dry_run_report", {}).get("ok"),
            {
                "batch_count": report.get("build_batch_dry_run_report", {}).get("batch_count"),
                "packet_count": report.get("build_batch_dry_run_report", {}).get("packet_count"),
                "records_checked": report.get("build_batch_dry_run_report", {}).get("records_checked"),
                "failed_batch_count": report.get("build_batch_dry_run_report", {}).get("failed_batch_count"),
                "failed": report.get("build_batch_dry_run_report", {}).get("failed"),
            },
        ),
        _check(
            "build-promotion-index",
            "ready downstream promotion queue exists for every dry-run-passing build batch",
            report.get("build_promotion_index", {}).get("ok"),
            {
                "ready_count": report.get("build_promotion_index", {}).get("ready_count"),
                "blocked_count": report.get("build_promotion_index", {}).get("blocked_count"),
                "record_count": report.get("build_promotion_index", {}).get("record_count"),
                "failed": report.get("build_promotion_index", {}).get("failed"),
            },
        ),
        _check(
            "promoted-batches",
            "every promotion-ready batch has a materialized promotion report",
            report.get("promoted_batches_report", {}).get("ok"),
            {
                "promoted_count": report.get("promoted_batches_report", {}).get("promoted_count"),
                "blocked_count": report.get("promoted_batches_report", {}).get("blocked_count"),
                "record_count": report.get("promoted_batches_report", {}).get("record_count"),
                "failed": report.get("promoted_batches_report", {}).get("failed"),
            },
        ),
        _check(
            "downstream-build-chain-integrity",
            "downstream build artifacts preserve exact final-record id coverage through every handoff",
            report.get("downstream_build_chain_integrity", {}).get("ok"),
            {
                "counts": report.get("downstream_build_chain_integrity", {}).get("counts"),
                "failed": report.get("downstream_build_chain_integrity", {}).get("failed"),
            },
        ),
        _check(
            "handoff-manifest",
            "compact handoff manifest exists for future builders and agents",
            report.get("handoff_manifest", {}).get("ok"),
            {
                "target_count": report.get("handoff_manifest", {}).get("target_count"),
                "artifact_count": len(report.get("handoff_manifest", {}).get("artifact_map", {})),
                "failed": report.get("handoff_manifest", {}).get("failed"),
            },
        ),
        _check(
            "domain-and-kit-merge-review",
            "domain and kit merge review stages produced reviewed merge artifacts",
            report.get("domain_merge_report", {}).get("pairs_reviewed", 0) > 0
            and report.get("merge_review_report", {}).get("pairs_reviewed", 0) > 0
            and report.get("domain_canonicalization_report", {}).get("ok")
            and report.get("kit_canonicalization_report", {}).get("ok"),
            {
                "domain_pairs_reviewed": report.get("domain_merge_report", {}).get("pairs_reviewed", 0),
                "kit_pairs_reviewed": report.get("merge_review_report", {}).get("pairs_reviewed", 0),
                "domain_canonicalization_ok": report.get("domain_canonicalization_report", {}).get("ok"),
                "kit_canonicalization_ok": report.get("kit_canonicalization_report", {}).get("ok"),
            },
        ),
        _check(
            "lfm-slot-decision-tree",
            "LFM slot decision tree accepted every sampled final record",
            report.get("lfm_slot_decision_tree", {}).get("ok")
            and report.get("lfm_slot_decision_tree", {}).get("records_rejected") == 0,
            report.get("lfm_slot_decision_tree", {}),
        ),
        _check(
            "simulator-slot-smoke",
            "simplified NexusSimulator-style reset/step/snapshot smoke accepted sampled records",
            report.get("simulator_slot_smoke", {}).get("ok")
            and report.get("simulator_slot_smoke", {}).get("rejected") == 0,
            report.get("simulator_slot_smoke", {}),
        ),
        _check(
            "stage-ledger",
            "stage ledger maps every contract to artifacts and passing gates",
            stage_ledger.get("ok") and not stage_ledger.get("missing_contracts"),
            {
                "stage_count": stage_ledger.get("stage_count"),
                "contract_count": stage_ledger.get("contract_count"),
                "missing_contracts": stage_ledger.get("missing_contracts"),
                "failed_gates": [
                    stage.get("name") for stage in stage_ledger.get("stages", []) if not stage.get("gate_ok")
                ],
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "completion_ready": all(check["ok"] for check in checks) and line_count >= 10000,
        "stage": "objective-audit",
        "goal": "verify the completed FractalKitPipeline run against the durable 10k staged harness objective",
        "checks": checks,
        "passed": sum(1 for check in checks if check["ok"]),
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    records = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return records


def _check(name: str, requirement: str, ok: bool, evidence: Any) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }


def _temperature_schedule_ok(schedule: Dict[str, Any]) -> bool:
    return (
        schedule.get("expand", 0) > schedule.get("reveal", 999)
        and schedule.get("reveal", 0) > schedule.get("reduce", 999)
        and schedule.get("relevance_check") <= schedule.get("reduce", 999)
        and schedule.get("domain_merge_review") <= schedule.get("reduce", 999)
        and schedule.get("kit_merge_review") <= schedule.get("reduce", 999)
        and schedule.get("slot_decision") <= schedule.get("reduce", 999)
    )
