from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List

from kituniverse_harness.ingestion import ShardedJsonlIngestionService
from workflow_harnesses.fractal_kit_pipeline.atomic_filter_audit_stage import build_atomic_filter_audit
from workflow_harnesses.fractal_kit_pipeline.build_batch_dry_run_stage import run_build_batch_dry_run
from workflow_harnesses.fractal_kit_pipeline.build_batch_manifest_stage import build_batch_manifest as build_batches
from workflow_harnesses.fractal_kit_pipeline.build_batch_packet_stage import write_build_batch_packets
from workflow_harnesses.fractal_kit_pipeline.build_batch_replay_smoke_stage import run_build_batch_replay_smoke
from workflow_harnesses.fractal_kit_pipeline.build_promotion_index_stage import build_promotion_index
from workflow_harnesses.fractal_kit_pipeline.build_work_order_stage import build_work_orders
from workflow_harnesses.fractal_kit_pipeline.artifact_schema_index_stage import build_artifact_schema_index
from workflow_harnesses.fractal_kit_pipeline.dependency_graph_audit_stage import build_dependency_graph_audit
from workflow_harnesses.fractal_kit_pipeline.diversity_audit_stage import build_diversity_audit
from workflow_harnesses.fractal_kit_pipeline.downstream_build_chain_integrity_stage import (
    build_downstream_build_chain_integrity,
)
from workflow_harnesses.fractal_kit_pipeline.domain_merge_input_audit_stage import (
    build_domain_merge_input_audit,
)
from workflow_harnesses.fractal_kit_pipeline.domain_canonicalization_audit_stage import (
    build_domain_canonicalization_audit,
)
from workflow_harnesses.fractal_kit_pipeline.final_bucket_reconciliation_stage import (
    build_final_bucket_reconciliation,
)
from workflow_harnesses.fractal_kit_pipeline.final_lineage_integrity_stage import (
    build_final_lineage_integrity,
)
from workflow_harnesses.fractal_kit_pipeline.feed_forward_artifact_integrity_stage import (
    build_feed_forward_artifact_integrity,
)
from workflow_harnesses.fractal_kit_pipeline.first_stage_breadth_audit_stage import (
    build_first_stage_breadth_audit,
)
from workflow_harnesses.fractal_kit_pipeline.handoff_manifest_stage import build_handoff_manifest
from workflow_harnesses.fractal_kit_pipeline.idea_matrix_audit_stage import build_idea_matrix_audit
from workflow_harnesses.fractal_kit_pipeline.merge_review_coverage_stage import (
    build_merge_review_coverage,
)
from workflow_harnesses.fractal_kit_pipeline.prompt_control_indirection_stage import (
    build_prompt_control_indirection,
)
from workflow_harnesses.fractal_kit_pipeline.revealed_reduced_audit_stage import (
    build_revealed_reduced_audit,
)
from workflow_harnesses.fractal_kit_pipeline.stage_contract_integrity_stage import (
    build_stage_contract_integrity,
)
from workflow_harnesses.fractal_kit_pipeline.run_artifacts import (
    count_jsonl,
    write_json,
    write_jsonl,
    write_markdown_report,
)
from workflow_harnesses.fractal_kit_pipeline.objective_audit_stage import build_objective_audit
from workflow_harnesses.fractal_kit_pipeline.promoted_batch_stage import write_promoted_batches
from workflow_harnesses.fractal_kit_pipeline.source_shape_audit_stage import build_source_shape_audit
from workflow_harnesses.fractal_kit_pipeline.slot_decision_trace_integrity_stage import (
    build_slot_decision_trace_integrity,
)
from workflow_harnesses.fractal_kit_pipeline.stage_ledger_stage import build_stage_ledger
from workflow_harnesses.fractal_kit_pipeline.stage_resume_plan_stage import build_stage_resume_plan


async def run_final_output_stage(
    run_id: str,
    run_dir: Path,
    final_bucket: ShardedJsonlIngestionService,
    target_count: int,
    started: float,
    concurrency: int,
    expansion_points: List[Dict[str, Any]],
    expansion_report: Dict[str, Any],
    reveal_records: List[Dict[str, Any]],
    idea_records: List[Dict[str, Any]],
    filtered_records: List[Dict[str, Any]],
    filter_report: Dict[str, Any],
    domain_merge_report: Dict[str, Any],
    canonicalized_records: List[Dict[str, Any]],
    canonicalization_report: Dict[str, Any],
    merge_report: Dict[str, Any],
    kit_canonicalized_records: List[Dict[str, Any]],
    kit_canonicalization_report: Dict[str, Any],
    final_quality_report: Dict[str, Any],
    slot_decision_report: Dict[str, Any],
    simulator_report: Dict[str, Any],
    final_records: List[Dict[str, Any]],
    router_stats: Dict[str, Any],
) -> Dict[str, Any]:
    write_jsonl(run_dir / "expansion-points.jsonl", expansion_points)
    write_json(run_dir / "expansion-report.json", expansion_report)
    first_stage_breadth_audit = build_first_stage_breadth_audit(expansion_points, expansion_report)
    write_json(run_dir / "first-stage-breadth-audit.json", first_stage_breadth_audit)
    write_jsonl(run_dir / "revealed-reduced.jsonl", reveal_records)
    revealed_reduced_audit = build_revealed_reduced_audit(expansion_points, reveal_records)
    write_json(run_dir / "revealed-reduced-audit.json", revealed_reduced_audit)
    write_jsonl(run_dir / "idea-matrix.jsonl", idea_records)
    idea_matrix_audit = build_idea_matrix_audit(idea_records, reveal_records, target_count)
    write_json(run_dir / "idea-matrix-audit.json", idea_matrix_audit)
    write_jsonl(run_dir / "filtered-candidates.jsonl", filtered_records)
    write_json(run_dir / "filter-report.json", filter_report)
    atomic_filter_audit = build_atomic_filter_audit(idea_records, filtered_records, filter_report, target_count)
    write_json(run_dir / "atomic-filter-audit.json", atomic_filter_audit)
    write_json(run_dir / "domain-merge-report.json", domain_merge_report)
    domain_merge_input_audit = build_domain_merge_input_audit(
        filtered_records,
        domain_merge_report,
        target_count,
    )
    write_json(run_dir / "domain-merge-input-audit.json", domain_merge_input_audit)
    write_jsonl(run_dir / "domain-canonicalized.jsonl", canonicalized_records)
    write_json(run_dir / "domain-canonicalization-report.json", canonicalization_report)
    domain_canonicalization_audit = build_domain_canonicalization_audit(
        filtered_records,
        canonicalized_records,
        domain_merge_report,
        canonicalization_report,
        target_count,
    )
    write_json(run_dir / "domain-canonicalization-audit.json", domain_canonicalization_audit)
    write_json(run_dir / "merge-review-report.json", merge_report)
    write_jsonl(run_dir / "kit-canonicalized.jsonl", kit_canonicalized_records)
    write_json(run_dir / "kit-canonicalization-report.json", kit_canonicalization_report)
    write_jsonl(run_dir / "selected-final-records.jsonl", final_records)
    write_json(run_dir / "final-quality-report.json", final_quality_report)
    source_shape_audit = build_source_shape_audit(run_dir, final_records, target_count)
    write_json(run_dir / "source-shape-audit.json", source_shape_audit)
    diversity_audit = build_diversity_audit(final_records, target_count)
    write_json(run_dir / "diversity-audit.json", diversity_audit)
    dependency_graph_audit = build_dependency_graph_audit(final_records, target_count)
    write_json(run_dir / "dependency-graph-audit.json", dependency_graph_audit)
    build_batch_manifest = build_batches(final_records, target_count)
    write_json(run_dir / "build-batches.json", build_batch_manifest)
    build_batch_replay_smoke = run_build_batch_replay_smoke(final_records, build_batch_manifest)
    write_json(run_dir / "build-batch-replay-smoke.json", build_batch_replay_smoke)
    final_jsonl = run_dir / "final-kits.jsonl"
    write_jsonl(final_jsonl, final_records)
    build_work_orders_records, build_work_orders_report = build_work_orders(
        build_batch_manifest,
        str(final_jsonl),
        str(run_dir),
    )
    write_jsonl(run_dir / "build-work-orders.jsonl", build_work_orders_records)
    write_json(run_dir / "build-work-orders-report.json", build_work_orders_report)
    build_batch_packets, build_batch_packets_report = write_build_batch_packets(
        run_dir,
        final_records,
        build_work_orders_records,
    )
    write_json(run_dir / "build-batch-packets-report.json", build_batch_packets_report)
    build_batch_dry_run_report = run_build_batch_dry_run(run_dir, build_batch_packets)
    write_json(run_dir / "build-batch-dry-run-report.json", build_batch_dry_run_report)
    build_promotion_index_report = build_promotion_index(
        build_batch_manifest,
        build_batch_packets_report,
        build_batch_dry_run_report,
    )
    write_json(run_dir / "build-promotion-index.json", build_promotion_index_report)
    promoted_batches_report = write_promoted_batches(run_dir, build_promotion_index_report)
    write_json(run_dir / "promoted-batches-report.json", promoted_batches_report)
    downstream_build_chain_integrity = build_downstream_build_chain_integrity(run_dir, target_count)
    write_json(run_dir / "downstream-build-chain-integrity.json", downstream_build_chain_integrity)
    write_json(run_dir / "lfm-slot-decisions.json", slot_decision_report)
    write_json(run_dir / "simulator-slot-smoke.json", simulator_report)
    slot_decision_trace_integrity = build_slot_decision_trace_integrity(
        final_records=final_records,
        slot_decision_report=slot_decision_report,
        simulator_report=simulator_report,
    )
    write_json(run_dir / "slot-decision-trace-integrity.json", slot_decision_trace_integrity)

    ingest_results = await final_bucket.ingest_many(final_records, concurrency=concurrency)
    bucket_report = final_bucket.report()
    line_count, malformed = count_jsonl(final_jsonl)
    duplicate_ids = len(final_records) - len({record["record_id"] for record in final_records})
    failed_ingests = [result.to_dict() for result in ingest_results if not result.ok]
    final_bucket_reconciliation = build_final_bucket_reconciliation(
        final_jsonl=final_jsonl,
        final_bucket_run_dir=str(final_bucket.run_dir),
        target_count=target_count,
    )
    write_json(run_dir / "final-bucket-reconciliation.json", final_bucket_reconciliation)
    feed_forward_artifact_integrity = build_feed_forward_artifact_integrity(
        run_dir=run_dir,
        target_count=target_count,
        expected_counts={
            "expansion_points": len(expansion_points),
            "reveal_records": len(reveal_records),
            "idea_records": len(idea_records),
            "filtered_records": len(filtered_records),
            "canonicalized_records": len(canonicalized_records),
            "kit_canonicalized_records": len(kit_canonicalized_records),
            "final_records": len(final_records),
        },
    )
    write_json(run_dir / "feed-forward-artifact-integrity.json", feed_forward_artifact_integrity)
    stage_contract_integrity = build_stage_contract_integrity(run_dir)
    write_json(run_dir / "stage-contract-integrity.json", stage_contract_integrity)
    final_lineage_integrity = build_final_lineage_integrity(run_dir, target_count)
    write_json(run_dir / "final-lineage-integrity.json", final_lineage_integrity)
    merge_review_coverage = build_merge_review_coverage(
        domain_merge_report=domain_merge_report,
        domain_canonicalization_report=canonicalization_report,
        kit_merge_report=merge_report,
        kit_canonicalization_report=kit_canonicalization_report,
        final_records=final_records,
    )
    write_json(run_dir / "merge-review-coverage.json", merge_review_coverage)
    prompt_control_indirection = build_prompt_control_indirection(run_dir, router_stats)
    write_json(run_dir / "prompt-control-indirection.json", prompt_control_indirection)
    stage_resume_plan = build_stage_resume_plan(run_dir, target_count)
    write_json(run_dir / "stage-resume-plan.json", stage_resume_plan)
    artifact_schema_index = build_artifact_schema_index(run_dir, target_count)
    write_json(run_dir / "artifact-schema-index.json", artifact_schema_index)
    handoff_manifest = build_handoff_manifest(
        run_dir=run_dir,
        final_bucket_run_dir=str(final_bucket.run_dir),
        target_count=target_count,
        build_batch_manifest=build_batch_manifest,
        build_work_orders_report=build_work_orders_report,
        build_batch_packets_report=build_batch_packets_report,
        build_promotion_index=build_promotion_index_report,
        promoted_batches_report=promoted_batches_report,
        downstream_build_chain_integrity=downstream_build_chain_integrity,
        router_stats=router_stats,
    )
    write_json(run_dir / "handoff-manifest.json", handoff_manifest)

    base_ok = (
        line_count == target_count
        and malformed == 0
        and duplicate_ids == 0
        and bucket_report["ok"]
        and bucket_report["total_records"] == target_count
        and final_bucket_reconciliation["ok"]
        and first_stage_breadth_audit["ok"]
        and revealed_reduced_audit["ok"]
        and idea_matrix_audit["ok"]
        and atomic_filter_audit["ok"]
        and domain_merge_input_audit["ok"]
        and domain_canonicalization_audit["ok"]
        and feed_forward_artifact_integrity["ok"]
        and stage_contract_integrity["ok"]
        and final_lineage_integrity["ok"]
        and merge_review_coverage["ok"]
        and prompt_control_indirection["ok"]
        and stage_resume_plan["ok"]
        and artifact_schema_index["ok"]
        and slot_decision_trace_integrity["ok"]
        and canonicalization_report["ok"]
        and kit_canonicalization_report["ok"]
        and final_quality_report["ok"]
        and source_shape_audit["ok"]
        and diversity_audit["ok"]
        and dependency_graph_audit["ok"]
        and build_batch_manifest["ok"]
        and build_batch_replay_smoke["ok"]
        and build_work_orders_report["ok"]
        and build_batch_packets_report["ok"]
        and build_batch_dry_run_report["ok"]
        and build_promotion_index_report["ok"]
        and promoted_batches_report["ok"]
        and downstream_build_chain_integrity["ok"]
        and handoff_manifest["ok"]
        and slot_decision_report["ok"]
        and simulator_report["ok"]
        and not failed_ingests
    )
    report = {
        "ok": base_ok,
        "workflow_harness": "FractalKitPipeline",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "elapsed_seconds": round(time.time() - started, 3),
        "final_jsonl": str(final_jsonl),
        "final_records": len(final_records),
        "final_jsonl_lines": line_count,
        "final_jsonl_malformed": malformed,
        "duplicate_record_ids": duplicate_ids,
        "final_bucket_run_dir": str(final_bucket.run_dir),
        "final_bucket_report": bucket_report,
        "final_bucket_reconciliation": final_bucket_reconciliation,
        "first_stage_breadth_audit": first_stage_breadth_audit,
        "revealed_reduced_audit": revealed_reduced_audit,
        "idea_matrix_audit": idea_matrix_audit,
        "atomic_filter_audit": atomic_filter_audit,
        "domain_merge_input_audit": domain_merge_input_audit,
        "domain_canonicalization_audit": domain_canonicalization_audit,
        "feed_forward_artifact_integrity": feed_forward_artifact_integrity,
        "stage_contract_integrity": stage_contract_integrity,
        "final_lineage_integrity": final_lineage_integrity,
        "merge_review_coverage": merge_review_coverage,
        "prompt_control_indirection": prompt_control_indirection,
        "stage_resume_plan": stage_resume_plan,
        "artifact_schema_index": artifact_schema_index,
        "failed_ingests": failed_ingests,
        "expansion_points": len(expansion_points),
        "expansion_report": expansion_report,
        "revealed_reduced_records": len(reveal_records),
        "idea_matrix_records": len(idea_records),
        "filtered_candidate_records": len(filtered_records),
        "selected_final_records": len(final_records),
        "filter_report": filter_report,
        "domain_merge_report": domain_merge_report,
        "domain_canonicalization_report": canonicalization_report,
        "merge_review_report": merge_report,
        "kit_canonicalization_report": kit_canonicalization_report,
        "final_quality_report": final_quality_report,
        "source_shape_audit": source_shape_audit,
        "diversity_audit": diversity_audit,
        "dependency_graph_audit": dependency_graph_audit,
        "build_batch_manifest": build_batch_manifest,
        "build_batch_replay_smoke": build_batch_replay_smoke,
        "build_work_orders_report": build_work_orders_report,
        "build_work_orders_jsonl": str(run_dir / "build-work-orders.jsonl"),
        "build_batch_packets_report": build_batch_packets_report,
        "build_batch_packets": len(build_batch_packets),
        "build_batch_dry_run_report": build_batch_dry_run_report,
        "build_promotion_index": build_promotion_index_report,
        "promoted_batches_report": promoted_batches_report,
        "downstream_build_chain_integrity": downstream_build_chain_integrity,
        "handoff_manifest": handoff_manifest,
        "lfm_slot_decision_tree": slot_decision_report,
        "simulator_slot_smoke": simulator_report,
        "slot_decision_trace_integrity": slot_decision_trace_integrity,
        "router_stats": router_stats,
        "manifest": str(run_dir / "manifest.json"),
        "error": None,
    }
    write_json(run_dir / "report.json", report)
    write_markdown_report(run_dir / "report.md", report)
    write_json(run_dir / "stage-ledger.json", {"status": "building"})
    stage_ledger = build_stage_ledger(
        run_dir=run_dir,
        target_count=target_count,
        expansion_points=expansion_points,
        expansion_report=expansion_report,
        first_stage_breadth_audit=first_stage_breadth_audit,
        reveal_records=reveal_records,
        revealed_reduced_audit=revealed_reduced_audit,
        idea_records=idea_records,
        idea_matrix_audit=idea_matrix_audit,
        filtered_records=filtered_records,
        atomic_filter_audit=atomic_filter_audit,
        filter_report=filter_report,
        domain_merge_report=domain_merge_report,
        domain_merge_input_audit=domain_merge_input_audit,
        canonicalized_records=canonicalized_records,
        canonicalization_report=canonicalization_report,
        domain_canonicalization_audit=domain_canonicalization_audit,
        merge_report=merge_report,
        kit_canonicalized_records=kit_canonicalized_records,
        kit_canonicalization_report=kit_canonicalization_report,
        final_quality_report=final_quality_report,
        source_shape_audit=source_shape_audit,
        diversity_audit=diversity_audit,
        dependency_graph_audit=dependency_graph_audit,
        build_batch_manifest=build_batch_manifest,
        build_batch_replay_smoke=build_batch_replay_smoke,
        build_work_orders_report=build_work_orders_report,
        build_batch_packets_report=build_batch_packets_report,
        build_batch_dry_run_report=build_batch_dry_run_report,
        build_promotion_index=build_promotion_index_report,
        promoted_batches_report=promoted_batches_report,
        downstream_build_chain_integrity=downstream_build_chain_integrity,
        final_bucket_reconciliation=final_bucket_reconciliation,
        feed_forward_artifact_integrity=feed_forward_artifact_integrity,
        stage_contract_integrity=stage_contract_integrity,
        final_lineage_integrity=final_lineage_integrity,
        merge_review_coverage=merge_review_coverage,
        prompt_control_indirection=prompt_control_indirection,
        stage_resume_plan=stage_resume_plan,
        artifact_schema_index=artifact_schema_index,
        handoff_manifest=handoff_manifest,
        slot_decision_report=slot_decision_report,
        simulator_report=simulator_report,
        slot_decision_trace_integrity=slot_decision_trace_integrity,
        final_records=final_records,
        final_bucket_run_dir=str(final_bucket.run_dir),
        line_count=line_count,
        malformed=malformed,
        duplicate_ids=duplicate_ids,
        bucket_report=bucket_report,
        failed_ingests=failed_ingests,
    )
    write_json(run_dir / "stage-ledger.json", stage_ledger)
    objective_audit = build_objective_audit(
        run_dir=run_dir,
        final_bucket_run_dir=str(final_bucket.run_dir),
        report=report,
        stage_ledger=stage_ledger,
    )
    write_json(run_dir / "objective-audit.json", objective_audit)
    report["stage_ledger"] = stage_ledger
    report["stage_ledger_path"] = str(run_dir / "stage-ledger.json")
    report["objective_audit"] = objective_audit
    report["objective_audit_path"] = str(run_dir / "objective-audit.json")
    report["ok"] = base_ok and stage_ledger["ok"] and objective_audit["ok"]
    write_json(run_dir / "report.json", report)
    write_markdown_report(run_dir / "report.md", report)
    return report
