from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from kituniverse_harness.ingestion import ShardedJsonlIngestionService


def run_id() -> str:
    return f"{time.strftime('%Y%m%d-%H%M%S')}-{int((time.time() % 1) * 1000):03d}"


def research_pack() -> Dict[str, Any]:
    return {
        "kit_shape": {
            "required": [
                "stable id",
                "domain path",
                "requires/provides tokens",
                "resources",
                "events",
                "systems",
                "public api",
                "snapshot/reset policy",
                "validation path",
                "renderer boundary",
            ],
            "promotion_rule": "headless deterministic proof before promotion",
            "simulator_shape": "reset -> step/loop -> snapshot -> report",
        },
        "local_sources_reviewed": [
            "/Users/crimsonwheeler/Documents/GitHub/NexusEngine/src/domain-service-kit.js",
            "/Users/crimsonwheeler/Documents/GitHub/NexusEngine/docs/KIT_STATUS_0.0.3.md",
            "/Users/crimsonwheeler/Documents/GitHub/NexusEngine-Editor/src/editor-kit-registry.js",
            "/Users/crimsonwheeler/Documents/GitHub/NexusSimulator/NexusSimulator-V1/memory.md",
        ],
        "public_sources_reviewed": [
            "https://github.com/LuminaryLabs-Dev/NexusRealtime",
            "https://github.com/LuminaryLabs-Agents/NexusRealtime-ProtoKits",
        ],
    }


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )


def count_jsonl(path: Path) -> Tuple[int, int]:
    count = 0
    malformed = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            count += 1
            try:
                json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
    return count, malformed


def build_error_report(
    ok: bool,
    run_id_value: str,
    run_dir: Path,
    final_bucket: ShardedJsonlIngestionService,
    error: Optional[str],
) -> Dict[str, Any]:
    return {
        "ok": ok,
        "workflow_harness": "FractalKitPipeline",
        "run_id": run_id_value,
        "run_dir": str(run_dir),
        "final_bucket_run_dir": str(final_bucket.run_dir),
        "error": error,
    }


def write_markdown_report(path: Path, report: Dict[str, Any]) -> None:
    lines = [
        "# Fractal Kit Pipeline Report",
        "",
        f"- ok: `{str(report['ok']).lower()}`",
        f"- final records: `{report['final_records']}`",
        f"- final jsonl: `{report['final_jsonl']}`",
        f"- final bucket: `{report['final_bucket_run_dir']}`",
        f"- final bucket reconciliation: `{str(report['final_bucket_reconciliation']['ok']).lower()}`",
        f"- first-stage breadth audit: `{str(report['first_stage_breadth_audit']['ok']).lower()}`",
        f"- revealed/reduced audit: `{str(report['revealed_reduced_audit']['ok']).lower()}`",
        f"- idea matrix audit: `{str(report['idea_matrix_audit']['ok']).lower()}`",
        f"- atomic filter audit: `{str(report['atomic_filter_audit']['ok']).lower()}`",
        f"- domain merge input audit: `{str(report['domain_merge_input_audit']['ok']).lower()}`",
        f"- domain canonicalization audit: `{str(report['domain_canonicalization_audit']['ok']).lower()}`",
        f"- feed-forward artifact integrity: `{str(report['feed_forward_artifact_integrity']['ok']).lower()}`",
        f"- stage contract integrity: `{str(report['stage_contract_integrity']['ok']).lower()}`",
        f"- final lineage integrity: `{str(report['final_lineage_integrity']['ok']).lower()}`",
        f"- merge review coverage: `{str(report['merge_review_coverage']['ok']).lower()}`",
        f"- prompt/control indirection: `{str(report['prompt_control_indirection']['ok']).lower()}`",
        f"- slot decision trace integrity: `{str(report['slot_decision_trace_integrity']['ok']).lower()}`",
        f"- stage resume plan: `{str(report['stage_resume_plan']['ok']).lower()}`",
        f"- artifact schema index: `{str(report['artifact_schema_index']['ok']).lower()}`",
        f"- malformed: `{report['final_jsonl_malformed']}`",
        f"- duplicate ids: `{report['duplicate_record_ids']}`",
        f"- expansion points: `{report['expansion_points']}`",
        f"- idea matrix records: `{report['idea_matrix_records']}`",
        f"- filtered candidate records: `{report['filtered_candidate_records']}`",
        f"- selected final records: `{report['selected_final_records']}`",
        f"- domain groups: `{report['domain_merge_report']['canonical_group_count']}`",
        f"- domain merge pairs reviewed: `{report['domain_merge_report']['pairs_reviewed']}`",
        f"- merge pairs reviewed: `{report['merge_review_report']['pairs_reviewed']}`",
        f"- source shape audit: `{str(report['source_shape_audit']['ok']).lower()}`",
        f"- diversity audit: `{str(report['diversity_audit']['ok']).lower()}`",
        f"- unique signatures: `{report['diversity_audit']['unique_signatures']}`",
        f"- dependency graph audit: `{str(report['dependency_graph_audit']['ok']).lower()}`",
        f"- provided dependency tokens: `{report['dependency_graph_audit']['unique_provided_tokens']}`",
        f"- build batches: `{report['build_batch_manifest']['batch_count']}`",
        f"- max batch size: `{report['build_batch_manifest']['max_batch_size']}`",
        f"- build batch replay: `{str(report['build_batch_replay_smoke']['ok']).lower()}`",
        f"- replayed batch records: `{report['build_batch_replay_smoke']['records_replayed']}`",
        f"- build work orders: `{report['build_work_orders_report']['work_order_count']}`",
        f"- build batch packets: `{report['build_batch_packets_report']['packet_count']}`",
        f"- build batch dry run: `{str(report['build_batch_dry_run_report']['ok']).lower()}`",
        f"- dry-run records checked: `{report['build_batch_dry_run_report']['records_checked']}`",
        f"- promotion-ready batches: `{report['build_promotion_index']['ready_count']}`",
        f"- promoted batches: `{report['promoted_batches_report']['promoted_count']}`",
        f"- downstream build-chain integrity: `{str(report['downstream_build_chain_integrity']['ok']).lower()}`",
        f"- handoff manifest: `{str(report['handoff_manifest']['ok']).lower()}`",
        f"- LFM slot tree: `{str(report['lfm_slot_decision_tree']['ok']).lower()}`",
        f"- simulator smoke: `{str(report['simulator_slot_smoke']['ok']).lower()}`",
    ]
    if report.get("stage_ledger"):
        lines.extend(
            [
                f"- stage ledger: `{str(report['stage_ledger']['ok']).lower()}`",
                f"- stage ledger stages: `{report['stage_ledger']['stage_count']}`",
            ]
        )
    if report.get("objective_audit"):
        lines.extend(
            [
                f"- objective audit: `{str(report['objective_audit']['ok']).lower()}`",
                f"- objective completion ready: `{str(report['objective_audit']['completion_ready']).lower()}`",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
