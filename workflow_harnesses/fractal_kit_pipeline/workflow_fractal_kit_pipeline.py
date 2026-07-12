from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from kituniverse_harness.smart_router import SmartRoutingService
from workflow_harnesses.fractal_kit_pipeline.atomic_filter_stage import (
    filter_atomic_idempotent,
)
from workflow_harnesses.fractal_kit_pipeline.domain_merge_review import (
    run_recursive_domain_merge_review,
)
from workflow_harnesses.fractal_kit_pipeline.domain_canonicalization_stage import (
    apply_domain_canonicalization,
)
from workflow_harnesses.fractal_kit_pipeline.expansion_stage import run_expansion_stage
from workflow_harnesses.fractal_kit_pipeline.final_output_stage import (
    run_final_output_stage,
)
from workflow_harnesses.fractal_kit_pipeline.final_quality_gate_stage import (
    run_final_quality_gate,
)
from workflow_harnesses.fractal_kit_pipeline.idea_matrix_stage import build_idea_matrix
from workflow_harnesses.fractal_kit_pipeline.kit_merge_review import (
    run_recursive_kit_merge_review,
    select_final_records,
)
from workflow_harnesses.fractal_kit_pipeline.kit_canonicalization_stage import (
    apply_kit_canonicalization,
)
from workflow_harnesses.fractal_kit_pipeline.reveal_reduce_stage import (
    run_reveal_reduce_stage,
)
from workflow_harnesses.fractal_kit_pipeline.run_artifacts import (
    build_error_report,
    write_json,
)
from workflow_harnesses.fractal_kit_pipeline.simulator_slot_smoke import (
    run_simulator_slot_smoke,
)
from workflow_harnesses.fractal_kit_pipeline.slot_decision_tree import (
    run_lfm_slot_decision_tree,
)
from workflow_harnesses.fractal_kit_pipeline.setup_stage import run_setup_stage


DEFAULT_BASE_URL = "http://10.0.0.137:1234/v1"
DEFAULT_MODEL = "lfm2.5-350m-heretic-high-reasoning-i1"
DEFAULT_SOURCE = "reusable realtime game and simulation capabilities"
DEFAULT_LIST_GOAL = "Reusable game and simulation capabilities"
DEFAULT_RUN_ROOT = Path("runs/workflow-harnesses/fractal-kit-pipeline")
DEFAULT_FINAL_BUCKET_ROOT = Path("runs/final-buckets/fractal-kit-pipeline")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-fractal-kit-pipeline",
        description="Build staged expansion, reduction, merge review, and final kit JSONL artifacts.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--list-goal", default=DEFAULT_LIST_GOAL)
    parser.add_argument("--target-count", type=int, default=10000)
    parser.add_argument("--model-seed-count", type=int, default=128)
    parser.add_argument("--concurrency", type=int, default=128)
    parser.add_argument("--max-predictions", type=int, default=128)
    parser.add_argument("--max-context-tokens", type=int, default=100)
    parser.add_argument("--max-tokens", type=int, default=8)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--provider-retries", type=int, default=1)
    parser.add_argument("--merge-review-pairs", type=int, default=256)
    parser.add_argument("--review-depth", type=int, default=2)
    parser.add_argument("--slot-decision-sample", type=int, default=64)
    parser.add_argument("--shards", type=int, default=256)
    parser.add_argument("--run-root", default=str(DEFAULT_RUN_ROOT))
    parser.add_argument("--final-bucket-root", default=str(DEFAULT_FINAL_BUCKET_ROOT))
    args = parser.parse_args(argv)

    report = asyncio.run(
        run_pipeline(
            base_url=args.base_url,
            model=args.model,
            source=args.source,
            list_goal=args.list_goal,
            target_count=args.target_count,
            model_seed_count=args.model_seed_count,
            concurrency=args.concurrency,
            max_predictions=args.max_predictions,
            max_context_tokens=args.max_context_tokens,
            max_tokens=args.max_tokens,
            timeout_seconds=args.timeout_seconds,
            provider_retries=args.provider_retries,
            merge_review_pairs=args.merge_review_pairs,
            review_depth=args.review_depth,
            slot_decision_sample=args.slot_decision_sample,
            shards=args.shards,
            run_root=Path(args.run_root),
            final_bucket_root=Path(args.final_bucket_root),
        )
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


async def run_pipeline(
    base_url: str,
    model: str,
    source: str,
    list_goal: str,
    target_count: int,
    model_seed_count: int,
    concurrency: int,
    max_predictions: int,
    max_context_tokens: int,
    max_tokens: int,
    timeout_seconds: int,
    provider_retries: int,
    merge_review_pairs: int,
    review_depth: int,
    slot_decision_sample: int,
    shards: int,
    run_root: Path,
    final_bucket_root: Path,
) -> Dict[str, Any]:
    run_id, run_dir, final_bucket, health = run_setup_stage(
        base_url=base_url,
        model=model,
        source=source,
        list_goal=list_goal,
        target_count=target_count,
        model_seed_count=model_seed_count,
        concurrency=concurrency,
        max_predictions=max_predictions,
        max_context_tokens=max_context_tokens,
        timeout_seconds=timeout_seconds,
        shards=shards,
        run_root=run_root,
        final_bucket_root=final_bucket_root,
    )

    if not health.get("ok"):
        report = build_error_report(False, run_id, run_dir, final_bucket, "provider health failed")
        write_json(run_dir / "report.json", report)
        return report

    router = SmartRoutingService(
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
        max_predictions=max_predictions,
        max_context_tokens=max_context_tokens,
    )
    started = time.time()
    try:
        expansion_points, expansion_report = await run_expansion_stage(
            router=router,
            list_goal=list_goal,
            model_seed_count=model_seed_count,
            max_tokens=max_tokens,
            provider_retries=provider_retries,
        )
        reveal_records = await run_reveal_reduce_stage(
            router=router,
            source=source,
            list_goal=list_goal,
            expansion_points=expansion_points,
            max_tokens=max_tokens,
            provider_retries=provider_retries,
            concurrency=concurrency,
        )
        idea_records = build_idea_matrix(target_count + max(512, target_count // 10), reveal_records)
        filtered_records, filter_report = filter_atomic_idempotent(idea_records)
        domain_merge_report = await run_recursive_domain_merge_review(
            router=router,
            records=filtered_records,
            review_pairs=merge_review_pairs,
            review_depth=review_depth,
            concurrency=concurrency,
            provider_retries=provider_retries,
        )
        canonicalized_records, canonicalization_report = apply_domain_canonicalization(
            filtered_records,
            domain_merge_report,
        )
        merge_report = await run_recursive_kit_merge_review(
            router=router,
            records=canonicalized_records,
            review_pairs=merge_review_pairs,
            review_depth=review_depth,
            provider_retries=provider_retries,
            concurrency=concurrency,
        )
        kit_canonicalized_records, kit_canonicalization_report = apply_kit_canonicalization(
            canonicalized_records,
            merge_report,
        )
        final_records = select_final_records(kit_canonicalized_records, merge_report, target_count)
        final_records, final_quality_report = run_final_quality_gate(final_records, target_count)
        slot_decision_report = await run_lfm_slot_decision_tree(
            router=router,
            records=final_records,
            sample_size=slot_decision_sample,
            concurrency=concurrency,
            provider_retries=provider_retries,
        )
        simulator_report = run_simulator_slot_smoke(final_records[: min(64, len(final_records))])
    finally:
        router_stats = router.stats()
        router.shutdown()

    return await run_final_output_stage(
        run_id=run_id,
        run_dir=run_dir,
        final_bucket=final_bucket,
        target_count=target_count,
        started=started,
        concurrency=concurrency,
        expansion_points=expansion_points,
        expansion_report=expansion_report,
        reveal_records=reveal_records,
        idea_records=idea_records,
        filtered_records=filtered_records,
        filter_report=filter_report,
        domain_merge_report=domain_merge_report,
        canonicalized_records=canonicalized_records,
        canonicalization_report=canonicalization_report,
        merge_report=merge_report,
        kit_canonicalized_records=kit_canonicalized_records,
        kit_canonicalization_report=kit_canonicalization_report,
        final_quality_report=final_quality_report,
        slot_decision_report=slot_decision_report,
        simulator_report=simulator_report,
        final_records=final_records,
        router_stats=router_stats,
    )


if __name__ == "__main__":
    raise SystemExit(main())
