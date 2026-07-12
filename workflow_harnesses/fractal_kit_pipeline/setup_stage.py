from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Tuple

from kituniverse_harness.ingestion import ShardedJsonlIngestionService
from kituniverse_harness.providers import LMStudioProvider
from workflow_harnesses.fractal_kit_pipeline.run_artifacts import (
    research_pack,
    run_id as make_run_id,
    write_json,
)
from workflow_harnesses.fractal_kit_pipeline.stage_contracts import stage_contracts, stage_names
from workflow_harnesses.fractal_kit_pipeline.temperature_schedule import TEMPERATURE_SCHEDULE


def run_setup_stage(
    base_url: str,
    model: str,
    source: str,
    list_goal: str,
    target_count: int,
    model_seed_count: int,
    concurrency: int,
    max_predictions: int,
    max_context_tokens: int,
    timeout_seconds: int,
    shards: int,
    run_root: Path,
    final_bucket_root: Path,
) -> Tuple[str, Path, ShardedJsonlIngestionService, Dict[str, Any]]:
    validate_controls(
        target_count=target_count,
        concurrency=concurrency,
        max_predictions=max_predictions,
        max_context_tokens=max_context_tokens,
    )
    run_id = make_run_id()
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    final_bucket = ShardedJsonlIngestionService.create(
        root=final_bucket_root,
        run_id=run_id,
        shard_count=shards,
    )

    health = LMStudioProvider(base_url=base_url, model=model, timeout_seconds=timeout_seconds).health()
    write_json(run_dir / "provider-health.json", health)
    write_json(
        run_dir / "manifest.json",
        build_manifest(
            run_id=run_id,
            base_url=base_url,
            model=model,
            source=source,
            list_goal=list_goal,
            target_count=target_count,
            model_seed_count=model_seed_count,
            concurrency=concurrency,
            max_predictions=max_predictions,
            max_context_tokens=max_context_tokens,
            final_bucket=final_bucket,
        ),
    )
    write_json(run_dir / "research-pack.json", research_pack())
    write_json(run_dir / "stage-contracts.json", {"stages": stage_contracts()})
    return run_id, run_dir, final_bucket, health


def validate_controls(
    target_count: int,
    concurrency: int,
    max_predictions: int,
    max_context_tokens: int,
) -> None:
    if target_count < 1:
        raise ValueError("--target-count must be at least 1")
    if concurrency != 128 or max_predictions != 128:
        raise ValueError("this harness requires --concurrency 128 and --max-predictions 128")
    if max_context_tokens > 100:
        raise ValueError("--max-context-tokens must not exceed 100")


def build_manifest(
    run_id: str,
    base_url: str,
    model: str,
    source: str,
    list_goal: str,
    target_count: int,
    model_seed_count: int,
    concurrency: int,
    max_predictions: int,
    max_context_tokens: int,
    final_bucket: ShardedJsonlIngestionService,
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "workflow_harness": "FractalKitPipeline",
        "base_url": base_url,
        "model": model,
        "source": source,
        "list_goal": list_goal,
        "target_count": target_count,
        "model_seed_count": model_seed_count,
        "concurrency": concurrency,
        "max_predictions": max_predictions,
        "max_context_tokens": max_context_tokens,
        "temperature_schedule": TEMPERATURE_SCHEDULE,
        "first_stage_policy": "expand aggressively, reject only obvious waste",
        "final_bucket_run_dir": str(final_bucket.run_dir),
        "stage_contract_artifact": "stage-contracts.json",
        "stage_graph": stage_names(),
    }
