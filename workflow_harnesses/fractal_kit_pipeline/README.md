# Fractal Kit Pipeline

Builds a staged feed-forward pipeline for large kit candidate generation.

## Stage Flow

```text
research-pack
-> expansion-points
-> first-stage breadth audit
-> revealed-information
-> reduced-capabilities
-> revealed/reduced audit
-> idea-matrix
-> idea-matrix audit
-> atomic/idempotent filter
-> atomic filter audit
-> recursive domain merge review
-> domain merge input audit
-> domain canonicalization
-> domain canonicalization audit
-> recursive kit merge review
-> kit canonicalization
-> final quality gate
-> source shape audit
-> build batch packets
-> build batch dry run
-> build promotion index
-> promoted batches
-> downstream build chain integrity
-> final bucket reconciliation
-> feed-forward artifact integrity
-> stage resume plan
-> artifact schema index
-> handoff manifest
-> final sharded bucket
-> simulator slot smoke
-> optional Playwright slot smoke
```

The model prompts never name the final Nexus-specific kit target. Model stages
ask for expansion points, revealed reusable information, and reduced capability
phrases. The mapper then turns that evidence into kit-shaped records using the
repo-reviewed kit contract.

## First-Stage Policy

The expansion stage uses this rule:

```text
expand aggressively, reject only obvious waste
```

It keeps candidates that are non-empty, not exact duplicates, pass a permissive
Y/N relevance check, are loosely connected to `LIST GOAL`, and remain
record-safe bounded text. Record safety includes blocking direct target-term
leakage before any relevance prompt can feed that candidate back to the model.
It does not reject awkward wording, near-duplicate
meaning, weak grammar, rough labels, or odd but connected ideas. Later stages
handle `filter -> merge -> reduce -> map`.

The injected prompt stance is:

```text
LIST GOAL: {"..."}
RULE: Generate unusual but relevant new items.
AVOID: exact repeats only.
ALLOW: rough names, strange angles, partial concepts.
DO NOT POLISH: preserve awkward, partial, or weird connected ideas.
RETURN: comma-separated list.
```

The run writes `expansion-report.json` with accepted/rejected counts and the
first 200 rejection records plus the prompt stance and `reject_only` policy
used for that run.
Comma and newline model output is parsed as multiple candidates. A single rough
phrase is preserved as one candidate instead of being split into words, because
word splitting creates early parser artifacts that belong in later filters.

## Merge Artifacts

- `stage_contracts.py` owns the explicit stage map written to
  `stage-contracts.json`; `manifest.json` uses the same ordered stage names.
- `setup_stage.py` owns control validation, run id creation, run directory
  creation, final bucket creation, provider health, `manifest.json`, and
  `research-pack.json`.
- `temperature_schedule.py` owns the high-to-low model temperature policy used
  by expansion, reveal/reduce, merge review, and slot decision stages.
- `expansion_stage.py` owns broad first-stage generation, obvious-waste
  rejection, and permissive Y/N relevance checks.
- `first_stage_breadth_audit_stage.py` writes
  `first-stage-breadth-audit.json`, proving first-stage rejection stays limited
  to obvious waste, exact repeats, record safety, target leakage, and loose
  relevance failure before semantic cleanup.
- `reveal_reduce_stage.py` owns the expansion-point to revealed-information to
  reduced-capability pass. It treats generic reduced answers such as
  `reusable`, `reuse`, or `module` as filler and falls back to the concrete
  expansion point.
- `revealed_reduced_audit_stage.py` writes `revealed-reduced-audit.json`,
  proving every accepted expansion point produced one compact, non-generic
  revealed/reduced record with enough reduced phrase breadth.
- `idea_matrix_stage.py` owns deterministic matrix expansion from reduced
  capabilities into kit-shaped candidate records.
- `idea-matrix.jsonl` persists that matrix before filtering so the feed-forward
  chain can resume at the atomic/idempotent filter boundary.
- `idea_matrix_audit_stage.py` writes `idea-matrix-audit.json`, proving matrix
  expansion hit the target plus buffer, preserved reduced reveal signals,
  produced required payload fields, and stayed broad across domains.
- `atomic_filter_stage.py` owns atomic/idempotent filtering and duplicate
  semantic-key rejection.
- `filtered-candidates.jsonl` persists the kept atomic/idempotent records
  before recursive domain merge review.
- `atomic_filter_audit_stage.py` writes `atomic-filter-audit.json`, proving the
  filter report accounts for all matrix records and the kept set is
  target-sized, semantic-key unique, atomic, idempotent, and sourced from
  `idea-matrix.jsonl`.
- `domain-merge-report.json` groups similar domain paths through recursive
  low-temperature Y/N review and preserves aliases plus source record ids.
- `domain_merge_input_audit_stage.py` writes
  `domain-merge-input-audit.json`, proving filtered broad candidates form a
  namespaced, target-sized, broad domain index before canonicalization.
- `domain_canonicalization_stage.py` applies reviewed domain groups to every
  candidate record, preserving aliases, needs, dependencies, and source ids in
  `domain-canonicalized.jsonl`.
- `domain_canonicalization_audit_stage.py` writes
  `domain-canonicalization-audit.json`, proving canonical domain metadata,
  aliases, evidence source ids, and source payload fields survived the stage.
- `merge-review-report.json` reviews individual kit records by needs and
  dependencies before final selection.
- `kit_merge_review.py` owns kit-pair review plus final record selection.
- `kit_canonicalization_stage.py` applies reviewed same-kit groups to every
  candidate record, preserving alias record ids, alias names, needs,
  dependencies, and reviewed same-pair evidence in `kit-canonicalized.jsonl`.
- `selected-final-records.jsonl` persists the target-count selected records
  before final quality, source-shape, diversity, dependency, batch, and output
  stages.
- `final_quality_gate_stage.py` verifies final records before bucket ingestion:
  uniqueness, JSON safety, required slots, atomic/idempotent flags, renderer
  boundary, and domain plus kit canonicalization metadata.
- `source_shape_audit_stage.py` verifies final records against the
  source-reviewed ProtoKit/domain-kit shape recorded in `research-pack.json`
  without feeding that shape into model generation prompts.
- `diversity_audit_stage.py` verifies final kit breadth across canonical
  domains, categories, names, and build-relevant signatures.
- `dependency_graph_audit_stage.py` verifies final `requires`/`provides`
  graph coherence, namespaced dependency tokens, and dependency breadth.
- `build_batch_manifest_stage.py` splits final kits into deterministic bounded
  batches for later staged build workers and writes `build-batches.json`.
- `build_batch_replay_smoke_stage.py` reconstructs every build batch from
  final records and verifies required slots before downstream batch work.
- `build_work_order_stage.py` emits `build-work-orders.jsonl`, one bounded
  downstream work order per build batch.
- `build_batch_packet_stage.py` materializes each work order into
  `build-inputs/<batch-id>/` with isolated `kit-records.jsonl`,
  `work-order.json`, and `packet-report.json`.
- `build_batch_dry_run_stage.py` consumes each isolated build input packet and
  writes `batch-results/<batch-id>/build-report.json` plus an aggregate
  `build-batch-dry-run-report.json`.
- `build_promotion_index_stage.py` writes `build-promotion-index.json`, a
  ready/blocked queue for downstream batch promotion workers.
- `promoted_batch_stage.py` consumes the promotion queue and writes
  `promoted-batches/<batch-id>/promotion-report.json` plus
  `promoted-batches-report.json`.
- `downstream_build_chain_integrity_stage.py` reconciles final record ids,
  batch ids, packet JSONL files, dry-run results, promotion entries, and
  promoted reports across the whole downstream build chain.
- `final_bucket_reconciliation_stage.py` verifies the sharded final bucket
  contains exactly the same record ids as `final-kits.jsonl`.
- `feed_forward_artifact_integrity_stage.py` verifies all persisted
  feed-forward JSONL stage artifacts exist, parse, and match expected counts.
- `stage_resume_plan_stage.py` writes `stage-resume-plan.json`, mapping every
  stage contract to concrete artifacts, controls, deferred-final markers, and
  resume hints.
- `artifact_schema_index_stage.py` writes `artifact-schema-index.json`, a
  compact parseable schema summary for major JSON and JSONL run artifacts.
- `handoff_manifest_stage.py` writes `handoff-manifest.json`, a compact
  start-here map for future agents and downstream builders.
- `stage_ledger_stage.py` records every stage contract against live run
  artifacts, counts, gate status, and resume hints in `stage-ledger.json`.
- `objective_audit_stage.py` checks each completed run against the durable
  harness constraints and writes `objective-audit.json`, including a
  `completion_ready` flag for 10k-scale proof.
- `run_artifacts.py` owns run ids, research-pack metadata, JSON/JSONL writes,
  JSONL validation counts, and markdown report output.
- `final_output_stage.py` owns stage artifact writes, final `final-kits.jsonl`,
  final bucket ingestion, final JSONL validation counts, stage ledger writes,
  objective audit writes, and run report assembly.

## Command

```bash
python3 workflow_harnesses/fractal_kit_pipeline/workflow_fractal_kit_pipeline.py \
  --target-count 10000 \
  --model-seed-count 128 \
  --concurrency 128 \
  --max-predictions 128 \
  --max-context-tokens 100 \
  --merge-review-pairs 256 \
  --review-depth 2
```

## Final Records

Each final JSONL record contains:

- `name`
- `domain`
- `domain_path`
- `requires`
- `provides`
- `resources`
- `events`
- `systems`
- `public_api`
- `inputs`
- `outputs`
- `state_rules`
- `tests`
- `snapshot`
- `renderer_boundary`
- `promotion`
- `merge_key`
- `semantic_key`

## Latest Proof

- Run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-041500-054/report.json`
- Result: 10,000 final records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids.
- Domain canonicalization audit: 11,000 filtered records, 11,000
  canonicalized records, 11,000 reviewed canonical applications, 47 canonical
  groups, 0 lost source payload fields.
- Downstream chain: 83 build batches, 83 work orders, 83 packets, 83 dry-run
  results, 83 promotion-ready entries, and 83 promoted reports reconciled to
  the same 10,000 final record ids.
- Resume plan: 41/41 stage contracts mapped to concrete artifacts or
  deferred-final markers with controls preserved.
- Gates: stage ledger 41/41, objective audit `completion_ready=true`, router
  controls at 100 context tokens and 128 max predictions.
- Playwright replay: 64/64 accepted, 64/64 LFM decision traces replayed, all
  routes aborted, no artifact writes.

## Smoke

The Python simulator smoke copies the NexusSimulator shape:

```text
reset -> step loop -> snapshot -> report
```

The Playwright smoke is separate and non-mutating:

```bash
node workflow_harnesses/fractal_kit_pipeline/playwright_slot_smoke.mjs \
  runs/workflow-harnesses/fractal-kit-pipeline/<run-id>/final-kits.jsonl \
  64
```

It loads an in-memory page through `setContent`, aborts every route, and writes
only stdout.
