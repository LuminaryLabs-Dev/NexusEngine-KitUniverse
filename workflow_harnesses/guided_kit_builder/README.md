# Guided Kit Builder

Turns one full kit description into a comparable, slot-filled domain service kit. The primary 350M model call uses a strict JSON schema; deterministic processing owns identifiers, namespaces, renderer boundaries, slot validation, and link injection.

## Stages

```text
description intake
-> structured slot draft
-> deterministic normalization
-> slot validation and bounded repair
-> kit comparison
-> link injection
-> final output
```

Every model request is routed with `max_predictions=1`. Workflow tasks may queue concurrently, but only one LM Studio prediction is active.

## Ten-Kit Benchmark

```bash
python3 workflow_harnesses/guided_kit_builder/workflow_guided_kit_builder.py --benchmark
```

The benchmark writes `manifest.json`, `intake-cases.json`, `chain-ledger.jsonl`, `kit-drafts.jsonl`, `final-kits.jsonl`, `comparisons.json`, `link-injections.jsonl`, `benchmark-report.json`, `report.json`, and `report.md` under `runs/workflow-harnesses/guided-kit-builder/<run-id>/`.

Benchmark state, input, replay-key, dependency, and provision slots are explicit guided intake facts. The report records slot source counts so benchmark coverage does not misrepresent those facts as model discoveries.

## One Description

```bash
python3 workflow_harnesses/guided_kit_builder/workflow_guided_kit_builder.py \
  --title "Round Timer" \
  --domain-hint simulation-timing \
  --requires clock:delta \
  --provides time:tick,time:expired \
  --owned-state remaining-time,running \
  --inputs start,pause,resume,tick \
  --idempotency-key tick-event-id \
  --description "Own remaining round time, accept tick, emit expiry, reset and snapshot state, and ignore duplicate tick ids."
```
