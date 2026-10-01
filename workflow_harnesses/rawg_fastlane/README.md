# RAWG Fast Lane

This is an additive, AST-driven alternative to the reference per-game matrix
worker. It never changes or deletes the reference lane.

```text
external RAWG JSONL
-> normalize mechanics in memory
-> fingerprint through coarse, balanced, or fine config
-> append group and source ledgers
-> select one source pointer per novel evidence group
-> optionally expand representatives with bounded local-model lanes
-> optionally run a read-only bounded Codex review/recall loop
-> report speed, reduction, shard integrity, and quality
```

Run a deterministic benchmark without model or Codex calls:

```bash
python3 -m kituniverse_harness.cli rawg-fastlane \
  --ast workflow_harnesses/rawg_fastlane/configs/fast-balanced.ast.json \
  --workspace runs/rawg-fastlane/benchmark-balanced \
  --max-records 50000 --skip-model --skip-codex
```

Workflow AST validation caps general task concurrency at 64, 1.2B at eight
active predictions, the dedicated 350M swarm at 64 active predictions, context
at 2,000 tokens per slot, JSONL shards at 90,000,000 bytes, and Codex loops at
three passes. Codex runs read-only and may recommend config operations; it cannot
edit repositories or promote kits. The 350M swarm only supplies ideas; evidence
filters, 1.2B refinement, merging, and Codex review remain separate authority
stages.

The swarm/refine composition uses one 350M call per representative to produce up
to four high-temperature seeds, then batches all surviving parents from that
representative into one 1.2B walk call. Codex receives stage rates plus sampled
raw, accepted, and rejected nodes in a bounded artifact-backed review packet.

Support-first refinement canonicalizes action-object clusters, rejects generic
support inflation, and spends 1.2B calls at support milestones. A bounded,
deterministically ordered singleton-recall sample keeps rare evidence eligible
without reverting to per-record refinement.

The shadow-production AST retains up to 12 distinct secondary evidence strata
per coarse group. This bounds model work while preserving substantially more
within-group diversity than the three-strata speed benchmark. Promotion remains
disabled for the entire full-corpus shadow pass.
