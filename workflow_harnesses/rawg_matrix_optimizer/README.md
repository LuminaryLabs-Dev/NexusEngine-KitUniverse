# RAWG Matrix Optimizer

Benchmarks LFM 1.2B and LFM 350M as high-temperature seed generators and
recursive subdomain walkers. Every profile must reach a subdomain level, keeps
raw calls and derived nodes in separate records, and writes directly to JSONL
shards capped at 90 MB. Codex reviews profile metrics and samples, then selects
the next production strategy.

```bash
python3 -m kituniverse_harness.cli rawg-matrix-optimize \
  --sample-games 4 \
  --max-depth 2 \
  --concurrency 8 \
  --parallel-per-model 8 \
  --context-per-slot 2000 \
  --shard-max-mb 90
```

The benchmark does not promote kits. Its completion ledger identity is source
hash plus profile id; the production worker uses that identity to prevent
reprocessing when this optimizer is connected to its resumable workspace.

## Production

```bash
python3 -m kituniverse_harness.cli rawg-matrix-run \
  --workspace runs/rawg-881k/matrix-production \
  --profile 12b-seed-12b-walk-lean-beam2-d2 \
  --concurrency 8 \
  --parallel-per-model 8 \
  --context-per-slot 2000 \
  --shard-max-mb 90
```

The worker resumes from `completion-ledger-*.jsonl`, never regenerates an
existing source-hash/profile identity, updates `status.json` after each batch,
and holds before free disk falls below 10 GiB.

## Incremental cluster-to-kit review

```bash
python3 -m kituniverse_harness.cli rawg-matrix-cluster \
  --workspace runs/rawg-881k/matrix-production \
  --min-support 2 \
  --max-candidates 25 \
  --shard-max-mb 90
```

Each source result is consumed once into append-only cluster events. Exact and
morphological aliases preserve source evidence and parents. Codex receives only
new support-milestone clusters not already matched in Nexus, and writes
proposal-only kit build requests.

## Fail-forward quality lifecycle

No generated artifact is silently discarded. Raw calls and rejected nodes stay
in their source result; accepted evidence becomes an append-only cluster event;
low-support clusters remain dormant; and Codex decisions become quality-feedback
events. Codex may accept, repair, or defer a cluster. Deferred clusters preserve
their evidence, aliases, rejection reasons, and systemic-error signals and are
eligible for reconsideration when support reaches the next power-of-two bucket
(`2 -> 4 -> 8 -> 16`). A repaired label must pass a second evidence-preserving
Codex review before it can become a proposal-only kit request.
