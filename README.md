# NexusEngine-KitUniverse

Simple chained harnesses for local-model domain work.

## Commands

```bash
python3 -m kituniverse_harness.cli ask-provider --health "Return {\"ok\": true}"
python3 -m kituniverse_harness.cli chain-ask-for-domain-list
python3 -m kituniverse_harness.cli chain-ask-for-domain-list --model lfm2.5-8b-a1b
python3 workflow_harnesses/chainstormer/workflow_chainstormer.py --loops 100
python3 workflow_harnesses/dsk_domain_translator/workflow_dsk_domain_translator.py --idea "game where players build tools" --subdomains 3 --depth 2
python3 workflow_harnesses/category_discovery/workflow_category_discovery.py --name "Mario Party" --loops 6 --max-add 8
python3 workflow_harnesses/domain_discovery/workflow_domain_discovery.py --name "Mario Party" --loops 10
python3 workflow_harnesses/domain_discovery/workflow_domain_discovery.py --benchmark --loops 5
python3 workflow_harnesses/subdomain_discovery/workflow_subdomain_discovery.py --domains "dice-roll, board-navigation, coin-collection" --depth 1 --passes-per-parent 2
python3 workflow_harnesses/matrix_node_explorer/workflow_matrix_node_explorer.py --nodes 16 --concurrency 256 --max-predictions 128 --max-context-tokens 100 --min-chain-steps 4 --max-chain-steps 8 --list-goal "Game Ideas"
python3 -m kituniverse_harness.cli guided-kit-builder --benchmark
python3 -m kituniverse_harness.cli --idea "A replay-safe command ledger"
python3 -m kituniverse_harness.cli batch --source "A tactical dungeon party game" --count 50
python3 -m kituniverse_harness.cli batch --source-file ./idea.md --count 50 --batch-size 50
python3 -m kituniverse_harness.cli batch --resume <run-id>
python3 -m kituniverse_harness.cli serve --workspace runs/rawg-881k/live --open
python3 -m kituniverse_harness.cli rawg-process --workspace runs/rawg-881k/shadow --max-records 1000
python3 -m kituniverse_harness.cli domain-loop --game-id baldurs-gate-iii --max-passes 8 --codex-review

python3 -m kituniverse_harness.cli rawg-chainstorm --game-id baldurs-gate-iii --rounds 3 --time-budget-seconds 10

python3 -m kituniverse_harness.cli rawg-matrix-optimize --sample-games 4 --max-depth 2 --parallel-per-model 8 --context-per-slot 2000 --shard-max-mb 90
python3 -m kituniverse_harness.cli runtime-proof --manifest /path/to/runtime-proof-manifest.json
python3 -m kituniverse_harness.buckets intake ideas --content "first final submission"
python3 -m kituniverse_harness.ingestion stress --records 256 --concurrency 256 --shards 256
```

## Live RAWG capability processing

`kituniverse serve` runs the editable RAWG worker and local browser operator
directly from this checkout. Start, pause, resume, drain, and stop are hero
controls; model, batching, replay, source, and promotion settings stay in the
advanced foldout. State is checkpointed under `runs/rawg-881k/<workspace-id>/`.

The worker streams all 177 external RAWG JSONL files (881,069 records) from
`NexusRealtime-Ideas/games/rawg/chunks` without copying the 1.4 GiB dataset. It
normalizes every row, groups equivalent evidence fingerprints, calls the local
LFM only for novel fingerprints, clusters mechanics, compares those clusters to
live Nexus Engine and ProtoKits contracts, and emits provenance-backed
`kit.build-request.v1` records.

Defaults are `10.0.0.137:1234/v1`,
`lfm2.5-350m-heretic-high-reasoning-i1`, context 2048, and four parallel
predictions. Auto-promotion is deliberately off until a staged run enables it.
When enabled, Codex reviews clusters and the existing fresh validation,
duplicate, NexusSimulator, review, quarantine, and transaction gates decide
KitUniverse promotion. The pipeline never writes to NexusEngine or ProtoKits.

Code edits drain the active batch, atomically checkpoint, and restart under a
new source-hash epoch. Completed evidence shards are gzip-compressed, and the
worker holds below 10 GiB free disk or when the model/reviewer/simulator gate is
unavailable. See
`workflow_harnesses/rawg_capability_pipeline/README.md` for record contracts and
rollout commands.

`domain-loop` is the bounded high-recall lane for one representative or
unresolved record. It selects mechanical sentences and tags, extracts and
grounds facts, reviews each fact in a tiny context, converts accepted facts to
seed capabilities, then rotates through eight domain types. Deterministic gates
and typed self-review run before optional Codex CLI review. Use it selectively:
the final Baldur's Gate III proof required 21 local calls plus Codex review.

Default provider:

- base URL: `http://10.0.0.38:1234/v1`
- model: `lfm2.5-350m-heretic-high-reasoning`
- strict chain validation currently passes with override model `lfm2.5-8b-a1b`

## First Chain

`chain-ask-for-domain-list` runs three steps:

1. seed domain candidates
2. feed candidates into an expansion/grouping prompt
3. feed expanded output into a final five-domain list prompt

Each run writes artifacts under `runs/chain-ask-for-domain-list/<run-id>/`.

## Workflow Harnesses

Workflow harnesses live under `workflow_harnesses/<name>/`.

`chainstormer` runs one model in a tangent loop. Each iteration makes two calls:
a tangent thought call, then a detached translation call that converts the
thought into a coherent idea of the requested type. It records every turn in a JSON list at
`runs/workflow-harnesses/chainstormer/<run-id>/chainstorm.json`.

Default workflow provider:

- base URL: `http://10.0.0.137:1234/v1`
- model: `lfm2.5-350m-heretic-high-reasoning-i1`
- idea type: `game idea`
- reply length: `one line`

`guided-kit-builder` turns a full kit description into a comparable,
slot-attributed kit contract. Source-owned facts remain authoritative, the model
fills semantic gaps through grammar-backed JSON, deterministic normalization
enforces record safety, and exact `provides -> requires` matches create immutable
link records. The router permits only one active model prediction.

Latest guided benchmark:

- run: `runs/workflow-harnesses/guided-kit-builder/20260709-155152-010/report.json`
- result: 10/10 kits accepted, 7 exact-token links, 10 model calls, 0 repairs
- integrity: 0 malformed records, 1 peak active prediction, 1.0 minimum coverage
- attribution: every final slot records its intake, model, or contract source

The hero `kituniverse --idea` form maps to the parameterized batch workflow with
`--count 1`. The explicit `batch` command accepts any positive count and repeats
the same source-matrix, generation, validation, duplicate, real NexusSimulator,
Sol medium review, and journaled promotion loop until exactly that many new kits
are promoted or the bounded attempt limit is reached. Passing subsets commit;
failures are quarantined with evidence and replacements continue.

Batch runs are checkpointed under
`runs/workflow-harnesses/kit-universe-batch/<run-id>/`. `--resume` accepts either
the run ID or its full run-directory path after an endpoint or reviewer
interruption. `--dry-run` exercises all gates without changing
`runs/kit-universe-1000/`; `--skip-codex-review` is accepted only with dry-run.
Resolve the real simulator through `--simulator-cli`,
`NEXUS_SIMULATOR_CLI`, or the `nexus-sim` executable on `PATH`.

The 50-kit production proof is
`runs/workflow-harnesses/kit-universe-batch/20260710-021420-248/final-report.json`: 50
promotions from 104 attempts in four batches, with every promoted record covered
by fresh validation, NexusSimulator acceptance, Sol acceptance, and duplicate
evidence. Current cumulative progress is 58/1,000 promotion-ready.

`dsk-domain-translator` turns any idea into a DSK-style work domain tree.
`--subdomains` controls breadth per parent and `--depth` controls recursive
layers. The root uses one domain translation call; each child uses one
chainstorm subdomain call plus one domain translation call.

Default DSK translator provider:

- base URL: `http://10.0.0.38:1234/v1`
- model: `lfm2.5-350m-heretic-high-reasoning`
- matrix artifact: `coverage-matrix.json`

`category-discovery` is the flat pre-hierarchy discovery pass. It takes an
input name/description and loops over category lanes, adding comma-separated
categories to `categories.json` while tracking attempts in `category-ledger.json`.
Use it when direct domain/subdomain discovery starts inventing weak hierarchy.

`domain-discovery` takes an `IDEA SOURCE NAME` and `IDEA SOURCE DESCRIPTION`,
then loops over the same model with the accepted domain list passed back as
memory. Each turn can add at most three new comma-separated domain names.

Default domain-discovery provider:

- base URL: `http://10.0.0.137:1234/v1`
- model: `lfm2.5-1.2b-instruct`
- final artifact: `domains.json`
- turn ledger: `domain-discovery.json`

`subdomain-discovery` takes seed domains, loops through each parent domain, and
asks for up to three child subdomains per parent pass. The full ledger stays in
`subdomain-ledger.json`, while prompts use compact recent memory for efficiency.
Exhausted parent passes are recorded instead of failing the whole run.

`matrix-node-explorer` runs async matrix instances that each chain multiple
short high-temperature generations. Each step outputs a short candidate list,
converts artifacts back to `LIST GOAL`, falls back to the raw candidate when the
tiny model collapses conversion into generic filler, asks a semantic `Y`/`N`
game-connection question, feeds the last output into the next step, and writes
the full record through sharded JSONL ingestion. A smart router keeps prompts near
`--max-context-tokens` and caps simultaneous provider calls with
`--max-predictions`.

First-stage expansion policy:

- expand aggressively, reject only obvious waste
- keep non-empty, exact-unique, record-safe outputs that pass the Y/N relevance gate
- reject empty outputs, exact repeats, pure formatting/numeric junk, and record-unsafe text
- allow rough wording, strange angles, partial concepts, odd connected tangents, and near-duplicates
- defer semantic cleanup to `filter -> merge -> reduce -> map`

`fractal-kit-pipeline` runs the larger feed-forward path:

```text
research-pack
-> expansion-points
-> revealed-information
-> reduced-capabilities
-> idea-matrix
-> atomic/idempotent filter
-> recursive merge review
-> domain merge input audit
-> domain canonicalization audit
-> final sharded bucket
-> stage contract integrity
-> final lineage integrity
-> merge review coverage
-> prompt/control indirection
-> slot decision trace integrity
-> simulator slot smoke
```

It keeps LM Studio prompts under `--max-context-tokens 100`, requires
`--concurrency 128` and `--max-predictions 128`, scales from high-temperature
expansion to low-temperature merge review, and writes final kit records to both
`final-kits.jsonl` and a sharded final bucket.

Latest 10k proof:

- run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-041500-054/report.json`
- final JSONL: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-041500-054/final-kits.jsonl`
- final bucket: `runs/final-buckets/fractal-kit-pipeline/20260709-041500-054/`
- result: 10,000 records, 0 malformed records, 0 duplicate ids, domain canonicalization audit clean, stage ledger 41/41, objective audit completion-ready

The optional Playwright slot smoke lives at
`workflow_harnesses/fractal_kit_pipeline/playwright_slot_smoke.mjs`. It loads an
in-memory page, aborts every route, writes no artifacts, and validates kit slots
through browser state.

## Bucket Service

Buckets are folders under `buckets/<bucket-name>/`.

Each bucket contains:

- `intake/`: raw intake records
- `heuristic-state/`: duplicate-check state and per-intake heuristic results
- `llm-step/`: placeholder LLM gate, currently `auto-pass`
- `final/`: accepted final submissions and `submissions.json`

The first heuristic rejects duplicate normalized content already present in final submissions.

## Ingestion Service

`ingestion-service` is a sharded JSONL ingestion layer for high-concurrency
matrix-style runs.

It writes:

- `manifest.json`: run metadata and shard count
- `shards/shard-000.jsonl` through `shards/shard-NNN.jsonl`: append-only records
- `report.json`: line-count and malformed-record validation
- `stress-report.json`: stress-test result when using the stress command

Concurrency model:

- records are assigned to shards by stable hash of `record_id`
- each shard has its own async lock
- writes are appended as one JSON object per line
- stress validation counts every JSONL line and checks malformed records

Useful commands:

```bash
python3 -m kituniverse_harness.ingestion create --shards 256
python3 -m kituniverse_harness.ingestion ingest --run-dir runs/ingestion/<run-id> --record-json '{"payload":{"input":["a","b","c"],"output":"d"}}'
python3 -m kituniverse_harness.ingestion stress --records 256 --concurrency 256 --shards 256
```
