# MatrixNodeExplorer-Harness

Explores short matrix nodes with high-concurrency async tasks, chained local
loops, semantic Y/N judging, heuristic variation checks, smart prediction
routing, and sharded JSONL ingestion.

Each matrix instance:

1. Starts from three matrix items.
2. Generates a comma-separated batch of next nodes.
3. Converts each raw candidate back to `LIST GOAL`, such as `{"Game Ideas"}`.
4. Keeps the raw candidate when conversion collapses into generic filler.
5. Asks whether each selected candidate is a connected game-design node with `Y` or `N`.
6. Accepts candidates that pass the Y/N check and only rejects obvious waste:
   empty outputs, exact duplicates, pure formatting junk, record-unsafe text,
   and relevance-check failures.
7. Feeds the last generated output into the next chain step.
8. Writes one JSONL record containing all chain steps and accepted outputs.

## Command

```bash
python3 workflow_harnesses/matrix_node_explorer/workflow_matrix_node_explorer.py \
  --nodes 32 \
  --concurrency 256 \
  --max-predictions 128 \
  --max-context-tokens 100 \
  --min-chain-steps 4 \
  --max-chain-steps 8 \
  --items-per-step 3 \
  --max-tokens 5 \
  --temperature 1.4 \
  --list-goal "Game Ideas"
```

## Routing

- `--concurrency` controls matrix instances.
- `--max-predictions` controls simultaneous model calls.
- `--max-context-tokens` trims prompts before dispatch.
- `--max-tokens` controls the per-item target and judge token budget.
- `--min-chain-steps` and `--max-chain-steps` let each async instance run a
  different chain depth.

First-stage policy:

```text
expand aggressively, reject only obvious waste
```

The prompt stance is:

```text
LIST GOAL: {"Game Ideas"}
RULE: Generate unusual but relevant new items.
AVOID: exact repeats only.
ALLOW: rough names, strange angles, partial concepts.
DO NOT POLISH: preserve awkward, partial, or weird connected ideas.
RETURN: comma-separated list.
```

Keep if non-empty, not an exact duplicate, record-safe, and loosely connected to
`LIST GOAL` through the Y/N relevance check. Do not reject awkward wording,
near-duplicate meaning, weak grammar, rough labels, or odd but connected ideas.
Those are left for later `filter -> merge -> reduce -> map` stages.

## Artifacts

- `manifest.json`: run settings.
- `outputs.json`: accepted outputs as a list and comma-separated string.
- `report.json` / `report.md`: summary and router stats.
- `runs/matrix-node-explorer-ingestion/<run-id>/shards/*.jsonl`: full records.
