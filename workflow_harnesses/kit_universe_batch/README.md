# KitUniverse Batch Harness

Generate an exact count of new promotion-ready kits from one or more source records.

```bash
python3 -m workflow_harnesses.kit_universe_batch.workflow_kit_universe_batch \
  --source "A party platform game with rounds, hazards, rewards, and respawns" \
  --count 50 \
  --simulator-cli /path/to/NexusSimulator-V1/src/cli.js
```

`--count 1` and `--count 50` use the same stages. Failures are quarantined and do not advance the requested count. `--skip-codex-review` is accepted only with `--dry-run`; reviewless runs cannot promote.

Resume a held or interrupted run without regenerating a completed pending batch:

```bash
kituniverse batch --resume <run-id> --simulator-cli /path/to/NexusSimulator-V1/src/cli.js
```
