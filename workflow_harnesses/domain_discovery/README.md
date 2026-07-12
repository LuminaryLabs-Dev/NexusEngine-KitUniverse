# Domain Discovery Workflow

Loops over a game source name and description to discover domain service kits.

Each turn passes the accepted domain list back into the next prompt as memory.
The model may add at most three new domains per turn, and the accepted output
is stored as a comma-separated list.

## Command

```bash
python3 workflow_harnesses/domain_discovery/workflow_domain_discovery.py \
  --name "Mario Party" \
  --description "A multiplayer party board game where players roll dice..." \
  --loops 5
```

## Benchmark

```bash
python3 workflow_harnesses/domain_discovery/workflow_domain_discovery.py \
  --benchmark \
  --loops 5 \
  --base-url http://10.0.0.137:1234/v1 \
  --model lfm2.5-1.2b-instruct
```

## Artifacts

- `domain-discovery.json`: turn-by-turn prompts, raw outputs, accepted domains,
  rejected duplicates, and growing domain memory.
- `domains.json`: final domain list and comma-separated output.
- `report.json` / `report.md`: run summary.
