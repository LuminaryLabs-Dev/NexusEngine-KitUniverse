# Subdomain Discovery Workflow

Discovers child subdomains for each seed domain with a ledger-backed queue.

The harness loops through each parent domain, asks the model for up to three
new comma-separated child subdomains, records accepted and rejected outputs, and
keeps full ledger memory locally. Prompt memory is compacted with a
`--memory-window`, while the ledger still enforces global duplicate checks.

## Command

```bash
python3 workflow_harnesses/subdomain_discovery/workflow_subdomain_discovery.py \
  --name "Mario Party" \
  --domains "dice-roll, board-navigation, coin-collection" \
  --depth 1 \
  --passes-per-parent 2
```

## Inputs

- `--domains`: comma-separated seed domains.
- `--domains-file`: optional JSON file with a `domains` list, such as a
  `workflow-domain-discovery` `domains.json` artifact.
- `--depth`: how many child layers to discover.
- `--passes-per-parent`: how many discovery turns each parent gets per layer.
- `--memory-window`: how many recent ledger names are injected into prompts.

## Artifacts

- `subdomain-ledger.json`: full node ledger, turns, attempts, accepted names,
  rejected names, and stats.
- `subdomain-tree.json`: nested domain tree.
- `report.json` / `report.md`: run summary.
