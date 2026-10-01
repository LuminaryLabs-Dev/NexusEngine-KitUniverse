# Gemma Chain Harness

One seed crosses three linear stages: exactly five system ideas, a provenance-linked domain list, then a reconciled three-level taxonomy. Gemma drafts; deterministic gates decide whether each stage advances. No kit or domain is promoted.

```bash
kituniverse gemma-chain --seed "a living underwater city adapting to pressure and resource scarcity"
```

Each run is preserved under `runs/gemma/chain-harness/<run-id>/`, including rejected raw responses and the accepted artifact from every stage.

## Optional Codex Luna endpoint

Ask Codex CLI a bounded read-only question with `gpt-5.6-luna` and `xhigh` reasoning:

```bash
kituniverse ask-codex-luna "Review the current taxonomy and identify its highest-risk ambiguity."
```

This endpoint is not an automatic fourth stage. It writes `request.json`, `response.md`, and `report.json` under `runs/gemma/codex-luna/<run-id>/`; prompts are passed over stdin rather than exposed in the process list.
