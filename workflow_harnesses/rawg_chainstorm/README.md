# RAWG Chainstorm

This harness gives the local LFM one narrow job: expand a game evidence pointer
into short tangential but category-adjacent ideas. It batches every active
pointer into one request per round, so two to four branching rounds require only
two to four local calls. Codex CLI then performs the agentic work of rejecting,
merging, mapping, and converting useful nodes into proposal-only kit build
requests.

```bash
python3 -m kituniverse_harness.cli rawg-chainstorm \
  --game-id baldurs-gate-iii \
  --rounds 4 \
  --time-budget-seconds 10
```

The harness always attempts two local rounds. It starts rounds three or four
only when the average measured call time fits inside the remaining local
budget. The local time budget excludes Codex review. Raw generations, filtered nodes,
loop state, Codex proposals, and build requests remain separate artifacts. No
kit is promoted by this workflow.
