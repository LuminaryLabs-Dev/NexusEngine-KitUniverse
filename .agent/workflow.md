# Workflow

Status: active

## Purpose

Coordinate work through repo-local agent files before implementation.

## Current Flow

- Read repo `memory.md` and `goal.md`.
- Probe live local-model endpoints before selecting a model.
- Use `ask_provider` before adding or running chain harnesses.
- Keep single-entry orchestration separate from sub-harness execution.
- Validate with direct CLI probes and artifact inspection.
- For guided kit generation, preserve intake-owned slots, draft missing semantics with structured output, normalize contract-owned fields, repair validation failures, then compare exact provision/requirement tokens and emit links.
- For each KitUniverse batch: source normalization -> deterministic coverage matrix -> checkpointed 350M generation/repair -> fresh-process validation -> duplicate gate -> real NexusSimulator proof -> one Sol medium review -> file-locked prepared/committed promotion.
- `--count` changes only the exact target, not the per-kit quality gates. Promote accepted subsets, quarantine failures, and loop replacement batches until the target or bounded attempt limit is reached.
- The bare hero form `kituniverse --idea "..."` maps to the batch workflow with `--count 1`; use `kituniverse batch --source ... --count N` for explicit batches.
- Resume from the last checkpoint and generation JSONL; never regenerate matrix nodes that already have persisted results.
- `completed` and `remaining` are promotion-ready counts. Raw immutable failures remain in `kits.jsonl` but are quarantined by `promotion-audit.json` and never participate in connection links.
- For RAWG processing: stream -> normalize -> fingerprint/group -> extract novel evidence -> cluster -> compare live inventory -> rank gaps -> Codex cluster review -> existing KitUniverse gates. Drain on code change and never write outside KitUniverse without approval.
- For unresolved RAWG evidence: select mechanical units -> extract facts -> ground source IDs -> focused fact review -> fact-to-capability seeds -> domain act/review passes -> Codex final review. Require Codex-approved output for a successful reviewed run.
- For exhaustive RAWG work: source row -> complete evidence map -> atomic mechanic relations -> kit map -> domain/subdomain map -> DSK and temporal map -> master evidence merge -> live inventory comparison -> 1.2B contract refinement -> real KitUniverse build/validation gates. A source row is incomplete until its evidence and insufficient-evidence status are persisted; a kit is incomplete until implementation proof passes.
