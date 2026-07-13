# RAWG Exhaustive Validation

## Live routing

- Endpoint: `http://10.0.0.137:1234/v1`.
- LFM 350M: loaded context 32,768, parallel 64; exhaustive worker ceiling 512 tokens per prediction.
- LFM 1.2B Instruct: loaded context 16,000, parallel 8; exhaustive worker ceiling 2,000 tokens per prediction.
- The 128-game proof at `runs/rawg-881k/exhaustive-concurrency-128` reached actual peak predictions 64 and 8, respectively, with zero malformed JSONL.
- The packed 12-pointer benchmark at `runs/rawg-881k/exhaustive-packed-12-benchmark` processed 1,000 source maps and 64 simultaneous 350M pages, adding 26 grounded relations, skipping 171 deterministic duplicates, rejecting 427 unsupported outputs, and measuring 202.997 pages/minute. At the observed 1.218 controlled evidence units per game, the full 350M pass projects to about 89,869 pages or 7.4 hours.

## Corrected extraction proof

- `runs/rawg-881k/exhaustive-runtime-proof-v5` is the clean end-to-end baseline.
- 16 source rows produced 16 unique evidence ledgers and 16 unique game evidence maps.
- Deterministic extraction produced 18 grounded interactions; the 350M evidence-local pass added one novel grounded relation, skipped ten duplicates, and rejected six unsupported relations.
- Interaction, kit-observation, and master-evidence ledgers each reconcile at 19 unique identities with zero duplicates.
- Domain plus atomic subdomain is the master-kit identity; all trigger, condition, effect, temporal, and source variations remain attached evidence. The proof produced 15 unique master kits.
- The 1.2B stage refined eight missing master kits: six passed alignment and two remained repair-required.
- Four generated ESM `defineDomainServiceKit()` packages passed NexusSimulator `kit.runtime-proof` with 23/23 checks each: enemy defeat, enemy stun, race choice, and enemy attack.
- Runtime proof covers real import/install, namespaced tokens, metadata, deterministic descriptor, declared inputs/outputs, duplicate replay, reset, snapshot round-trip, public import, syntax, and renderer isolation.
- These four are runtime-proven implementations, not yet final KitUniverse promotions. Semantic near-duplicate and Codex novelty review remain required before they count as genuinely novel master additions.

## Codex-gated build proof

- `runs/rawg-881k/exhaustive-codex-runtime-v8` is the first complete authoritative chain: 32 source records, 51 unique observations, 38 master boundaries, eight 1.2B-refined candidates, eight exact Codex decisions, one accepted novel master kit, one build request, and one runtime-proven implementation.
- Codex accepted Race Choice because the imperative source directly entails the transition and no implemented semantic equivalent was found. It rejected seven candidates for adjective/passive parsing, existing generic attack/defeat/status behavior, target-subtype aliases, or a contract that lost an essential evidence condition.
- `n-race-choose-kit` passed 23/23 NexusSimulator runtime checks with zero errors.
- Prior `runs/rawg-881k/*` packages are benchmark evidence, not novelty inventory. Authoritative existing-capability truth is Nexus Engine, ProtoKits, and promotion-ready KitUniverse records.
- The expanded lexical recall proof over 1,000 games produced 1,160 unique direct observations and 509 domain-plus-subdomain master boundaries. These broad observations require 1.2B formatting plus Codex entailment/novelty review; raw counts never imply accepted kits.

## Safety

- Prior fast-lane and matrix workspaces remain untouched reference evidence.
- Production promotion is disabled.
- Source and derived ledgers are append-only and resumable; JSONL shards cap at 90,000,000 bytes and processing holds below 10 GiB free.

## Full production evidence

- `runs/rawg-881k/exhaustive-production/` reconciles exactly 881,069 unique source-ledger identities, source hashes, evidence-map IDs, and game/domain-kit-map IDs, with matching lineage sets and zero malformed JSONL.
- The deterministic pass produced 494,552 one-to-one mechanic-interaction and kit-observation records before model extraction.
- The first two 350M dispatches committed 4,096/4,096 successful page results, zero blank responses, and 855 newly accepted grounded interactions.
- Production exposed and corrected two resilience gaps: completed pages/refinements now persist individually through `asyncio.as_completed`, and the configured low-disk hold is enforced across extraction, merge, refinement, Codex review, and runtime builds rather than mapping alone.
- `runs/rawg-881k/exhaustive-checkpoint-smoke/` live-proved 11 individually checkpointed 350M pages, eight individually checkpointed 1.2B refinements, zero malformed JSONL, and explicit `low-disk-space` holds for both model stages.
- Manifest v2 and `processing-epoch-events` preserve Git commit, dirty-tree hash, AST hash, prior-run pointer, and pipeline epoch. The resumed production epoch `33382f62310a0d6e891bdd2bc0cde456512d9183781fac7c66664d881cf41777` re-reconciled all 881,069 maps, restored 64 active LM connections, and persisted new page records with that exact epoch ID.

## Pointer decomposition and fail-forward proof

- The earlier one-relation-per-clause production epoch was stopped because roughly 0.56 deterministic observations per game could not prove exhaustive decomposition.
- `rawg.game-kit-pointer-map.v1` compactly maps each direct mechanic, curated metadata-to-domain signal, and platform constraint into versioned facet profiles. Unknown tags are evidence-only, repeated seeds retain multiple evidence references, and sparse games are marked evidence-limited rather than padded.
- `runs/rawg-881k/exhaustive-pointer-v5-live-smoke/` processed 32 games into 15,184 defensible atomic nodes; eleven 350M pages accepted five grounded additions, rejected 58 unsupported tokens, skipped 28 duplicates, and used the all-action-token parser.
- Final master identity is one canonical domain plus action/target capability family. The 64-game `runs/rawg-881k/exhaustive-family-smoke/` proof retained 30,448 per-game facet nodes but merged them into 105 masters: 55 direct behaviors, 21 domain roots, and 29 platform adapters. Facets remain required contract/proof coverage rather than millions of separate micro-kits.
- Unknown action/target families use neutral `mechanics` instead of inheriting an arbitrary first evidence domain. Against the current 495k interaction ledger, canonical fallback reduces projected direct masters from 31,211 accidental domain variants to 6,355 families while preserving every interaction and evidence pointer.
- Pointer v4 persists a column-defined seed table using exact `[field,index]` evidence locators instead of repeated evidence/interaction hashes and semantic variants. The 32-game storage proof measures 2,662 bytes/game, projecting about 2.185 GiB for all 881,069 pointer maps.
- The skeptical 1.2B gate distinguishes mechanic-entailed, kit-quality-required, adapter-required, domain-architecture-required, and explicit-evidence-required facets. Protected failures become `review-required`; they are never silently erased.
- Codex rejected eight protected false/duplicate facets with direct evidence and live inventory reasons. In `runs/rawg-881k/exhaustive-pointer-race-proof/`, it independently accepted race-choice target resolution, repaired the contract, and produced one queued build.
- The final family chain generated `n-race-selection-kit`, which passed NexusSimulator `kit.runtime-proof` with 23/23 checks and zero errors. This proves pointer evidence -> skeptical LFM -> fail-forward Codex repair -> implementation -> runtime proof; it remains benchmark evidence until complete production reconciliation.
