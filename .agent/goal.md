# Goal

Status: active

## Purpose

Turn the complete RAWG corpus into an exhaustively decomposed, evidence-backed master inventory containing thousands of genuinely novel, built, and runtime-validated reusable kits. The active implementation is pointer-driven, epoch-aware, fail-forward, and runs directly from the editable KitUniverse checkout.

## Exhaustive RAWG Goal

- Decompose all 881,069 source records without representative-only sampling.
- Persist the complete game-to-kit, domain/subdomain, DSK, temporal, merge, and proof package for every record.
- Maintain one append-only master kit inventory with every source observation attached.
- Use LFM 350M at up to 64 active predictions for grounded breadth and LFM 1.2B at up to eight for contract refinement.
- Build every genuinely novel master kit and require existing contract, duplicate, NexusSimulator, review, and transaction proof before counting it.
- Do not declare completion for scans, clusters, proposals, or queued build requests.

## Success Criteria

- Exactly 881,069 unique sources reconcile through evidence and compact pointer maps with zero malformed, skipped, duplicate, or untraceable records.
- Every supported per-game mechanic/domain/category/platform seed expands through defensible versioned facet profiles; unknown metadata remains evidence-only and never pads engine-kit counts.
- LM Studio uses `10.0.0.137:1234/v1`, with up to 64 `lfm2.5-350m` extraction lanes and eight `lfm2.5-1.2b-instruct` refinement lanes.
- Protected evidence fails forward from LFM rejection into batched Codex entailment, repair, novelty, and inventory review.
- Every Codex-accepted novel master is implemented and passes NexusSimulator `kit.runtime-proof`; only those runtime-proven packages count toward the thousands-of-kits result.
- All stages checkpoint, record code epochs, preserve prior artifacts, and hold below 10 GiB free.

## Proof

- Passing run: `runs/chain-ask-for-domain-list/20260708-041519/report.md`
- Passing model: `lfm2.5-8b-a1b`

## Workflow Harness Goal

- Add `workflow_harnesses/chainstormer/workflow_chainstormer.py`.
- Support `--loops 100`.
- Record every input/output pair in `chainstorm.json` as a JSON list.
- Use compact tangent prompt state with `--idea-type` and `--reply-length`.
- Add `workflow-dsk-domain-translator` for idea to DSK domain tree generation with breadth/depth controls.
- Add `workflow-category-discovery` for flat input-associated category discovery before hierarchy.
- Add `workflow-domain-discovery` for looping game-name/game-description domain discovery with at most three new comma-separated domains per turn.
- Add `workflow-subdomain-discovery` for ledger-backed traversal of each domain/subdomain parent with up to three child subdomains per pass.

## Workflow Harness Proof

- Passing smoke run: `runs/workflow-harnesses/chainstormer/20260708-050412/report.md`
- JSON list proof: `runs/workflow-harnesses/chainstormer/20260708-050412/chainstorm.json`
- 100-loop game idea run: `runs/workflow-harnesses/chainstormer/20260708-050749/chainstorm.json`
- 100-loop result: mechanically passed, but produced only 7 unique outputs and repeated later turns.
- 100-loop tangent game idea run: `runs/workflow-harnesses/chainstormer/20260708-051305/chainstorm.json`
- 100-loop tangent result: mechanically passed with 92 unique outputs, but still needs stronger idea-type alignment.
- 100-loop two-call translation run: `runs/workflow-harnesses/chainstormer/20260708-051522/chainstorm.json`
- 100-loop two-call result: completed 200/200 calls with 90 unique thoughts and 98 unique ideas, but still drifted outside game ideas late in the run.
- DSK translator depth-2 run: `runs/workflow-harnesses/dsk-domain-translator/20260708-052134/report.md`
- DSK translator depth-2 result: completed 13/13 calls for 7 unique domain nodes.
- DSK breadth-3 comparison: `runs/workflow-harnesses/dsk-domain-translator/20260708-065742-665-100/domain-records.json`
- DSK breadth-10 comparison: `runs/workflow-harnesses/dsk-domain-translator/20260708-065752-203-403/domain-records.json`
- DSK comparison result: mechanics passed, subdomain quality failed due to duplicated/root-like/filler domains.
- DSK 50-subdomain validation: `runs/workflow-harnesses/dsk-domain-translator/20260708-070900-675-10495/domain-records.json`
- DSK 50-subdomain result: 51 records, 51 unique domains, 0 duplicates, 0 validation-error records, 0 weak banned names.
- DSK Mario 5x4 run: `runs/workflow-harnesses/dsk-domain-translator/20260708-071725-691-12924/domain-records.json`
- DSK Mario 5x4 result: 781 records, 781 unique domains, 0 duplicates, 0 validation-error records, 0 weak banned names.
- DSK matrix-backed Mario 50-subdomain run: `runs/workflow-harnesses/dsk-domain-translator/20260708-073821-918-28888/domain-records.json`
- DSK matrix-backed 50 result: 51 records, 51 unique domains, 50 unique child surface/axis pairs, 0 numeric suffix domains, 0 validation-error records.
- Category discovery smoke run: `runs/workflow-harnesses/category-discovery/20260708-205117-272/report.md`
- Category discovery smoke result: 23 accepted flat categories over 6 turns on `lfm2.5-1.2b-instruct`.
- Domain discovery benchmark run: `runs/workflow-harnesses/domain-discovery/benchmarks/20260708-133212-333/benchmark-report.json`
- Domain discovery benchmark result: 4/4 games passed over 5 loops each, with 43 accepted domains total on `lfm2.5-1.2b-instruct`.
- Subdomain discovery smoke run: `runs/workflow-harnesses/subdomain-discovery/20260708-133735-394/report.md`
- Subdomain discovery smoke result: 3 roots, 6 parent passes, 7 accepted subdomains, 2 exhausted turns, full ledger and tree artifacts.

## Bucket Service Goal

- Add folder-backed buckets.
- Add intake service per bucket.
- Add heuristic-state step that rejects duplicates from final submissions.
- Add LLM step placeholder that currently auto-passes.

## Ingestion Service Goal

- Add sharded JSONL ingestion for high-concurrency dataset writes.
- Support 256 concurrent submissions without SQLite/file-lock bottlenecks.
- Validate written line counts and malformed JSONL records.

## Matrix Node Explorer Goal

- Add MatrixNodeExplorer-Harness for async matrix-source exploration.
- Each matrix instance should run chained high-temperature generation loops.
- Each raw candidate should pass through `CONVERT TO GOAL` such as `{"Game Ideas"}` before judging.
- Each generated candidate should get a semantic `Y`/`N` game-connection check.
- Use smart routing to cap context around 100 tokens and model predictions at 128 while allowing higher matrix task concurrency.
- Store full records safely through sharded JSONL ingestion.
- First expansion stage should be as expansive as possible while rejecting only obvious waste; later stages handle filter, merge, reduce, and map.

## Bucket Service Proof

- Accepted smoke final: `buckets/smoke-bucket/final/20260708-050625-3738f7d218bc.json`
- Duplicate rejection: `buckets/smoke-bucket/heuristic-state/20260708-050629-3738f7d218bc.json`
- Placeholder LLM auto-pass: `buckets/smoke-bucket/llm-step/20260708-050625-3738f7d218bc.json`

## Ingestion Service Proof

- 256-concurrency stress run: `runs/ingestion/20260708-221709-484/stress-report.json`
- Result: 256 requested, 256 written, 0 malformed records, 0 failures.

## Matrix Node Explorer Proof

- Routed chained run: `runs/workflow-harnesses/matrix-node-explorer/20260708-223053-928/report.json`
- Result: 16 matrix instances, concurrency 256, max predictions 128, max context 100, chain depth 4-8, 16 JSONL records, 0 malformed ingestion records.
- Expansive first-stage smoke: `runs/workflow-harnesses/matrix-node-explorer/20260708-224723-167/report.json`
- Expansive first-stage result: 8 matrix instances, 16 accepted outputs, 0 malformed ingestion records.
- First-stage policy update smoke: `runs/workflow-harnesses/matrix-node-explorer/20260708-225430-683/report.json`
- First-stage policy update result: 6 matrix instances, 38 accepted broad outputs, 100 max context tokens, 128 max predictions, 0 malformed ingestion records.

## Fractal Kit Pipeline Goal

- Add a staged feed-forward pipeline that can build more than 10,000 kit-shaped candidates.
- Keep model prompts under 100 context tokens.
- Use 128 task concurrency and 128 max active predictions.
- Use high-temperature expansion, lower-temperature reveal/reduce, and low-temperature merge review.
- Avoid telling the model the final Nexus kit target; let the model find expansion points, revealed information, and reduced capabilities.
- Filter for atomic and idempotent kit records.
- Merge/review through needs/dependencies using recursive Y/N gates.
- Feed final records into a sharded final bucket and consolidated JSONL.
- Include simplified NexusSimulator-style slot validation.
- Include a no-network/no-write Playwright slot smoke.

## Fractal Kit Pipeline Proof

- 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260708-230141-484/report.json`
- Final JSONL: `runs/workflow-harnesses/fractal-kit-pipeline/20260708-230141-484/final-kits.jsonl`
- Final bucket: `runs/final-buckets/fractal-kit-pipeline/20260708-230141-484/`
- Result: 10,000 final records, 10,000 unique ids, 10,000 semantic keys, 121 domains, 24 families, 0 malformed JSONL records, 0 bucket malformed records.
- Routing proof: 640 routed LM calls, max context 100, max predictions 128.
- Merge proof: 256 recursive needs/dependency Y/N review pairs.
- Simulator proof: 64/64 records accepted through reset, step loop, snapshot, and report.
- Playwright proof: 64/64 records accepted with all routes aborted and no artifact writes.
- First-stage policy smoke: `runs/workflow-harnesses/fractal-kit-pipeline/20260708-231228-897/report.json`
- First-stage policy result: 64 final records, 0 malformed JSONL, 0 duplicate ids, 13/19 expansion candidates accepted, 6 rejected as obvious waste or relevance-check failures, router caps 100 context tokens and 128 predictions.
- First-stage Playwright replay: 16/16 records accepted, 4/4 LFM decision traces replayed, all routes aborted, no artifact writes.
- Domain-merge 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260708-231532-860/report.json`
- Domain-merge 10k result: 10,000 final records, 10,000 unique ids, 10,000 semantic keys, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records.
- Domain-merge proof: 121 domain paths reduced into 47 canonical domain groups, 74 domain merge pairs reviewed, 256 kit merge pairs reviewed, 1,678 routed LM calls, max context 100, max predictions 128.
- Domain-merge Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Kit-merge module 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260708-231940-336/report.json`
- Kit-merge module result: 10,000 final records, 10,000 unique ids, 10,000 semantic keys, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records.
- Kit-merge module proof: extracted `recursive-kit-merge-review` stage reviewed 256 kit pairs, preserved 47 canonical domain groups, used 1,624 routed LM calls, max context 100, max predictions 128.
- Kit-merge module Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Expansion/reveal module 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260708-232541-480/report.json`
- Expansion/reveal module result: 10,000 final records, 10,000 unique ids, 10,000 semantic keys, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records.
- Expansion/reveal module proof: extracted `expansion_stage.py` and `reveal_reduce_stage.py`, accepted 216/348 first-stage candidates, produced 216 reveal/reduce records, reviewed 74 domain pairs and 256 kit pairs, used 1,682 routed LM calls, max context 100, max predictions 128.
- Expansion/reveal module Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Matrix/filter module 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260708-233026-014/report.json`
- Matrix/filter module result: 10,000 final records, 10,000 unique ids, 10,000 semantic keys, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records.
- Matrix/filter module proof: extracted `idea_matrix_stage.py` and `atomic_filter_stage.py`, built 11,000 matrix candidates, kept 11,000 atomic/idempotent records, reviewed 74 domain pairs and 256 kit pairs, used 1,663 routed LM calls, max context 100, max predictions 128.
- Matrix/filter module Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Artifact module 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260708-233626-084/report.json`
- Artifact module result: 10,000 final records, 10,000 unique ids, 10,000 semantic keys, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records, manifest exists, report markdown exists.
- Artifact module proof: extracted `run_artifacts.py`, built 11,000 matrix candidates, kept 11,000 atomic/idempotent records, reviewed 74 domain pairs and 256 kit pairs, used 1,649 routed LM calls, max context 100, max predictions 128.
- Artifact module Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Final-output stage 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260708-234420-753/report.json`
- Final-output stage result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records, 0 failed ingests.
- Final-output stage proof: extracted `final_output_stage.py`, accepted 201/337 first-stage candidates, produced 201 reveal/reduce records, kept 11,000 atomic/idempotent records, reviewed 74 domain pairs and 256 kit pairs, used 1,634 routed LM calls, max context 100, max predictions 128.
- Final-output stage Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Setup stage 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260708-234954-823/report.json`
- Setup stage result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records, 0 failed ingests.
- Setup stage proof: extracted `setup_stage.py`, preserved provider health, manifest, research pack, final bucket setup, accepted 190/335 first-stage candidates, produced 190 reveal/reduce records, kept 11,000 atomic/idempotent records, reviewed 74 domain pairs and 256 kit pairs, used 1,612 routed LM calls, max context 100, max predictions 128.
- Setup stage Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Temperature schedule 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260708-235520-290/report.json`
- Temperature schedule result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records, 0 failed ingests.
- Temperature schedule proof: extracted `temperature_schedule.py`, centralized high-to-low temperatures, manifest recorded expand 1.4, reveal 0.9, reduce 0.45, and low-temperature Y/N gates at 0.1; accepted 197/326 first-stage candidates, produced 197 reveal/reduce records, kept 11,000 atomic/idempotent records, reviewed 74 domain pairs and 256 kit pairs, used 1,617 routed LM calls, max context 100, max predictions 128.
- Temperature schedule Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Domain canonicalization 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-000050-085/report.json`
- Domain canonicalization result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records, 0 failed ingests.
- Domain canonicalization proof: extracted `domain_canonicalization_stage.py`, applied reviewed domain merge groups to 11,000/11,000 filtered records before kit merge, wrote `domain-canonicalized.jsonl`, preserved aliases/needs/dependencies/source ids, reviewed 74 domain pairs and 256 kit pairs, used 1,653 routed LM calls, max context 100, max predictions 128.
- Domain canonicalization Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.

## Guided Kit Builder Goal

- Convert full kit descriptions into guided, comparable contracts with explicit slot provenance.
- Preserve source-owned facts, use the 350M model for semantic gaps, and use deterministic normalization only for contract safety.
- Enforce one active model call, validate each kit, repair failures, then emit exact-token connection records.

## Guided Kit Builder Proof

- Final benchmark: `runs/workflow-harnesses/guided-kit-builder/20260709-155152-010/report.json`
- Result: 10/10 accepted, 7 links, 10 model calls, 0 repairs, 0 malformed records, and peak active predictions of 1.
- Prose-only proof: `runs/workflow-harnesses/guided-kit-builder/20260709-155103-211/` accepted 1/1 after generic-domain and placeholder-plan hardening.

## Parameterized KitUniverse Batch Goal

- Use one workflow for any positive `--count`; the count is an exact promotion target, not an attempt count.
- Keep every candidate independently immutable and require fresh validation, four duplicate signatures, real NexusSimulator contract proof, and Sol medium acceptance before promotion.
- Preserve rejected evidence, generate replacements under a bounded attempt budget, checkpoint provider results, and commit passing subsets through a prepared/committed journal.

## Parameterized Batch Proof

- Production run: `runs/workflow-harnesses/kit-universe-batch/20260710-021420-248/final-report.json`
- Result: 50/50 promoted from 104 attempts across four batches; 54 failures quarantined by the run.
- Resume run: `runs/workflow-harnesses/kit-universe-batch/20260710-023726-100/final-report.json` continued with zero LM calls and unchanged generation evidence.
- Cumulative state: 66 raw, 58 promotion-ready, 114 total quarantined, 32 validated links, 942 remaining.
- Kit canonicalization 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-000643-578/report.json`
- Kit canonicalization result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records, 0 failed ingests.
- Kit canonicalization proof: extracted `kit_canonicalization_stage.py`, applied reviewed same-kit groups to 11,000/11,000 filtered records before final selection, wrote `kit-canonicalized.jsonl`, preserved alias record ids/names/needs/dependencies/review evidence, reviewed 74 domain pairs and 256 kit pairs, kept 10,744 canonical kit groups, used 1,660 routed LM calls, max context 100, max predictions 128.
- Kit canonicalization Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Final quality gate 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-001231-407/report.json`
- Final quality gate result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records, 0 failed ingests.
- Final quality gate proof: extracted `final_quality_gate_stage.py`, kept 10,000/10,000 final records with 0 rejections, verified 10,000 unique record ids, 10,000 unique semantic keys, 10,000 unique merge keys, 47 canonical domains, 10,000 canonical kit ids in the final set, used 1,639 routed LM calls, max context 100, max predictions 128.
- Final quality gate Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Stage contracts 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-001752-957/report.json`
- Stage contracts result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records, 0 failed ingests.
- Stage contracts proof: extracted `stage_contracts.py`, wrote `stage-contracts.json` with 14 stages, matched `manifest.json` `stage_graph` to those stage names exactly, kept 10,000/10,000 records passing final quality, used 1,622 routed LM calls, max context 100, max predictions 128.
- Stage contracts Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- First-stage parser refinement run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-002323-181/report.json`
- First-stage parser refinement result: single rough phrase output is preserved as one candidate, comma/newline output still expands into candidates, 64 final records, 64 JSONL lines, 0 malformed JSONL, 0 duplicate ids, 12/23 first-stage candidates accepted, 11 rejected only by loose relevance check.
- First-stage parser refinement Playwright replay: 16/16 records accepted, 4/4 LFM decision traces replayed, all routes aborted, no artifact writes.
- First-stage parser refinement 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-002439-140/report.json`
- First-stage parser refinement 10k result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records, 0 failed ingests, 202/334 first-stage candidates accepted, 123 loose relevance rejections, 9 exact duplicate rejections, 1,635 routed LM calls, max context 100, max predictions 128.
- First-stage parser refinement 10k Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Stage ledger 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-003019-091/report.json`
- Stage ledger 10k result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records, 0 failed ingests, `stage-ledger.json` wrote 14/14 stages, 14/14 contracts, no missing contracts, no failed gates, 1,653 routed LM calls, max context 100, max predictions 128.
- Stage ledger Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Objective audit 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-003535-524/report.json`
- Objective audit 10k result: `objective-audit.json` wrote `ok=true`, `completion_ready=true`, 13/13 checks passed, 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 malformed bucket records, 0 failed ingests, stage ledger ok, 1,639 routed LM calls, max context 100, max predictions 128.
- Objective audit Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Diversity audit 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-004311-503/report.json`
- Diversity audit 10k result: `diversity-audit.json` wrote `ok=true` with 47 canonical domains, 24 categories, 10,000 unique build signatures, max duplicate signature count 1, stage ledger 15/15, objective audit `completion_ready=true`, 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 failed ingests, 1,634 routed LM calls, max context 100, max predictions 128.
- Diversity audit Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Dependency graph audit 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-004841-623/report.json`
- Dependency graph audit 10k result: `dependency-graph-audit.json` wrote `ok=true` with 8 required tokens, 1,753 provided tokens, 8 primary dependency roots, 0 direct self edges, 0 malformed tokens, stage ledger 16/16, objective audit `completion_ready=true`, 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 failed ingests, 1,652 routed LM calls, max context 100, max predictions 128.
- Dependency graph audit Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Build batch manifest 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-005404-411/report.json`
- Build batch manifest 10k result: `build-batches.json` wrote `ok=true` with 83 batches across 8 primary dependency groups, max batch size 128, 0 duplicate assignments, 0 missing assignments, stage ledger 17/17, objective audit `completion_ready=true`, 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 failed ingests, 1,640 routed LM calls, max context 100, max predictions 128.
- Build batch manifest Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Build batch replay smoke 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-005935-480/report.json`
- Build batch replay smoke 10k result: `build-batch-replay-smoke.json` wrote `ok=true` with 83/83 batches replayed, 10,000 records replayed, 0 failed batches, stage ledger 18/18, objective audit `completion_ready=true`, 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 failed ingests, 1,610 routed LM calls, max context 100, max predictions 128.
- Build batch replay smoke Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Build work order 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-010507-626/report.json`
- Build work order 10k result: `build-work-orders.jsonl` wrote 83 work orders for 83 batches and 10,000 assigned records, stage ledger 19/19, objective audit `completion_ready=true`, 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 0 failed ingests, 1,629 routed LM calls, max context 100, max predictions 128.
- Build work order Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- First-stage aggressive expansion run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-010955-166/report.json`
- First-stage aggressive expansion result: `expansion-report.json` records `DO NOT POLISH`, explicit `reject_only`, `keep_if`, `do_not_reject_for`, and later `filter -> merge -> reduce -> map`; accepted 21/21 first-stage candidates, produced 32 final records, 32 JSONL lines, 0 malformed JSONL, 0 duplicate ids, 1 build work order, and stage ledger 19/19.
- First-stage aggressive expansion Playwright replay: 16/16 records accepted, 4/4 LFM decision traces replayed, all routes aborted, no artifact writes.
- Build batch packet 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-011438-271/report.json`
- Build batch packet 10k result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 83 build batches, 83 work orders, 83 materialized `build-inputs/<batch-id>/` packets, 10,000 packet records, 0 missing packet record refs, 0 malformed packet JSONL files, stage ledger 20/20, objective audit `completion_ready=true`, 1,868 routed LM calls, max context 100, max predictions 128.
- Build batch packet Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Build batch dry-run 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-012003-453/report.json`
- Build batch dry-run 10k result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 83 build batches, 83 work orders, 83 build input packets, 83 batch dry-run reports, 10,000 records checked, 0 failed batches, stage ledger 21/21, objective audit `completion_ready=true`, 1,819 routed LM calls, max context 100, max predictions 128.
- Build batch dry-run Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Build promotion index 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-012450-224/report.json`
- Build promotion index 10k result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 83 build batches, 83 work orders, 83 build input packets, 83 dry-run batch reports, 83 promotion-ready batches, 0 blocked batches, 10,000 promotion-index records, stage ledger 22/22, objective audit `completion_ready=true`, 1,842 routed LM calls, max context 100, max predictions 128.
- Build promotion index Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Promoted batches 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-013006-221/report.json`
- Promoted batches 10k result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 83 build batches, 83 work orders, 83 build input packets, 83 dry-run batch reports, 83 promotion-ready batches, 83 promoted batch reports, 0 blocked promotions, 10,000 promoted records, stage ledger 23/23, objective audit `completion_ready=true`, 1,828 routed LM calls, max context 100, max predictions 128.
- Promoted batches Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Handoff manifest 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-013705-966/report.json`
- Handoff manifest 10k result: 10,000 final records, 10,000 JSONL lines, 10,000 final bucket records, 0 malformed JSONL, 0 duplicate ids, 83 build batches, 83 work orders, 83 build input packets, 83 promoted batch reports, 10,000 promoted records, `handoff-manifest.json` with 17 artifact pointers, stage ledger 24/24, objective audit `completion_ready=true`, 1,848 routed LM calls, max context 100, max predictions 128.
- Handoff manifest Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Source shape audit 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-014459-403/report.json`
- Source shape audit 10k result: 10,000 final records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, checked 10,000/10,000 source-shaped records with 0 failures, 83 build batches, 83 build input packets, 83 promoted batch reports, `handoff-manifest.json` with 18 artifact pointers, stage ledger 25/25, objective audit `completion_ready=true`, 1,833 routed LM calls, max context 100, max predictions 128.
- Source shape audit Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Final bucket reconciliation 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-015036-941/report.json`
- Final bucket reconciliation 10k result: 10,000 final records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, matched 10,000 final JSONL ids to 10,000 sharded bucket ids with 0 missing, 0 extra, and 0 bucket duplicates, `handoff-manifest.json` with 19 artifact pointers, stage ledger 26/26, objective audit `completion_ready=true`, 1,841 routed LM calls, max context 100, max predictions 128.
- Final bucket reconciliation Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Persisted idea matrix 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-015539-836/report.json`
- Persisted idea matrix 10k result: `idea-matrix.jsonl` wrote 11,000 records, `filter-report.json` raw count matched 11,000, kept 11,000 atomic/idempotent records, produced 10,000 final records, kept final JSONL and final bucket reconciliation clean, `handoff-manifest.json` with 20 artifact pointers, stage ledger 26/26, objective audit `completion_ready=true`, 1,817 routed LM calls, max context 100, max predictions 128.
- Persisted idea matrix Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Persisted filtered candidates 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-020045-608/report.json`
- Persisted filtered candidates 10k result: `filtered-candidates.jsonl` wrote 11,000 records, `filter-report.json` kept count matched 11,000 with 0 rejections, produced 10,000 final records, kept final JSONL and final bucket reconciliation clean, `handoff-manifest.json` with 21 artifact pointers, stage ledger 26/26, objective audit `completion_ready=true`, 1,864 routed LM calls, max context 100, max predictions 128.
- Persisted filtered candidates Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Persisted final selection 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-020529-743/report.json`
- Persisted final selection 10k result: `selected-final-records.jsonl` wrote 10,000 records, final quality kept 10,000/10,000 with 0 rejections, produced clean final JSONL and final bucket reconciliation, `handoff-manifest.json` with 22 artifact pointers, stage ledger 26/26, objective audit `completion_ready=true`, 1,857 routed LM calls, max context 100, max predictions 128.
- Persisted final selection Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Feed-forward artifact integrity 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-021052-193/report.json`
- Feed-forward artifact integrity 10k result: `feed-forward-artifact-integrity.json` verified 8/8 JSONL artifacts with 0 malformed lines and count alignment, including 11,000 matrix/filter/canonicalized records and 10,000 selected/final records; stage ledger 27/27, objective audit `completion_ready=true`, 1,841 routed LM calls, max context 100, max predictions 128.
- Feed-forward artifact integrity Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Stage contract integrity 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-021940-765/report.json`
- Stage contract integrity 10k result: `stage-contract-integrity.json` verified 28 code contracts against 28 manifest stages and 28 artifact stages, produced 10,000 final records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, reconciled 10,000 final bucket records, kept feed-forward artifact integrity clean, `handoff-manifest.json` with 24 artifact pointers, stage ledger 28/28, objective audit `completion_ready=true`, 1,814 routed LM calls, max context 100, max predictions 128.
- Stage contract integrity Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Final lineage integrity 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-022637-163/report.json`
- Final lineage integrity 10k result: `final-lineage-integrity.json` verified 10,000 final records against 10,000 selected records, 11,000 idea/filter/domain/kit canonicalized records, 10,000 canonical kit refs, 10,256 kit alias refs, and 568 domain evidence refs; produced 10,000 final records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, reconciled 10,000 final bucket records, `handoff-manifest.json` with 25 artifact pointers, stage ledger 29/29, objective audit `completion_ready=true`, 1,872 routed LM calls, max context 100, max predictions 128.
- Final lineage integrity Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Merge review coverage 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-023316-012/report.json`
- Merge review coverage 10k result: `merge-review-coverage.json` verified 74 clean domain Y/N pairs across depths 1 and 2, 256 clean kit Y/N pairs at depth 2, 47 domain canonical groups, 10,744 kit canonical groups, and canonical metadata on all 10,000 final records; produced 10,000 final records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, reconciled 10,000 final bucket records, `handoff-manifest.json` with 26 artifact pointers, stage ledger 30/30, objective audit `completion_ready=true`, 1,862 routed LM calls, max context 100, max predictions 128.
- Merge review coverage Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Prompt/control indirection 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-023939-684/report.json`
- Prompt/control indirection 10k result: `prompt-control-indirection.json` audited 669 persisted generation prompts with no direct final-target leakage, verified generic source/list-goal text, 100-token context control, 128 max predictions, 128 workflow concurrency, and high-to-low temperature schedule; produced 10,000 final records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, reconciled 10,000 final bucket records, `handoff-manifest.json` with 27 artifact pointers, stage ledger 31/31, objective audit `completion_ready=true`, 1,900 routed LM calls, max context 100, max predictions 128.
- Prompt/control indirection Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Slot decision trace integrity 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-024638-241/report.json`
- Slot decision trace integrity 10k result: `slot-decision-trace-integrity.json` verified 64 sampled records with 7 required slots each, 448 accepted LFM slot decision nodes, bounded prompts, filled final slot values, and simulator agreement over 64 records; produced 10,000 final records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, reconciled 10,000 final bucket records, `handoff-manifest.json` with 28 artifact pointers, stage ledger 32/32, objective audit `completion_ready=true`, 1,773 routed LM calls, max context 100, max predictions 128.
- Slot decision trace integrity Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Downstream build-chain integrity 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-030110-917/report.json`
- Downstream build-chain integrity 10k result: `downstream-build-chain-integrity.json` verified exact final-record coverage through 83 build batches, 83 work orders, 83 build input packets, 83 dry-run batch results, 83 promotion-ready entries, and 83 promoted reports; 10,000 final records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, 10,000 packet record ids, 10,000 promoted records, stage ledger 33/33, objective audit `completion_ready=true`, 1,824 routed LM calls, max context 100, max predictions 128.
- Downstream build-chain Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Stage resume plan 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-030852-960/report.json`
- Stage resume plan 10k result: `stage-resume-plan.json` verified 34/34 resume stage entries, no missing completed artifacts, manifest stage graph matched code contracts, controls preserved target 10,000, 100 context tokens, 128 max predictions, and 128 workflow concurrency; final output still produced 10,000 records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, stage ledger 34/34, objective audit `completion_ready=true`, and 1,796 routed LM calls.
- Stage resume plan Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- First expansion acceptance rule: maximize breadth by keeping non-empty, exact-unique, record-safe candidates with loose `LIST GOAL` relevance; reject only obvious waste and defer semantic cleanup to filter/merge/reduce/map.
- First-stage breadth audit 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-032657-987/report.json`
- First-stage breadth audit 10k result: `first-stage-breadth-audit.json` passed with 295/311 first-stage candidates accepted, rejection reasons limited to direct target leakage, exact duplicates, and loose relevance failures; final output produced 10,000 records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, schema index clean, stage ledger 36/36, objective audit `completion_ready=true`, 1,804 routed LM calls, max context 100, max predictions 128.
- First-stage breadth audit Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Revealed/reduced audit 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-033553-217/report.json`
- Revealed/reduced audit 10k result: `revealed-reduced-audit.json` passed with 306/306 expansion points covered, 240 unique reduced phrases, 0 generic reduced outputs, and 205 evidence-linked records; final output produced 10,000 records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, schema index clean, stage ledger 37/37, objective audit `completion_ready=true`, 1,841 routed LM calls, max context 100, max predictions 128.
- Revealed/reduced audit Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Idea-matrix audit 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-034453-100/report.json`
- Idea-matrix audit 10k result: `idea-matrix-audit.json` passed with 11,000 matrix records, 11,000 unique semantic keys, 121 domains, 24 categories, and 244 reveal signals; final output produced 10,000 records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, schema index clean, stage ledger 38/38, objective audit `completion_ready=true`, 1,831 routed LM calls, max context 100, max predictions 128.
- Idea-matrix audit Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Atomic filter audit 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-035202-612/report.json`
- Atomic filter audit 10k result: `atomic-filter-audit.json` passed with 11,000 matrix records, 11,000 filtered records, 11,000 unique semantic keys, 0 filter rejections; final output produced 10,000 records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, schema index clean, stage ledger 39/39, objective audit `completion_ready=true`, 1,875 routed LM calls, max context 100, max predictions 128.
- Atomic filter audit Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Domain merge input audit 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-040251-497/report.json`
- Domain merge input audit 10k result: `domain-merge-input-audit.json` passed with 11,000 filtered records, 121 unique domains, 24 parent domains, 24 categories, 74 reviewed domain pairs, 47 canonical groups; final output produced 10,000 records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, schema index clean with 37 JSON artifacts, stage ledger 40/40, objective audit `completion_ready=true`, 1,836 routed LM calls, max context 100, max predictions 128.
- Domain merge input audit Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
- Domain canonicalization audit 10k run: `runs/workflow-harnesses/fractal-kit-pipeline/20260709-041500-054/report.json`
- Domain canonicalization audit 10k result: `domain-canonicalization-audit.json` passed with 11,000 filtered records, 11,000 domain-canonicalized records, 11,000 reviewed canonical applications, 47 canonical groups; final output produced 10,000 records, 10,000 JSONL lines, 0 malformed JSONL, 0 duplicate ids, schema index clean with 38 JSON artifacts, stage ledger 41/41, objective audit `completion_ready=true`, 1,842 routed LM calls, max context 100, max predictions 128.
- Domain canonicalization audit Playwright replay: 64/64 records accepted, 64/64 LFM decision traces replayed, all routes aborted, no artifact writes.
