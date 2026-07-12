# Chain Domain List Model Quality

Status: active

## Feedback

The first chain harness should remain model-configurable. The requested high-reasoning 350m LFM model is reachable, but repeated final-list attempts produced weak or incomplete domain lists under strict validation.

## Evidence

- Failed model: `lfm2.5-350m-heretic-high-reasoning`
- Passing model: `lfm2.5-8b-a1b`
- Passing run: `runs/chain-ask-for-domain-list/20260708-041519/report.md`

## Lesson

Keep output-shape and quality gates in the chain, and do not treat provider reachability as proof that a model can satisfy the harness task.
