# Validation — 2026-07-11

## Deterministic checks

- Grounded candidate accepted.
- Broad genre, numeric placeholder, and unsupported candidate rejected.
- Typed act response parsed.
- Typed review must decide every candidate exactly once.
- Singular/plural source-term normalization passed.
- CLI help and Python compilation passed.

## Original direct-loop baseline

- Source: Baldur's Gate III, RAWG source line 5.
- Configuration: `lfm2.5-1.2b-instruct`, context 24576, parallel 1.
- Eight-pass run: `20260711-051244-661`.
- Local calls: 11.
- Local staging: 2 candidates.
- Codex-approved: 0.
- Result: correctly failed the quality gate. Codex found that the local model
  converted promotional narrative wording into unsupported mechanics.

## Updated mechanical-evidence chain on live RAWG

- Source: Baldur's Gate III, RAWG source line 5.
- Final run: `20260711-214724-341`.
- Selected mechanical units: 19.
- Extracted facts: 6; grounded and reviewed facts: 5.
- Deterministic narrative rejection: the promotional trust/power sentence.
- Local calls: 21.
- Local staged candidates: 7.
- Codex-approved: 4 (`select-races`, `play-origin-character`,
  `select-companions`, `party-size`).
- Total elapsed: 29.708 seconds, including 22.901 seconds of Codex review.
- Result: passed on a real RAWG record.

## Explicit-mechanics fixture

- Fixture: `fixtures/explicit-party-game.json`.
- Eight-pass run: `20260711-051326-144`.
- Local calls: 12.
- Local staging: 2 candidates.
- Codex-approved: 2 (`item-effect-application`, `spatial-navigation`).
- Total elapsed time: 21.723 seconds, including 13.669 seconds of Codex review.
- Result: passed. The loop, state, gates, and Codex boundary work when the source
  states mechanics explicitly.

Conclusion: staged mechanical evidence fixes the direct-loop failure on the
tested real record. The quality-oriented chain remains too call-heavy for blind
use across 881,069 records.
