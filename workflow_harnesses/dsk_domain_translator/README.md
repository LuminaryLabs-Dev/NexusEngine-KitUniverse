# DSK Domain Translator Workflow

Status: active

## Purpose

Translate any idea into a DSK-style work domain, then optionally chainstorm subdomains and translate each into deeper work domains.

The workflow uses a gameplay evidence matrix so generated domains are grounded in concrete surfaces such as jump arcs, enemy stomps, powerups, moving platforms, hazards, checkpoints, doors, camera scrolling, terrain tiles, and level exits.

## Command

```bash
python3 workflow_harnesses/dsk_domain_translator/workflow_dsk_domain_translator.py --idea "game where players build tools" --subdomains 3 --depth 2
```

## Length And Depth

- `--reply-length` controls the size of each model reply. Default: `one line`.
- `--subdomains` controls breadth: how many child ideas to generate for each parent domain.
- `--depth` controls recursive layers:
  - `0`: root domain only
  - `1`: root plus direct subdomains
  - `2`: root, subdomains, and sub-subdomains

Total nodes are `1 + subdomains + subdomains^2 ...` through the requested depth.
Each non-root node uses two model calls: one chainstorm subdomain idea, then one DSK domain translation.

## Rejection Repair

Each generated domain is checked before acceptance.

Rejected domains include:

- duplicate names
- parent-copy names
- filler names such as `slot`, `slice`, `info`, `misc`, `general`
- drift words such as `kebab` or `kitchen`
- repeated purposes
- missing inputs, outputs, or acceptance criteria

Rejected domains get repair attempts. If repair still fails, the harness creates a deterministic unique fallback from the assigned work axis so the requested subdomain count is preserved.

Fallback domains are matrix-backed: names combine a gameplay surface and work axis, such as `jump-arc-runtime-loop` or `stomp-collision-player-action`.

## Artifacts

- `domain-records.json`: flat list of every domain record.
- `domain-tree.json`: nested domain tree.
- `report.md`: readable summary.
