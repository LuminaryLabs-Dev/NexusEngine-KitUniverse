# Category Discovery Workflow

Discovers a flat category list associated with an input before forcing any
domain hierarchy.

This is meant to run before domain grouping or subdomain discovery. It asks for
categories across rotating lanes such as actions, resources, rules, map/spaces,
events, feedback, multiplayer, content, state, UI, tools, and validation.

## Command

```bash
python3 workflow_harnesses/category_discovery/workflow_category_discovery.py \
  --name "Mario Party" \
  --input "A multiplayer party board game where players roll dice..." \
  --loops 6 \
  --max-add 8
```

## Artifacts

- `category-ledger.json`: full turn ledger, prompts, raw model outputs,
  accepted categories, rejected categories, and memory.
- `categories.json`: final flat category list and comma-separated output.
- `report.json` / `report.md`: run summary.

## Role

Use this when domain/subdomain discovery starts inventing weak hierarchy. The
flat category list can be grouped into domains later.
