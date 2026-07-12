# Chainstormer Workflow Harness

Status: active

## Purpose

Run the same model in a compact tangent loop where each iteration makes two calls:

1. generate a tangent thought
2. translate that thought into a coherent idea of the requested type

## Command

```bash
python3 workflow_harnesses/chainstormer/workflow_chainstormer.py --loops 100
```

## Defaults

- base URL: `http://10.0.0.137:1234/v1`
- model: `lfm2.5-350m-heretic-high-reasoning-i1`
- idea type: `game idea`
- reply length: `one line`
- records: `runs/workflow-harnesses/chainstormer/<run-id>/chainstorm.json`

## Prompt Shape

Thought call:

```text
IDEA TYPE: {idea_type}
THINK TANGENTIALLY ABOUT: {last_thought}
RULE: Align to Idea type, reply in {reply_length}
```

Translation call:

```text
CONVERT THOUGHT INTO A COHERENT {idea_type} IDEA:
LAST THOUGHT: {last_thought}
RULE: Reply in {reply_length}
```

## Artifact Shape

`chainstorm.json` is a JSON list. Each item records:

- iteration index
- idea type
- reply length
- last thought
- thought prompt
- thought output
- idea prompt
- idea output
- usage
- error
- elapsed seconds
