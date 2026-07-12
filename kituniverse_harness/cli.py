from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from .chains import ChainConfig, run_chain_ask_for_domain_list
from .providers import DEFAULT_BASE_URL, DEFAULT_MODEL, LMStudioProvider, ask_provider


def main(argv: Optional[list[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = _build_main_parser()
    if _looks_like_batch_hero_command(argv):
        batch_parser = _build_batch_parser(prog="kituniverse")
        return _run_batch(batch_parser.parse_args(_idea_to_source(argv)))
    if _looks_like_guided_hero_command(argv):
        guided_parser = _build_guided_parser(prog="kituniverse")
        guided_args = guided_parser.parse_args(argv)
        if guided_args.idea and not guided_args.benchmark:
            guided_args.universe_turn = True
        return _run_guided(guided_args)

    args = parser.parse_args(argv)
    if args.command == "ask-provider":
        return _run_ask_provider(args)
    if args.command == "chain-ask-for-domain-list":
        return _run_chain(args)
    if args.command == "guided-kit-builder":
        return _run_guided(args)
    if args.command == "batch":
        return _run_batch(args)
    if args.command == "rawg-process":
        return _run_rawg_process(args)
    if args.command == "domain-loop":
        return _run_domain_loop(args)
    if args.command == "serve":
        return _run_operator(args)
    if args.command == "runtime-proof":
        return _run_runtime_proof(args)
    parser.error("provide --idea or an advanced command")
    return 2


def ask_provider_main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="ask-provider")
    _add_ask_provider_args(parser)
    return _run_ask_provider(parser.parse_args(argv))


def chain_ask_for_domain_list_main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="chain-ask-for-domain-list")
    _add_chain_args(parser)
    return _run_chain(parser.parse_args(argv))


def _build_main_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kituniverse",
        description="Turn one rough idea into one validated KitUniverse kit, or run an advanced command.",
        epilog='Hero example: kituniverse --idea "damage cooldown with invulnerability windows"',
    )
    parser.add_argument(
        "--idea",
        metavar="TEXT",
        help="Hero command: generate, review, validate, and commit one nonduplicate kit",
    )
    subcommands = parser.add_subparsers(dest="command")
    _add_ask_provider_args(subcommands.add_parser("ask-provider"))
    _add_chain_args(subcommands.add_parser("chain-ask-for-domain-list"))
    guided_parser = subcommands.add_parser("guided-kit-builder")
    from workflow_harnesses.guided_kit_builder.workflow_guided_kit_builder import (
        configure_parser as configure_guided_kit_parser,
    )

    configure_guided_kit_parser(guided_parser)
    batch_parser = subcommands.add_parser("batch")
    from workflow_harnesses.kit_universe_batch.workflow_kit_universe_batch import (
        configure_parser as configure_batch_parser,
    )

    configure_batch_parser(batch_parser)
    rawg_parser = subcommands.add_parser("rawg-process")
    rawg_parser.add_argument("--workspace", type=Path, default=Path("runs/rawg-881k/default"))
    rawg_parser.add_argument("--max-records", type=int)
    rawg_parser.add_argument("--no-model", action="store_true")
    domain_loop_parser = subcommands.add_parser("domain-loop")
    from workflow_harnesses.rawg_domain_loop.workflow_rawg_domain_loop import (
        configure_parser as configure_domain_loop_parser,
    )

    configure_domain_loop_parser(domain_loop_parser)
    serve_parser = subcommands.add_parser("serve")
    serve_parser.add_argument("--workspace", type=Path, default=Path("runs/rawg-881k/default"))
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)
    serve_parser.add_argument("--open", action="store_true")
    runtime_parser = subcommands.add_parser("runtime-proof")
    runtime_parser.add_argument("--manifest", type=Path, required=True)
    runtime_parser.add_argument("--simulator-cli")
    runtime_parser.add_argument("--output", type=Path)
    runtime_parser.add_argument("--timeout-seconds", type=int, default=300)
    return parser


def _build_batch_parser(prog: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Generate an exact count of NexusSimulator-validated KitUniverse kits.",
    )
    from workflow_harnesses.kit_universe_batch.workflow_kit_universe_batch import (
        configure_parser as configure_batch_parser,
    )

    configure_batch_parser(parser)
    return parser


def _build_guided_parser(prog: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Turn one rough idea into one validated, nonduplicate KitUniverse kit.",
        epilog="Advanced controls are available through: kituniverse guided-kit-builder --help",
    )
    from workflow_harnesses.guided_kit_builder.workflow_guided_kit_builder import (
        configure_parser as configure_guided_kit_parser,
    )

    configure_guided_kit_parser(parser)
    for action in parser._actions:
        if action.dest not in {"help", "idea"}:
            action.help = argparse.SUPPRESS
    return parser


def _looks_like_guided_hero_command(argv: list[str]) -> bool:
    if not argv:
        return False
    known_commands = {"ask-provider", "chain-ask-for-domain-list", "guided-kit-builder", "batch", "rawg-process", "domain-loop", "serve", "runtime-proof"}
    if argv[0] in known_commands:
        return False
    hero_flags = {
        "--idea",
        "--title",
        "--description",
        "--universe-turn",
        "--benchmark",
        "--domain-hint",
        "--requires",
        "--provides",
        "--owned-state",
        "--inputs",
        "--outputs",
        "--idempotency-key",
    }
    return any(argument.split("=", 1)[0] in hero_flags for argument in argv)


def _looks_like_batch_hero_command(argv: list[str]) -> bool:
    if not argv or argv[0] in {"ask-provider", "chain-ask-for-domain-list", "guided-kit-builder", "batch", "rawg-process", "domain-loop", "serve", "runtime-proof"}:
        return False
    flags = {argument.split("=", 1)[0] for argument in argv if argument.startswith("--")}
    guided_only = {
        "--benchmark",
        "--description",
        "--domain-hint",
        "--idempotency-key",
        "--inputs",
        "--outputs",
        "--owned-state",
        "--provides",
        "--requires",
        "--title",
        "--universe-turn",
    }
    return "--idea" in flags and not (flags & guided_only)


def _idea_to_source(argv: list[str]) -> list[str]:
    transformed: list[str] = []
    index = 0
    while index < len(argv):
        argument = argv[index]
        if argument == "--idea":
            transformed.extend(["--source", argv[index + 1]])
            index += 2
            continue
        if argument.startswith("--idea="):
            transformed.append(f"--source={argument.split('=', 1)[1]}")
        else:
            transformed.append(argument)
        index += 1
    if not any(value == "--count" or value.startswith("--count=") for value in transformed):
        transformed.extend(["--count", "1"])
    return transformed


def _add_provider_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--timeout-seconds", type=int, default=90)


def _add_ask_provider_args(parser: argparse.ArgumentParser) -> None:
    _add_provider_common_args(parser)
    parser.add_argument("prompt")
    parser.add_argument("--system", default="Return concise, valid JSON for the requested task.")
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--max-tokens", type=int, default=1600)
    parser.add_argument("--health", action="store_true")


def _add_chain_args(parser: argparse.ArgumentParser) -> None:
    _add_provider_common_args(parser)
    parser.add_argument(
        "--topic",
        default="simple chained harnesses with provider adapters and validation gates",
    )
    parser.add_argument("--run-root", default="runs")
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--max-tokens", type=int, default=2000)


def _run_ask_provider(args: argparse.Namespace) -> int:
    if args.health:
        provider = LMStudioProvider(
            base_url=args.base_url,
            model=args.model,
            timeout_seconds=args.timeout_seconds,
        )
        print(json.dumps(provider.health(), indent=2, sort_keys=True))
    response = ask_provider(
        prompt=args.prompt,
        system=args.system,
        base_url=args.base_url,
        model=args.model,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        timeout_seconds=args.timeout_seconds,
    )
    print(json.dumps(response.to_dict(), indent=2, sort_keys=True))
    return 0 if response.ok else 1


def _run_chain(args: argparse.Namespace) -> int:
    report = run_chain_ask_for_domain_list(
        ChainConfig(
            topic=args.topic,
            base_url=args.base_url,
            model=args.model,
            run_root=Path(args.run_root),
            max_retries=args.max_retries,
            temperature=args.temperature,
            max_tokens=args.max_tokens,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


def _run_guided(args: argparse.Namespace) -> int:
    from workflow_harnesses.guided_kit_builder.workflow_guided_kit_builder import (
        run_from_namespace as run_guided_kit_builder,
    )

    return run_guided_kit_builder(args)


def _run_batch(args: argparse.Namespace) -> int:
    from workflow_harnesses.kit_universe_batch.workflow_kit_universe_batch import (
        run_from_namespace as run_kit_universe_batch,
    )

    return run_kit_universe_batch(args)


def _run_rawg_process(args: argparse.Namespace) -> int:
    from workflow_harnesses.rawg_capability_pipeline.worker import main as run_worker

    argv = ["--workspace", str(args.workspace)]
    if args.max_records is not None:
        argv.extend(["--max-records", str(args.max_records)])
    if args.no_model:
        argv.append("--no-model")
    return run_worker(argv)


def _run_domain_loop(args: argparse.Namespace) -> int:
    from workflow_harnesses.rawg_domain_loop.workflow_rawg_domain_loop import run_from_namespace

    return run_from_namespace(args)


def _run_operator(args: argparse.Namespace) -> int:
    from workflow_harnesses.rawg_capability_pipeline.operator import serve

    serve(args.workspace, args.host, args.port, args.open)
    return 0


def _run_runtime_proof(args: argparse.Namespace) -> int:
    from kituniverse_harness.runtime_proof import main as runtime_main

    argv = ["--manifest", str(args.manifest), "--timeout-seconds", str(args.timeout_seconds)]
    if args.simulator_cli:
        argv.extend(["--simulator-cli", args.simulator_cli])
    if args.output:
        argv.extend(["--output", str(args.output)])
    return runtime_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
