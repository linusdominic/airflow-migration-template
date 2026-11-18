"""dp: run and inspect the platform from a terminal.

    dp selftest                       can this environment run pipelines?
    dp list                           the pipelines and their tasks
    dp seed                           write sample data for the example pipelines (local demo)
    dp run PIPELINE --date YYYY-MM-DD
    dp backfill PIPELINE --from YYYY-MM-DD --to YYYY-MM-DD
    dp render PIPELINE TASK           the SQL a task would run, for this environment's warehouse
    dp new-pipeline NAME              start a pipeline from the skeleton

The environment is chosen with PLATFORM_ENV (local, dev, prod). The default is local.
"""

import argparse
import sys
from datetime import date, timedelta

from dataplatform.config import ROOT, ConfigError, load_settings
from dataplatform.connections import Connections


def _settings():
    try:
        return load_settings()
    except ConfigError as error:
        raise SystemExit(f"configuration: {error}") from None


def cmd_selftest(args: argparse.Namespace) -> int:
    from dataplatform import selftest

    settings = _settings()
    outcomes = selftest.run(settings)
    width = max(len(outcome.name) for outcome in outcomes)
    for outcome in outcomes:
        print(f"{'PASS' if outcome.passed else 'FAIL'}  {outcome.name:<{width}}  {outcome.detail}")
    failed = sum(not outcome.passed for outcome in outcomes)
    print(f"\n{len(outcomes) - failed} of {len(outcomes)} checks passed in the {settings.env} environment.")
    return 1 if failed else 0


def cmd_list(args: argparse.Namespace) -> int:
    from dataplatform.registry import load_pipelines

    for name, pipeline in load_pipelines().items():
        print(f"{name}  [{pipeline.schedule}]  {pipeline.description}")
        for task in pipeline.ordered():
            after = f"  after {', '.join(task.after)}" if task.after else ""
            print(f"    {task.name:<18} {type(task).__name__}{after}")
    return 0


def cmd_seed(args: argparse.Namespace) -> int:
    from dataplatform import sample

    settings = _settings()
    if settings.env != "local" and not args.force:
        raise SystemExit("seed writes sample data. It only runs in the local environment unless --force is given.")
    data = sample.write(Connections(settings), args.days)
    first, last = data["days"][0], data["days"][-1]
    print(f"{len(data['files'])} files in the landing store and {len(data['payments'])} payments in the source,")
    print(f"for {first} to {last}. Try: dp backfill loans_daily --from {first} --to {last}")
    return 0


def _run(name: str, days: list[date]) -> int:
    from dataplatform.registry import get_pipeline
    from dataplatform.runner import context, run_pipeline

    settings = _settings()
    connections = Connections(settings)
    pipeline = get_pipeline(name)
    try:
        for day in days:
            print(f"{name}  {day}")
            for task, result in run_pipeline(context(pipeline, day, settings, connections)).items():
                print(f"    ok  {task:<18} {result.rows:>6} rows  {result.detail}")
    except Exception as error:
        print(f"    FAILED  {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    finally:
        connections.close()
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    return _run(args.pipeline, [date.fromisoformat(args.date)])


def cmd_backfill(args: argparse.Namespace) -> int:
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if end < start:
        raise SystemExit("--to is before --from")
    return _run(args.pipeline, [start + timedelta(days=n) for n in range((end - start).days + 1)])


def cmd_render(args: argparse.Namespace) -> int:
    from dataplatform.registry import get_pipeline
    from dataplatform.runner import context

    pipeline = get_pipeline(args.pipeline)
    task = pipeline.task(args.task)
    ctx = context(pipeline, date.fromisoformat(args.date), _settings())
    shown = getattr(task, "rendered", None) or getattr(task, "statements", None)
    if shown is None:
        raise SystemExit(f"{args.task} is a {type(task).__name__}, which has no SQL to show")
    print(";\n\n".join(shown(ctx)) + ";")
    return 0


def cmd_new(args: argparse.Namespace) -> int:
    from dataplatform.scaffold import new_pipeline

    try:
        created = new_pipeline(args.name, ROOT)
    except (ValueError, FileExistsError) as error:
        raise SystemExit(str(error)) from None
    for path in created:
        print(f"created {path.relative_to(ROOT)}")
    print(f"\nNext: edit pipelines/{args.name}/, then run: pytest tests/test_{args.name}.py")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="dp", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("selftest").set_defaults(func=cmd_selftest)
    commands.add_parser("list").set_defaults(func=cmd_list)
    seed = commands.add_parser("seed")
    seed.add_argument("--days", type=int, default=5)
    seed.add_argument("--force", action="store_true")
    seed.set_defaults(func=cmd_seed)
    run = commands.add_parser("run")
    run.add_argument("pipeline")
    run.add_argument("--date", required=True)
    run.set_defaults(func=cmd_run)
    backfill = commands.add_parser("backfill")
    backfill.add_argument("pipeline")
    backfill.add_argument("--from", dest="start", required=True)
    backfill.add_argument("--to", dest="end", required=True)
    backfill.set_defaults(func=cmd_backfill)
    render = commands.add_parser("render")
    render.add_argument("pipeline")
    render.add_argument("task")
    render.add_argument("--date", default="2026-01-01")
    render.set_defaults(func=cmd_render)
    new = commands.add_parser("new-pipeline")
    new.add_argument("name")
    new.set_defaults(func=cmd_new)
    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
