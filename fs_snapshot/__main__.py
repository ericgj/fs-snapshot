from argparse import ArgumentParser, FileType
from binascii import unhexlify
from glob import iglob
from itertools import chain
import logging
import sys
from typing import Iterable, Generator

from .command import config as command_config
from .command import store as command_store
from .command import diff as command_diff
from .command import query as command_query
from .adapter import config_file
from .adapter.logging import init_logger, init_db_logger
from .model.config import Config


def main():
    program = ArgumentParser(description="Snapshot and diff file system info")
    common = ArgumentParser(add_help=False)
    common.add_argument(
        "-c", "--config", help="Config file", type=str, default="fs-snapshot.ini"
    )
    common.add_argument(
        "--spec", help="Spec (config file section)", default="fs-snapshot"
    )
    common.add_argument(
        "--debug", help="Debug messages to log", action="store_true",
    )
    sub = program.add_subparsers(help="command help")

    config_parser(sub, [common])
    store_parser(sub, [common])
    diff_parser(sub, [common])
    query_parser(sub, [common])

    args = program.parse_args()

    logger = None
    try:
        config = get_config(args)

        logger = init_logger(
            level=logging.DEBUG if args.debug else logging.INFO,
            log_file=config.log_file,
        )

        init_db_logger(
            name=config.store_db_log_name,
            level=logging.DEBUG if args.debug else logging.INFO,
            log_file=config.store_db_log_file,
        )
    except Exception as e:
        if logger is not None:
            logger.exception(e)
        print(
            f"An unexpected error occurred:\n\n    {e}", file=sys.stderr,
        )
        exit(-1)

    try:
        args.func(config, args)
    except Exception as e:
        logger.exception(e)
        print(
            f"An unexpected error occurred:\n\n    {e}\n\nCheck the log for details.",
            file=sys.stderr,
        )
        exit(-1)


def config_parser(root, parents):
    cmd = root.add_parser(
        "config", description="Display config (for debugging)", parents=parents
    )
    cmd.set_defaults(func=exec_config)
    return cmd


def store_parser(root, parents):
    cmd = root.add_parser("store", description="Store a snapshot", parents=parents)
    cmd.set_defaults(func=exec_store)
    return cmd


def diff_parser(root, parents):
    cmd = root.add_parser("diff", description="Diff two snapshots", parents=parents)
    cmd.add_argument(
        "snapshot", type=unhexlify, help="ID of the earlier snapshot (hex)"
    )
    cmd.set_defaults(func=exec_diff)
    return cmd


def query_parser(root, parents):
    cmd = root.add_parser("query", description="Query file info", parents=parents)
    cmd.add_argument(
        "--snapshot",
        type=unhexlify,
        help="ID of snapshot (hex); or latest if not specified",
    )
    cmd.add_argument(
        "-q", "--query-file", "--query-files", nargs="*", help="Query file(s)"
    )
    cmd.add_argument(
        "-o",
        "--output-file",
        nargs="?",
        type=FileType("w"),
        default=sys.stdout,
        help="Output file",
    )
    cmd.add_argument(
        "-f",
        "--format",
        default="json",
        choices=command_query.FORMATS,
        help="Output format",
    )
    cmd.set_defaults(func=exec_query)
    return cmd


def exec_config(config: Config, args):
    command_config.main(config, args.config)


def exec_store(config: Config, args):
    command_store.main(config)


def exec_diff(config: Config, args):
    command_diff.main(config, args.snapshot)


def exec_query(config: Config, args):
    queries: Iterable[str] = []
    if args.query_file is None:
        inp = sys.stdin.read().strip()
        queries = [inp]
    else:
        # Note: this is needed in order to expand file globs in Windows, since
        # the Windows shell does not expand file globs automatically.
        queries = chain.from_iterable(read_glob(fname) for fname in args.query_file)

    command_query.main(
        config, queries, args.output_file, snapshot=args.snapshot, format=args.format,
    )


def read_glob(file_name: str) -> Generator[str, None, None]:
    for fname in iglob(file_name):
        data = ""
        with open(file_name, "r") as f:
            data = f.read()
        yield data


def get_config(args) -> Config:
    return get_config_spec(args.config, args.spec)


def get_config_spec(file_name: str, spec: str) -> Config:
    configs = config_file.parse_file(file_name)
    if spec not in configs:
        raise ValueError(
            f"No section found named '{spec}'. Check your spelling and config file."
        )
    return configs[spec]


if __name__ == "__main__":
    main()
