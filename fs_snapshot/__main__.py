from argparse import ArgumentParser
from binascii import unhexlify
import logging
import sys

from .command import store as command_store
from .command import diff as command_diff
from .adapter import config_file
from .adapter.logging import init_logger, init_db_logger
from .model.config import Config


def main():
    program = ArgumentParser(description="Snapshot and diff file system info")
    common = ArgumentParser(add_help=False)
    common.add_argument("spec", help="Spec name")
    common.add_argument(
        "-c", "--config", help="Config file", type=str, default="fs-snapshot.ini"
    )
    common.add_argument(
        "--debug", help="Debug messages to log", action="store_true",
    )
    sub = program.add_subparsers(help="command help")

    store_parser(sub, [common])
    diff_parser(sub, [common])

    args = program.parse_args()
    config = get_config(args)

    logger = init_logger(
        level=logging.DEBUG if args.debug else logging.INFO, log_file=config.log_file
    )

    init_db_logger(
        name=config.store_db_log_name,
        level=logging.DEBUG if args.debug else logging.INFO,
        log_file=config.store_db_log_file,
    )

    try:
        args.func(config, args)
    except Exception as e:
        logger.exception(e)
        print(
            f"An unexpected error occurred:\n\n    {e}\n\nCheck the log for details.",
            file=sys.stderr,
        )
        exit(-1)


def store_parser(root, parents):
    cmd = root.add_parser("store", description="Store a snapshot", parents=parents)
    cmd.set_defaults(func=exec_store)
    return cmd


def diff_parser(root, parents):
    cmd = root.add_parser("diff", description="Diff two snapshots", parents=parents)
    cmd.add_argument(
        "import_id", type=unhexlify, help="ID of the earlier snapshot (hex)"
    )
    cmd.set_defaults(func=exec_diff)
    return cmd


def exec_store(config: Config, args):
    command_store.main(config)


def exec_diff(config: Config, args):
    command_diff.main(config, args.import_id)


def get_config(args) -> Config:
    return get_config_spec(args.config, args.spec)


def get_config_spec(file_name: str, spec: str) -> Config:
    configs = config_file.parse_file(file_name)
    if spec not in configs:
        raise ValueError(
            f"No spec found named '{spec}'. Check your spelling and config file."
        )
    return configs[spec]


if __name__ == "__main__":
    main()
