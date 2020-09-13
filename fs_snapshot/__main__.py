from argparse import ArgumentParser
from binascii import unhexlify

from ..command import store as command_store
from ..command import diff as command_diff
from ..adapter import config_file


def main():
    program = ArgumentParser(description="Snapshot and diff file system info")
    common = ArgumentParser()
    common.add_argument(
        "-c", "--config", description="Config file", type=str, default="fs-snapshot.ini"
    )
    common.add_argument("spec", description="Spec name")
    sub = program.add_subparsers(help="command help")

    store_parser(sub, [common])
    diff_parser(sub, [common])

    args = program.parse_args()
    args.func(args)


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


def exec_store(args):
    config = get_config_spec(args.config, args.spec)
    command_store.main(config)


def exec_diff(args):
    config = get_config_spec(args.config, args.spec)
    command_diff.main(config, args.import_id)


def get_config_spec(file_name: str, spec: str):
    configs = config_file.parse_file(file_name)
    if spec not in configs:
        raise ValueError(
            f"No spec found named '{spec}'. Check your spelling and config file."
        )
    return configs[spec]


if __name__ == "__main__":
    main()
