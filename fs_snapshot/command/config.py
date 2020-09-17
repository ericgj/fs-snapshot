from ..model.config import Config


def main(config: Config, config_file: str):
    print("-" * 80)
    print(f"Configuration for {config_file}")
    print(f"Section {config.name}")
    print("-" * 80)
    print("metadata:")
    print_metadata(config.metadata)
    print("match_paths:")
    print_match_paths(config.match_paths)
    print("")
    print(f"root_dir                 : {config.root_dir}")
    print(f"log_file                 : {config.log_file}")
    print(f"store_db_file            : {config.store_db_file}")
    print(f"store_db_import_table    : {config.store_db_import_table}")
    print(f"store_db_file_info_table : {config.store_db_file_info_table}")
    print(f"multithread              : {config.multithread}")
    print(f"compare_digests          : {config.compare_digests}")
    print(f"archived_by              : {config.archived_by}")
    print(f"file_group_by            : {config.file_group_by}")
    print("-" * 80)


def print_match_paths(match_paths):
    maxlen = max(len(k) for k in match_paths)
    for (k, paths) in match_paths.items():
        if len(paths) == 1:
            print(f"    {k.ljust(maxlen)} : {paths[0]}")
        else:
            print(f"    {k.ljust(maxlen)} :")
            for path in paths:
                print(f"        - {path}")


def print_metadata(metadata):
    maxlen = max(len(k) for k in metadata)
    for (k, v) in metadata.items():
        print(f"    {k.ljust(maxlen)} : {v}")
