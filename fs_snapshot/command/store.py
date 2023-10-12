import sqlite3
import sys

from .util import connect_store_db
from ..adapter import filesys
from ..adapter.logging import get_logger
from ..adapter.store import Store
from ..model.config import Config

LOGGER = get_logger(__name__)


def main(config: Config):
    config_desc = config_log_string(config)
    LOGGER.info(f"Start: {config.name}\n{config_desc}")

    conn, store_db = connect_store_db(config)
    id = store_db.create_import(conn, config.name, config.metadata)

    # Note: multithread disabled
    n = store(conn, store_db, config, id)

    print(id.hex(), file=sys.stdout)
    LOGGER.info(f"{n} files stored.")
    LOGGER.info(f"End: {config.name}\n{config_desc}")


def store(
    conn: sqlite3.Connection,
    store_db: Store,
    config: Config,
    id: bytes,
) -> int:
    i = 0
    for file_info in filesys.search(
        root_dir=config.root_dir,
        match_paths=config.match_paths,
        gather_digests=config.compare_digests,
        is_archived=config.is_archived,
        calc_file_group=config.file_group_from,
    ):
        LOGGER.debug(f"Storing: {file_info.file_name}")
        store_db.import_files(conn, id, [file_info])
        i = i + 1
    return i


def config_log_string(config: Config) -> str:
    return "\n".join(
        [
            f"    root_dir: {config.root_dir}",
            f"    store_db_file: {config.store_db_file}",
            "    metadata:",
        ]
        + [f"        {k}: {v}" for (k, v) in config.metadata.items()]
    )
