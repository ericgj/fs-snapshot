import logging
import sqlite3
import sys
from threading import Thread
from typing import Iterable, Generator, Sequence

from .util import connect_store_db
from ..adapter import filesys
from ..adapter.logging import get_logger
from ..adapter.store import Store
from ..model.config import Config
from ..model.file_info import FileInfo

LOGGER = get_logger(__name__)


def main(config: Config):
    config_desc = config_log_string(config)
    LOGGER.info(f"Start: {config.name}\n{config_desc}")

    conn, store_db = connect_store_db(config)
    id = store_db.create_import(conn, config.name, config.metadata)

    if config.multithread:
        conn.close()
        store_multithread(config, id)
    else:
        store_singlethread(conn, store_db, config, id)

    print(id.hex(), file=sys.stdout)
    LOGGER.info(f"End: {config.name}\n{config_desc}")


def store_multithread(config: Config, id: bytes):
    threads = [
        Thread(target=connect_and_store, args=(config, id, file_type, mps))
        for (file_type, mps) in config.match_paths.items()
    ]

    for th in threads:
        th.start()

    for th in threads:
        th.join()


def store_singlethread(
    conn: sqlite3.Connection, store_db: Store, config: Config, id: bytes
):
    for (file_type, mps) in config.match_paths.items():
        store(conn, store_db, config, id, file_type, mps)


def connect_and_store(
    config: Config, id: bytes, file_type: str, match_paths: Sequence[str]
):
    conn, store_db = connect_store_db(config)
    store(conn, store_db, config, id, file_type, match_paths)


def store(
    conn: sqlite3.Connection,
    store_db: Store,
    config: Config,
    id: bytes,
    file_type: str,
    match_paths: Sequence[str],
):
    LOGGER.info(f"Start file type: {file_type}")
    files = filesys.search(
        config.root_dir,
        match_paths,
        file_type=file_type,
        gather_digests=config.compare_digests,
        is_archived=config.is_archived,
        calc_file_group=config.file_group_from,
    )

    if LOGGER.level <= logging.DEBUG:  # don't degrade performance if not debugging
        files = log_store(files)

    store_db.import_files(conn, id, files)

    LOGGER.info(f"End file type: {file_type}")


def log_store(file_infos: Iterable[FileInfo]) -> Generator[FileInfo, None, None]:
    for file_info in file_infos:
        LOGGER.debug(f"Storing: {file_info.file_name}")
        yield file_info


def config_log_string(config: Config) -> str:
    return "\n".join(
        [
            f"    root_dir: {config.root_dir}",
            f"    store_db_file: {config.store_db_file}",
            "    metadata:",
        ]
        + [f"        {k}: {v}" for (k, v) in config.metadata.items()]
    )
