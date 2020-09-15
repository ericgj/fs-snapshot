import logging
import sys
from threading import Thread
from typing import Iterator, Generator

from .util import connect_store_db
from ..adapter import filesys
from ..adapter.logging import get_logger
from ..model.config import Config
from ..model.file_info import FileInfo

LOGGER = get_logger(__name__)


def main(config: Config):
    conn, store_db = connect_store_db(config)
    id = store_db.create_import(conn, config.metadata)
    config_desc = config_log_string(config)

    threads = [Thread(target=store, args=(config, id, mp)) for mp in config.match_paths]

    LOGGER.info(f"Start:\n{config_desc}")
    for th in threads:
        th.start()

    for th in threads:
        th.join()

    print(id.hex(), file=sys.stdout)
    LOGGER.info(f"End:\n{config_desc}")


def store(config: Config, id: bytes, match_path: str):
    conn, store_db = connect_store_db(config)
    files: Iterator[FileInfo] = filesys.search(
        config.root_dir,
        match_path,
        is_archived=config.is_archived,
        calc_file_group=config.file_group_from,
        calc_file_type=config.file_type_from,
    )
    if LOGGER.level <= logging.DEBUG:  # don't degrade performance if not debugging
        files = log_store(files)
    store_db.import_files(conn, id, files)


def log_store(file_infos: Iterator[FileInfo]) -> Generator[FileInfo, None, None]:
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
