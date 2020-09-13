from threading import Thread

from .util import connect_store_db
from ..adapter import filesys
from ..model.config import Config


def main(config: Config):
    conn, store_db = connect_store_db(config)
    id = store_db.create_import(
        conn, config.metadata
    )

    threads = [
        Thread(target=save, args=(config, id, mp))
        for mp in config.match_paths
    ]
    for th in threads:
        th.start()

    for th in threads:
        th.join()


def save(config: Config, id: bytes, match_path: str):
    conn, store_db = connect_store_db(config)
    store_db.import_files(
        conn, id, filesys.search(config.root_dir, match_path, is_archived=config.is_archived),
    )


def has_archive_path(metadata) -> bool:
    return "archive" in metadata
