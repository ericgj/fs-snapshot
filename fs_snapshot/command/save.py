from threading import Thread

from .util import connect_store_db
from ..adapter import filesys
from ..model.config import Config, SearchSpec


def main(
    config: Config, spec: SearchSpec,
):
    conn, store_db = connect_store_db(config)
    id = store_db.create_import(
        conn, spec.metadata
    )

    threads = [
        Thread(target=save, args=(config, id, spec.root_dir, mp))
        for mp in spec.match_paths
    ]
    for th in threads:
        th.start()

    for th in threads:
        th.join()


def save(config: Config, id: bytes, root_dir: str, match_path: str):
    conn, store_db = connect_store_db(config)
    store_db.import_files(
        conn, id, filesys.search(root_dir, match_path, is_archived=has_archive_path),
    )


def has_archive_path(metadata) -> bool:
    return "archive" in metadata
