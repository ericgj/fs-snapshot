from glob import glob
import logging
import os.path
import sqlite3
from typing import Iterable, List, Dict

from fs_snapshot.command import store
from fs_snapshot.adapter.store import deserialized_tags
from fs_snapshot.adapter.logging import init_logger, init_db_logger
from fs_snapshot.model.config import Config, ArchivedBy, NotArchived, ArchivedByMetadata

ROOT_DIR = os.path.join("test", "fixtures", "store")


def test_store_single_match_path():
    root_dir = os.path.join(ROOT_DIR, "data_types")
    match_paths = [
        os.path.join(
            "{data_type}", "{account}", "csv", "{protocol}_{pr_or_qc}_C_*.CSV"
        ),
    ]
    config = build_config(
        root_dir=root_dir,
        match_paths=match_paths,
        metadata={"file_type": "Extract", "data_type": "ECG"},
        archived_by=NotArchived(),
    )

    init_logger(level=logging.DEBUG, log_file=config.log_file)
    init_db_logger(
        level=logging.DEBUG,
        name=config.store_db_log_name,
        log_file=config.store_db_log_file,
    )
    remove_db_files(config)
    store.main(config)

    assert_import_created_with_tags(config, "Extract", "ECG")

    exp = len([f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*.CSV"))])
    assert_n_files_stored(config, exp)


def test_store_multiple_match_paths():
    root_dir = os.path.join(ROOT_DIR, "data_types")
    match_paths = [
        os.path.join(
            "{data_type}", "{account}", "csv", "{protocol}_{pr_or_qc}_C_*.CSV"
        ),
        os.path.join(
            "{data_type}",
            "{account}",
            "csv",
            "{archive}",
            "{protocol}_{pr_or_qc}_C_*.CSV",
        ),
    ]
    config = build_config(
        root_dir=root_dir,
        match_paths=match_paths,
        metadata={"file_type": "Extract", "data_type": "ECG"},
        archived_by=ArchivedByMetadata("archive", {"archived", "archive"}),
    )

    init_logger(level=logging.DEBUG, log_file=config.log_file)
    init_db_logger(
        level=logging.DEBUG,
        name=config.store_db_log_name,
        log_file=config.store_db_log_file,
    )
    remove_db_files(config)
    store.main(config)

    assert_import_created_with_tags(config, "Extract", "ECG")

    exp_normal = [f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*.CSV"))]
    exp_archived = [
        f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*", "*.CSV"))
    ]
    assert_n_files_stored(config, len(exp_normal) + len(exp_archived))

    assert_imported_files(config, exp_normal, archived=False)
    assert_imported_files(config, exp_archived, archived=True)


def assert_import_created_with_tags(config: Config, file_type: str, data_type: str):
    conn = sqlite3.connect(config.store_db_file)
    c = conn.execute("SELECT `tags` FROM `__import__` LIMIT 1;")
    row = c.fetchone()
    if row is None:
        assert False, "DB error: unable to select"
    tags = deserialized_tags(str(row[0]))
    assert tags.get("file_type") == file_type, str(tags)
    assert tags.get("data_type") == data_type, str(tags)


def assert_n_files_stored(config: Config, exp: int):
    conn = sqlite3.connect(config.store_db_file)
    c = conn.execute("SELECT COUNT(*) FROM `file_info`;")
    row = c.fetchone()
    if row is None:
        assert False, "DB error: unable to select"
    act = int(row[0])
    assert exp == act, f"Expected {exp}, was {act}"


def assert_imported_files(config: Config, file_names: Iterable[str], archived=False):
    conn = sqlite3.connect(config.store_db_file)
    c = conn.execute(
        "SELECT dir_name, base_name, archived FROM `file_info` WHERE archived = ?;",
        (1 if archived else 0,),
    )
    rows = c.fetchall()
    if rows is None:
        assert False, "DB error: unable to select"
    act = set(
        [
            (
                os.path.join(str(row[0]), str(row[1])),
                True if int(row[2]) == 1 else False,
            )
            for row in rows
        ]
    )
    exp = set([(f, archived) for f in file_names])
    assert len(act - exp) == 0


def build_config(
    *,
    root_dir: str,
    match_paths: List[str],
    metadata: Dict[str, str],
    archived_by: ArchivedBy,
) -> Config:
    return Config(
        match_paths=match_paths,
        root_dir=root_dir,
        log_file=os.path.join(ROOT_DIR, "output", "fs-snapshot.log"),
        store_db_file=os.path.join(ROOT_DIR, "output", "fs-snapshot.sqlite"),
        store_db_import_table="__import__",
        store_db_file_info_table="file_info",
        metadata=metadata,
        archived_by=archived_by,
    )


def remove_db_files(config: Config):
    if os.path.exists(config.store_db_file):
        os.remove(config.store_db_file)


if __name__ == "__main__":
    test_store_single_match_path()
    test_store_multiple_match_paths()
