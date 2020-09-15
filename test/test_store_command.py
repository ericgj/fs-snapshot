from glob import glob
import logging
import os.path
import os
import sqlite3
from typing import Iterable, List, Dict, Tuple

from fs_snapshot.command import store
from fs_snapshot.adapter.store import deserialized_tags
from fs_snapshot.adapter.logging import init_logger, init_db_logger
from fs_snapshot.model.config import (
    Config,
    ArchivedBy,
    NotArchived,
    ArchivedByMetadata,
    CalcBy,
    NoCalc,
    CalcByMetadata,
)

ROOT_DIR = os.path.join("test", "fixtures", "store")


def test_store_single_match_path():
    root_dir = os.path.join(ROOT_DIR, "data_types")
    match_paths = [
        os.path.join(
            "{data_type}", "{account}", "csv", "{protocol}_{pr_or_qc}_C_*.CSV"
        ),
    ]
    config = build_config(
        name="ECG Extracts",
        root_dir=root_dir,
        match_paths=match_paths,
        metadata={"file_type": "Extract", "data_type": "ECG"},
        archived_by=NotArchived(),
        file_group_by=NoCalc(),
        file_type_by=NoCalc(),
    )

    init_logger(level=logging.DEBUG, log_file=config.log_file)
    init_db_logger(
        level=logging.DEBUG,
        name=config.store_db_log_name,
        log_file=config.store_db_log_file,
    )
    remove_db_files(config)
    store.main(config)

    assert_import_created_with_name_and_tags(config, "ECG Extracts", "Extract", "ECG")

    exp = len([f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*.CSV"))])
    assert_n_files_stored(config, exp)


def test_store_multiple_match_paths_with_archive():
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
        name="ECG Extracts",
        root_dir=root_dir,
        match_paths=match_paths,
        metadata={"file_type": "Extract", "data_type": "ECG"},
        archived_by=ArchivedByMetadata("archive", {"archived", "archive"}),
        file_group_by=NoCalc(),
        file_type_by=NoCalc(),
    )

    init_logger(level=logging.DEBUG, log_file=config.log_file)
    init_db_logger(
        level=logging.DEBUG,
        name=config.store_db_log_name,
        log_file=config.store_db_log_file,
    )
    remove_db_files(config)
    store.main(config)

    assert_import_created_with_name_and_tags(config, "ECG Extracts", "Extract", "ECG")

    exp_normal = [f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*.CSV"))]
    exp_archived = [
        f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*", "*.CSV"))
    ]
    assert_n_files_stored(config, len(exp_normal) + len(exp_archived))

    assert_imported_files(config, exp_normal, archived=False)
    assert_imported_files(config, exp_archived, archived=True)


def test_store_with_calc():
    root_dir = os.path.join(ROOT_DIR, "data_types")
    match_paths = [
        os.path.join(
            "{data_type}", "{account}", "csv", "{protocol}_{pr_or_qc}_C_*.CSV"
        ),
    ]
    config = build_config(
        name="ECG Extracts",
        root_dir=root_dir,
        match_paths=match_paths,
        metadata={"file_type": "Extract", "data_type": "ECG"},
        archived_by=NotArchived(),
        file_group_by=CalcByMetadata(format="{protocol} // {account}"),
        file_type_by=CalcByMetadata(format="{pr_or_qc}"),
    )

    init_logger(level=logging.DEBUG, log_file=config.log_file)
    init_db_logger(
        level=logging.DEBUG,
        name=config.store_db_log_name,
        log_file=config.store_db_log_file,
    )
    remove_db_files(config)
    store.main(config)

    assert_import_created_with_name_and_tags(config, "ECG Extracts", "Extract", "ECG")

    """ 
    Note: this is quite fragile. Depends on the protocol part of the file name
    being exactly 9 characters long for each fixture file.
    """
    exps = [
        (
            f,
            f"{f.split(os.sep)[7][:9]} // {f.split(os.sep)[5]}",
            f.split(os.sep)[7][10:12],
        )
        for f in glob(os.path.join(root_dir, "*", "*", "csv", "*.CSV"))
    ]

    assert_files_stored_with_group_and_type(config, exps)


def assert_import_created_with_name_and_tags(
    config: Config, name: str, file_type: str, data_type: str
):
    conn = sqlite3.connect(config.store_db_file)
    c = conn.execute("SELECT `name`, `tags` FROM `__import__` LIMIT 1;")
    row = c.fetchone()
    if row is None:
        assert False, "DB error: unable to select"
    assert str(row[0]) == name
    tags = deserialized_tags(str(row[1]))
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


def assert_files_stored_with_group_and_type(
    config: Config, exps: Iterable[Tuple[str, str, str]]
):
    conn = sqlite3.connect(config.store_db_file)
    c = conn.execute(
        "SELECT `dir_name`, `base_name`, `file_group`, `file_type` FROM `file_info`;"
    )
    rows = c.fetchall()
    if rows is None:
        assert False, "DB error: unable to select"
    row_lookup = dict([(os.path.join(str(r[0]), str(r[1])), r) for r in rows])
    for (exp_name, exp_group, exp_type) in exps:
        assert exp_name in row_lookup, f"Expected file stored: {exp_name}"
        row = row_lookup[exp_name]
        act_group = str(row[2])
        act_type = str(row[3])
        assert exp_group == act_group
        assert exp_type == act_type


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
    name: str,
    root_dir: str,
    match_paths: List[str],
    metadata: Dict[str, str],
    archived_by: ArchivedBy,
    file_group_by: CalcBy,
    file_type_by: CalcBy,
) -> Config:
    return Config(
        name=name,
        match_paths=match_paths,
        root_dir=root_dir,
        log_file=os.path.join(ROOT_DIR, "output", "fs-snapshot.log"),
        store_db_file=os.path.join(ROOT_DIR, "output", "fs-snapshot.sqlite"),
        store_db_import_table="__import__",
        store_db_file_info_table="file_info",
        metadata=metadata,
        archived_by=archived_by,
        file_group_by=file_group_by,
        file_type_by=file_type_by,
    )


def remove_db_files(config: Config):
    if os.path.exists(config.store_db_file):
        os.remove(config.store_db_file)


if __name__ == "__main__":
    test_store_single_match_path()
    test_store_multiple_match_paths_with_archive()
    test_store_with_calc()
