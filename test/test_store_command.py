from glob import glob
import logging
import os.path
import os
import sqlite3
from typing import Iterable, Mapping, Sequence, Dict, Tuple

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


def test_store_single_match_path_with_compare_digests():
    root_dir = os.path.join(ROOT_DIR, "data_types")
    match_paths = {
        "test": [
            os.path.join(
                "{data_type}", "{account}", "csv", "{protocol}_{pr_or_qc}_C_*.CSV"
            ),
        ]
    }
    config = build_config(
        name="store_single_match_path_with_compare_digests",
        root_dir=root_dir,
        match_paths=match_paths,
        compare_digests=True,
        metadata={"file_type": "Extract", "data_type": "ECG"},
        archived_by=NotArchived(),
        file_group_by=NoCalc(),
    )

    init_logger(level=logging.DEBUG, log_file=config.log_file)
    init_db_logger(
        level=logging.DEBUG,
        name=config.store_db_log_name,
        log_file=config.store_db_log_file,
    )
    remove_db_files(config)
    store.main(config)

    assert_import_created_with_name_and_tags(
        config, "store_single_match_path_with_compare_digests", "Extract", "ECG"
    )

    exp_all = len(
        [
            f
            for f in glob(os.path.join(root_dir, "**"), recursive=True)
            if os.path.isfile(f)
        ]
    )
    exp_tagged = len(
        [f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*.CSV"))]
    )
    assert_n_files_stored_with_tags(config, exp_tagged)
    assert_n_files_stored_without_tags(config, exp_all - exp_tagged)


def test_store_multiple_match_paths_with_archive_no_compare_digests():
    root_dir = os.path.join(ROOT_DIR, "data_types")
    match_paths = {
        "test": [
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
    }
    config = build_config(
        name="store_multiple_match_paths_with_archive_no_compare_digests",
        root_dir=root_dir,
        match_paths=match_paths,
        compare_digests=False,
        metadata={"file_type": "Extract", "data_type": "ECG"},
        archived_by=ArchivedByMetadata("archive", {"archived", "archive"}),
        file_group_by=NoCalc(),
    )

    init_logger(level=logging.DEBUG, log_file=config.log_file)
    init_db_logger(
        level=logging.DEBUG,
        name=config.store_db_log_name,
        log_file=config.store_db_log_file,
    )
    remove_db_files(config)
    store.main(config)

    assert_import_created_with_name_and_tags(
        config,
        "store_multiple_match_paths_with_archive_no_compare_digests",
        "Extract",
        "ECG",
    )

    exp_all = [
        f
        for f in glob(os.path.join(root_dir, "**"), recursive=True)
        if os.path.isfile(f)
    ]
    exp_normal = [f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*.CSV"))]
    exp_archived = [
        f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*", "*.CSV"))
    ]
    assert_n_files_stored_without_tags(
        config, len(exp_all) - (len(exp_normal) + len(exp_archived))
    )
    assert_n_files_stored_with_tags(config, len(exp_normal) + len(exp_archived))

    # Note: fragile; assumes unmatched file has a different extension than .CSV in fixture
    exp_not_archived = exp_normal + [
        f for f in exp_all if not os.path.splitext(f)[1] == ".CSV"
    ]
    assert_imported_files(config, exp_not_archived, archived=False)
    assert_imported_files(config, exp_archived, archived=True)


def test_store_with_calc():
    root_dir = os.path.join(ROOT_DIR, "data_types")
    match_paths = {
        "test": [
            os.path.join(
                "{data_type}", "{account}", "csv", "{protocol}_{pr_or_qc}_C_*.CSV"
            ),
        ]
    }
    config = build_config(
        name="store_with_calc",
        root_dir=root_dir,
        match_paths=match_paths,
        compare_digests=False,
        metadata={"file_type": "Extract", "data_type": "ECG"},
        archived_by=NotArchived(),
        file_group_by=CalcByMetadata(format="{protocol} // {account}"),
    )

    init_logger(level=logging.DEBUG, log_file=config.log_file)
    init_db_logger(
        level=logging.DEBUG,
        name=config.store_db_log_name,
        log_file=config.store_db_log_file,
    )
    remove_db_files(config)
    store.main(config)

    assert_import_created_with_name_and_tags(
        config, "store_with_calc", "Extract", "ECG"
    )

    """ 
    Note: this is quite fragile. Depends on the protocol part of the file name
    being exactly 9 characters long for each fixture file.
    """
    exps = [
        (
            f,
            f"{f.split(os.sep)[7][:9]} // {f.split(os.sep)[5]}",
        )
        for f in glob(os.path.join(root_dir, "*", "*", "csv", "*.CSV"))
    ]

    assert_files_stored_with_group(config, exps)


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


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


def assert_n_files_stored_with_tags(config: Config, exp: int):
    conn = sqlite3.connect(config.store_db_file)
    c = conn.execute("SELECT COUNT(*) FROM `file_info` WHERE LENGTH(`tags`) > 0;")
    row = c.fetchone()
    if row is None:
        assert False, "DB error: unable to select"
    act = int(row[0])
    assert exp == act, f"Expected {exp}, was {act}"


def assert_n_files_stored_without_tags(config: Config, exp: int):
    conn = sqlite3.connect(config.store_db_file)
    c = conn.execute("SELECT COUNT(*) FROM `file_info` WHERE LENGTH(`tags`) = 0;")
    row = c.fetchone()
    if row is None:
        assert False, "DB error: unable to select"
    act = int(row[0])
    assert exp == act, f"Expected {exp}, was {act}"


def assert_files_stored_with_group(config: Config, exps: Iterable[Tuple[str, str]]):
    conn = sqlite3.connect(config.store_db_file)
    c = conn.execute("SELECT `dir_name`, `base_name`, `file_group` FROM `file_info`;")
    rows = c.fetchall()
    if rows is None:
        assert False, "DB error: unable to select"
    row_lookup = dict([(os.path.join(str(r[0]), str(r[1])), r) for r in rows])
    for exp_name, exp_group in exps:
        assert exp_name in row_lookup, f"Expected file stored: {exp_name}"
        row = row_lookup[exp_name]
        act_group = str(row[2])
        assert exp_group == act_group


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
    match_paths: Mapping[str, Sequence[str]],
    compare_digests: bool,
    metadata: Dict[str, str],
    archived_by: ArchivedBy,
    file_group_by: CalcBy,
) -> Config:
    return Config(
        name=name,
        match_paths=match_paths,
        root_dir=root_dir,
        log_file=os.path.join(ROOT_DIR, "output", f"{name}.log"),
        store_db_file=os.path.join(ROOT_DIR, "output", f"{name}.sqlite"),
        store_db_import_table="__import__",
        store_db_file_info_table="file_info",
        compare_digests=compare_digests,
        metadata=metadata,
        archived_by=archived_by,
        file_group_by=file_group_by,
    )


def remove_db_files(config: Config):
    if os.path.exists(config.store_db_file):
        os.remove(config.store_db_file)


if __name__ == "__main__":
    test_store_single_match_path_with_compare_digests()
    test_store_multiple_match_paths_with_archive_no_compare_digests()
    test_store_with_calc()
