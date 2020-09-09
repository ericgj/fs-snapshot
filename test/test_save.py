from glob import glob
import os.path
import sqlite3
from typing import Iterable

from command import save
from adapter.db.monitor import deserialized_tags
from model.config import Config
from model.ert.study_file import SearchSpec, FileType, DataType

ROOT_DIR = os.path.join("test", "fixtures", "save")

CONFIG = Config(
    root_dir=ROOT_DIR,
    monitor_db_root_dir=os.path.join(ROOT_DIR, "output"),
    monitor_db_base_name="monitor.sqlite",
    monitor_db_import_table="__import__",
    monitor_db_file_info_table="file_info",
)


def test_save_single_match_path():
    remove_db_files()
    root_dir = os.path.join(ROOT_DIR, "data_types")
    match_paths = [
        os.path.join(
            "{data_type}", "{account}", "csv", "{protocol}_{pr_or_qc}_C_*.CSV"
        ),
    ]
    spec = SearchSpec(
        file_type=FileType.Extract,
        data_type=DataType.ECG,
        root_dir=root_dir,
        match_paths=match_paths,
    )
    save.main(CONFIG, spec)

    assert_import_created_with_tags(spec.file_type, spec.data_type)

    exp = len([f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*.CSV"))])
    assert_n_files_saved(exp)


def test_save_multiple_match_paths():
    remove_db_files()
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
    spec = SearchSpec(
        file_type=FileType.Extract,
        data_type=DataType.ECG,
        root_dir=root_dir,
        match_paths=match_paths,
    )
    save.main(CONFIG, spec)

    assert_import_created_with_tags(spec.file_type, spec.data_type)

    exp_normal = [f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*.CSV"))]
    exp_archived = [
        f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*", "*.CSV"))
    ]
    assert_n_files_saved(len(exp_normal) + len(exp_archived))

    assert_imported_files(exp_normal, archived=False)
    assert_imported_files(exp_archived, archived=True)


def assert_import_created_with_tags(file_type: FileType, data_type: DataType):
    conn = sqlite3.connect(CONFIG.monitor_db_file)
    c = conn.execute("SELECT `tags` FROM `__import__` LIMIT 1;")
    row = c.fetchone()
    if row is None:
        assert False, "DB error: unable to select"
    tags = deserialized_tags(str(row[0]))
    assert tags.get("file_type") == file_type.name, str(tags)
    assert tags.get("data_type") == data_type.name, str(tags)


def assert_n_files_saved(exp: int):
    conn = sqlite3.connect(CONFIG.monitor_db_file)
    c = conn.execute("SELECT COUNT(*) FROM `file_info`;")
    row = c.fetchone()
    if row is None:
        assert False, "DB error: unable to select"
    act = int(row[0])
    assert exp == act, f"Expected {exp}, was {act}"


def assert_imported_files(file_names: Iterable[str], archived=False):
    conn = sqlite3.connect(CONFIG.monitor_db_file)
    c = conn.execute(
        "SELECT file_name, archived FROM `file_info` WHERE archived = ?;",
        (1 if archived else 0,),
    )
    rows = c.fetchall()
    if rows is None:
        assert False, "DB error: unable to select"
    act = set([(str(row[0]), True if int(row[1]) == 1 else False) for row in rows])
    exp = set([(f, archived) for f in file_names])
    assert len(act - exp) == 0


def remove_db_files():
    if os.path.exists(CONFIG.monitor_db_file):
        os.remove(CONFIG.monitor_db_file)


if __name__ == "__main__":
    test_save_single_match_path()
    test_save_multiple_match_paths()
