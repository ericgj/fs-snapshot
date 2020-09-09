from glob import glob
import os.path
import sqlite3

from command import save
from model.config import Config
from model.study_file import SearchSpec, FileType

ROOT_DIR = os.path.join("test", "fixtures", "save")

CONFIG = Config(
    root_dir=ROOT_DIR,
    monitor_db_root_dir=os.path.join(ROOT_DIR, "output"),
    monitor_db_base_name="monitor.sqlite",
    monitor_db_import_table="__import__",
    monitor_db_study_file_table="study_file",
)


def test_save_single_match_path():
    remove_db_files()
    root_dir = os.path.join(ROOT_DIR, "data_types")
    match_path = os.path.join("{data_type}", "{account}", "csv", "*.CSV")
    spec = SearchSpec(
        file_type=FileType.Extract, root_dir=root_dir, match_paths=[match_path],
    )
    save.main(CONFIG, spec)

    exp = len([f for f in glob(os.path.join(root_dir, "*", "*", "csv", "*.CSV"))])
    assert_n_files_saved(exp)


def assert_n_files_saved(exp: int):
    conn = sqlite3.connect(CONFIG.monitor_db_file)
    c = conn.execute("SELECT COUNT(*) FROM `study_file`;")
    row = c.fetchone()
    if row is None:
        assert False, "DB error: unable to select count"
    act = int(row[0])
    assert exp == act


def remove_db_files():
    if os.path.exists(CONFIG.monitor_db_file):
        os.remove(CONFIG.monitor_db_file)


if __name__ == "__main__":
    test_save_single_match_path()
