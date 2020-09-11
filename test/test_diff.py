import logging
import os.path
import os
import sqlite3
from time import time, sleep

from hypothesis import given, settings, HealthCheck
import hypothesis.strategies as hyp


from examples.file_info import (
    list_of_examples,
    with_changes,
    then_was_moved,
    then_was_renamed,
    then_was_archived,
    then_was_modified,
)
from adapter.db.monitor import Monitor
from adapter import db
from model.config import Config
from model.file_info import diff_all  # , Action

CURRENT_TIME = time()

ROOT_DIR = os.path.join("test", "fixtures", "diff")

CONFIG = Config(
    root_dir=ROOT_DIR,
    monitor_db_root_dir=os.path.join(ROOT_DIR, "output"),
    monitor_db_base_name="monitor.sqlite",
    monitor_db_import_table="__import__",
    monitor_db_file_info_table="file_info",
)


@given(
    pair=list_of_examples(CURRENT_TIME, min_size=15, max_size=15).flatmap(
        with_changes(
            lambda i, f: [
                then_was_moved(f),
                hyp.just(f),
                then_was_modified(f),
                then_was_archived(f),
                then_was_renamed(f),
            ][i % 5]
        )
    )
)
@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow], max_examples=5)
def test_diff_no_new_or_removed(pair):
    original_files, changed_files = pair

    conn, monitor_db = connect_monitor_db(CONFIG)
    reset_db(conn, monitor_db)

    monitor_db.init_tables(conn)

    id = monitor_db.create_import(conn)
    monitor_db.import_files(conn, id, original_files)

    sleep(0.5)
    new_id = monitor_db.create_import(conn)
    monitor_db.import_files(conn, new_id, changed_files)

    latest_id, compares = monitor_db.fetch_file_import_compare_latest(conn, id)
    assert new_id == latest_id

    actions = diff_all(compares)
    actions
    # TODO assert actions match expected


def connect_monitor_db(config):
    conn = db.connect(config.monitor_db_file, config.monitor_db_log_file)
    monitor_db = Monitor(
        import_table=config.monitor_db_import_table,
        file_info_table=config.monitor_db_file_info_table,
        logger=logging.getLogger(config.monitor_db_log_name),
    )
    return (conn, monitor_db)


def remove_db_files():
    if os.path.exists(CONFIG.monitor_db_file):
        os.remove(CONFIG.monitor_db_file)


def reset_db(conn: sqlite3.Connection, monitor_db: Monitor):
    monitor_db.init_tables(conn)
    with conn:
        db.execute(conn, "DELETE FROM `file_info`;")
        db.execute(conn, "DELETE FROM `__import__`;")
