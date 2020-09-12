import logging
import os.path
import os
import sqlite3
from time import time, sleep
from typing import Optional, Iterable, Tuple, List, Dict

from hypothesis import given, settings, HealthCheck
import hypothesis.strategies as hyp


from examples.file_info import (
    list_of_examples,
    split_list_of_examples,
    with_changes,
    with_copies,
    then_was_moved,
    then_was_renamed,
    then_was_archived,
    then_was_modified,
)
from adapter.db.monitor import Monitor
from adapter import db
from model.config import Config
from model.file_info import (
    diff_all,
    FileInfo,
    CompareStates,
    Digest,
    Action,
    Created,
    Removed,
    Copied,
    Moved,
    Modified,
    Archived,
    Renamed,
)

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
def test_diff_changed(pair: Tuple[Iterable[FileInfo], Iterable[FileInfo]]):
    original_files, changed_files = pair

    conn, monitor_db = connect_monitor_db(CONFIG)
    reset_db(conn, monitor_db)
    monitor_db.init_tables(conn)

    latest_id, compares = import_and_compare(
        conn, monitor_db, original_files, changed_files
    )

    actions = diff_all(compares)
    assert_actions(
        actions,
        dict(
            [
                (f.digest, [Moved, None, Modified, Archived, Renamed,][i % 5])
                for (i, f) in enumerate(original_files)
            ]
        ),
    )


@given(
    pair=list_of_examples(CURRENT_TIME, min_size=15, max_size=15).flatmap(
        with_copies(lambda i, f: then_was_moved(f) if (i % 3) else hyp.just(None))
    )
)
@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow], max_examples=5)
def test_diff_copied(pair: Tuple[List[FileInfo], List[FileInfo]]):
    original_files, copied_files = pair
    assert len(copied_files) > 0

    conn, monitor_db = connect_monitor_db(CONFIG)
    reset_db(conn, monitor_db)
    monitor_db.init_tables(conn)

    latest_id, compares = import_and_compare(
        conn, monitor_db, original_files, original_files + copied_files
    )

    actions = diff_all(compares)
    assert_actions(actions, dict([(f.digest, Copied) for f in copied_files]))


@given(
    created_and_original_files=split_list_of_examples(
        CURRENT_TIME, min_size=1, max_size=15
    )
)
@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow], max_examples=5)
def test_diff_created(
    created_and_original_files: Tuple[List[FileInfo], List[FileInfo]]
):
    created_files, original_files = created_and_original_files

    conn, monitor_db = connect_monitor_db(CONFIG)
    reset_db(conn, monitor_db)
    monitor_db.init_tables(conn)

    latest_id, compares = import_and_compare(
        conn, monitor_db, original_files, original_files + created_files
    )

    actions = diff_all(compares)
    assert_actions(actions, dict([(f.digest, Created) for f in created_files]))


@given(
    removed_and_original_files=split_list_of_examples(
        CURRENT_TIME, min_size=1, max_size=15
    )
)
@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow], max_examples=5)
def test_diff_removed(
    removed_and_original_files: Tuple[List[FileInfo], List[FileInfo]]
):
    removed_files, original_files = removed_and_original_files

    conn, monitor_db = connect_monitor_db(CONFIG)
    reset_db(conn, monitor_db)
    monitor_db.init_tables(conn)

    latest_id, compares = import_and_compare(
        conn, monitor_db, original_files + removed_files, original_files
    )

    actions = diff_all(compares)
    assert_actions(actions, dict([(f.digest, Removed) for f in removed_files]))


def assert_actions(actions: Iterable[Action], exp: Dict[Digest, Optional[type]]):
    for action in actions:
        digest = get_original_digest(action)
        assert digest is not None
        if digest is None:
            return

        assert digest in exp
        action_type = exp[digest]
        assert action_type is not None
        if action_type is None:
            return

        assert isinstance(action, action_type)


def get_original_digest(action: Action) -> Optional[Digest]:
    if isinstance(action, Created):
        return None
    if isinstance(action, Removed):
        return action.original.digest
    if isinstance(action, Copied):
        return action.original.digest
    if isinstance(action, Moved):
        return action.original.digest
    if isinstance(action, Renamed):
        return action.original.digest
    if isinstance(action, Archived):
        return action.original.digest
    if isinstance(action, Modified):
        return action.original.digest
    raise ValueError(f"Unknown action type: {type(action)}")


def import_and_compare(
    conn: sqlite3.Connection,
    monitor_db: Monitor,
    original_files: Iterable[FileInfo],
    changed_files: Iterable[FileInfo],
) -> Tuple[bytes, List[CompareStates]]:
    id = monitor_db.create_import(conn)
    monitor_db.import_files(conn, id, original_files)

    sleep(1.1)
    new_id = monitor_db.create_import(conn)
    monitor_db.import_files(conn, new_id, changed_files)

    latest_id, compares = monitor_db.fetch_file_import_compare_latest(conn, id)
    assert new_id == latest_id

    return (latest_id, compares)


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
