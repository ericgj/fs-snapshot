from glob import glob
from logging import Logger
import os
import os.path
import platform
import re
import sqlite3
import subprocess
from typing import Any, Protocol, Optional, Generator, Iterable, Callable, Set, List

PLATFORM_SYSTEM = platform.system().lower()

REGEXP_INTERNAL_QUOTE = re.compile(r"^'|([^'])'")


# ------------------------------------------------------------------------------
# Error handling
# ------------------------------------------------------------------------------


class SqliteError(Exception):
    def __init__(self, err: sqlite3.Error, sql: str, params=None):
        self.err = err
        self.sql = sql
        self.params = params

    def __str__(self) -> str:
        return "\n".join(
            [
                str(self.err),
                "",
                "The following sql was attempted to be executed:",
                "-----------------------------------------------",
                self.sql,
                "-----------------------------------------------",
            ]
        )


class HasLogger(Protocol):
    logger: Logger


def log_errors(fn):
    """
    Decorator for methods of objects with `logger` property defined
    """

    def _log_errors(self: HasLogger, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except SqliteError as e:
            self.logger.error(f"{e.__class__.__name__}: {e}")
            raise e
        except sqlite3.Error as e:
            self.logger.error(f"{e.__class__.__name__}: {e}")
            raise e
        except sqlite3.Warning as w:
            self.logger.warning(str(w))
            raise w

    return _log_errors


# ------------------------------------------------------------------------------
# Connection init
# ------------------------------------------------------------------------------


def connect(db_file: str, logger: Optional[Logger] = None) -> sqlite3.Connection:
    c = sqlite3.connect(db_file)
    if logger is not None:
        c.set_trace_callback(logger.info)
    c.execute('PRAGMA foreign_keys = "ON"')
    c.enable_load_extension(True)
    for ext in fetch_extensions():
        c.load_extension(ext)
        if logger is not None:
            logger.debug(f"/* loaded sqlite3 extension: {ext} */")
    c.row_factory = sqlite3.Row
    return c


def fetch_extensions() -> Generator[str, None, None]:
    for fname in glob(os.path.join(f"ext/{PLATFORM_SYSTEM}/*")):
        base, _ = os.path.splitext(fname)
        yield base


# ------------------------------------------------------------------------------
# DB Schema
# ------------------------------------------------------------------------------

# TODO: use sqlite_ tables to fetch this info vs the shell


def table_exists(db_file: str, table: str) -> bool:
    return table in fetch_tables(db_file)


def fetch_tables(db_file: str) -> Set[str]:
    p = subprocess.run(
        ["sqlite3", db_file, ".tables"], capture_output=True, check=True, text=True
    )
    if p.stdout is None:
        return set()
    outp = p.stdout.strip()
    if outp == 0:
        return set()
    else:
        return set(t.strip() for t in outp.split())


# ------------------------------------------------------------------------------
# Base DBI adapters
# ------------------------------------------------------------------------------


def select(
    conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()
) -> List[sqlite3.Row]:
    c = _execute(conn, sql, params)
    empty_: List[sqlite3.Row] = []
    rows = c.fetchall()
    return empty_ if rows is None else rows


def select_one(
    conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()
) -> sqlite3.Row:
    c = _execute(conn, sql, params)
    row: Optional[sqlite3.Row] = c.fetchone()
    if row is None:
        raise IndexError()
    else:
        return row


def execute(
    conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()
) -> Optional[str]:
    c = _execute(conn, sql, params)
    return None if c.lastrowid is None else str(c.lastrowid)


def execute_many(
    conn: sqlite3.Connection, sql: str, params: Iterable[Iterable[Any]]
) -> Optional[int]:
    c = _executemany(conn, sql, params)
    return None if c.rowcount is None else int(c.rowcount)


def execute_script(conn: sqlite3.Connection, sql: str) -> None:
    _executescript(conn, sql)
    return None


def _execute(
    conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()
) -> sqlite3.Cursor:
    try:
        return conn.execute(sql, params)
    except sqlite3.Error as e:
        raise SqliteError(e, sql, params)


def _executemany(
    conn: sqlite3.Connection, sql: str, params: Iterable[Iterable[Any]]
) -> sqlite3.Cursor:
    try:
        return conn.executemany(sql, params)
    except sqlite3.Error as e:
        raise SqliteError(e, sql, params)


def _executescript(conn: sqlite3.Connection, sql: str) -> sqlite3.Cursor:
    try:
        return conn.executescript(sql)
    except sqlite3.Error as e:
        raise SqliteError(e, sql)


# ------------------------------------------------------------------------------
# Archiving db files
# ------------------------------------------------------------------------------


def archive(
    db_file: str,
    logger: Optional[Logger] = None,
    backup_file: Optional[str] = None,
    progress: Optional[Callable[[int, int, int], None]] = None,
):
    if backup_file is None:
        backup_file = fetch_next_backup_filename(db_file)
    conn = connect(db_file, logger)
    backup_conn = sqlite3.connect(backup_file)
    with backup_conn:
        conn.backup(backup_conn, pages=1, progress=progress)
    backup_conn.close()
    conn.close()
    os.remove(db_file)


def fetch_next_backup_filename(db_file: str):
    dir, fname = os.path.split(db_file)
    base, ext = os.path.splitext(fname)
    n = len([m for m in glob(os.path.join(dir, f"{base}.*{ext}"))])
    return os.path.join(dir, f"{base}.{n+1}{ext}")


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def name_literal(name: str) -> str:
    return f"`{name}`"


def quoted_string_literal(s: str) -> str:
    """ Note: I'm sure this doesn't block SQL injection so use with caution """
    return "'" + re.sub(REGEXP_INTERNAL_QUOTE, r"\1''", s) + "'"
