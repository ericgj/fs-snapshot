from html import escape
import json
import re
import sqlite3
from typing import Optional, Iterable, List, TextIO

from .util import connect
from ..adapter import db
from ..model.config import Config

FORMATS = {"json", "html"}

COMMENT_REGEX = re.compile(r"\s*\/\*\s*([^*]+)\s*\*\/")


def main(
    config: Config,
    input_files: Iterable[TextIO],
    output_file: TextIO,
    *,
    snapshot: Optional[bytes] = None,
    format: str = "json",
):
    conn = connect(config)
    conn.execute("PRAGMA query_only;")  # force db into read only state

    for input_file in input_files:
        sql = input_file.read().strip()
        title = parse_title_comment(sql)
        rows = select(conn, sql, snapshot=snapshot)
        print(serialize(rows, title=title, format=format), file=output_file)


def select(
    conn: sqlite3.Connection, sql: str, snapshot: Optional[bytes]
) -> List[sqlite3.Row]:
    rows: List[sqlite3.Row] = []
    if "?" in sql:  # a bit of a hack
        if snapshot is None:
            raise ValueError("This query requires a snapshot parameter")
        rows = db.select(conn, sql, (snapshot,))
    else:
        rows = db.select(conn, sql, ())
    return rows


def parse_title_comment(sql: str) -> Optional[str]:
    m = re.match(COMMENT_REGEX, sql)
    return None if m is None else m.group(1).strip()


def serialize(
    rows: List[sqlite3.Row], *, format: str, title: Optional[str] = None
) -> str:
    if format == "json":
        return serialize_json(rows, title=title)
    if format == "html":
        return serialize_html(rows, title=title)
    raise ValueError(f"Unknown format: {format}")


def serialize_json(rows: List[sqlite3.Row], title: Optional[str] = None) -> str:
    results = [dict(r) for r in rows]
    if title is None:
        return json.dumps(results, indent=2)
    else:
        return json.dumps({"title": title, "results": results})


def serialize_html(rows: List[sqlite3.Row], title: Optional[str] = None) -> str:
    lines = []
    if title is not None:
        lines.append(f"<h1>{escape(title)}</h1>")
    n = 0
    for (i, row) in enumerate(rows):
        dictrow = dict(row)
        n = i
        if i == 0:
            lines.append("<table>")
            lines.append("<thead>")
            lines.append("<tr>")
            lines.append("".join(f"<th>{escape(k)}</th>" for k in dictrow))
            lines.append("</tr>")
            lines.append("</thead>")

        lines.append("<tr>")
        lines.append("".join(f"<td>{escape(str(v))}</td>" for v in dictrow.values()))
        lines.append("</tr>")

    if n > 0:
        lines.append("</table>")

    return "".join(lines)
