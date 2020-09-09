import sqlite3
from time import time
from uuid import uuid4
from typing import Optional, Iterable, Any, Tuple, Dict, List

from adapter import db
from model.file_info import FileInfo


class Monitor:
    def __init__(self, *, import_table: str, file_info_table: str):
        self.import_table = import_table
        self.file_info_table = file_info_table

    def create_import(
        self, conn: sqlite3.Connection, tags: Dict[str, str] = {}
    ) -> bytes:
        id = self.get_import_id()
        with conn:
            self.init_tables(conn)
            ts = time()
            self.init_import(conn, id, ts, tags)
            self.delete_file_infos(conn, id)
        return id

    def import_files(
        self, conn: sqlite3.Connection, id: bytes, files: Iterable[FileInfo]
    ):
        with conn:
            self.insert_file_infos(conn, id, files)

    def fetch_imported_files(
        self, conn: sqlite3.Connection, id: bytes
    ) -> Optional[Tuple[float, Dict[str, str], List[FileInfo]]]:
        rows = self.select_imported_file_infos(conn, id)
        if len(rows) == 0:
            return None
        ts = float(rows[0]["import_timestamp"])
        tags = deserialized_tags(str(rows[0]["import_tags"]))
        return (
            ts,
            tags,
            [
                FileInfo(
                    digest=bytes(row["digest"]),
                    file_name=str(row["file_name"]),
                    created=float(row["created"]),
                    modified=float(row["modified"]),
                    size=int(row["size"]),
                    archived=True if int(row["archived"]) == 1 else False,
                    metadata=deserialized_tags(str(row["tags"])),
                )
                for row in rows
            ],
        )

    def get_import_id(self) -> bytes:  # impure
        return uuid4().bytes

    def init_tables(self, conn: sqlite3.Connection):
        self.init_import_table(conn)
        self.init_file_info_table(conn)

    def init_import_table(self, conn: sqlite3.Connection):
        sql = sql_script_init_import_table(self.import_table)
        db.execute_script(conn, sql)

    def init_file_info_table(self, conn: sqlite3.Connection):
        sql = sql_script_init_file_info_table(
            file_info_table=self.file_info_table, import_table=self.import_table,
        )
        db.execute_script(conn, sql)

    def init_import(
        self,
        conn: sqlite3.Connection,
        id: bytes,
        timestamp: float,
        tags: Dict[str, str] = {},
    ):
        sql, params = sql_insert_import(self.import_table, id, int(timestamp), tags)
        db.execute(conn, sql, params)

    def delete_file_infos(self, conn: sqlite3.Connection, id: bytes):
        sql, params = sql_delete_file_info(self.file_info_table, id)
        db.execute(conn, sql, params)

    def insert_file_infos(
        self, conn: sqlite3.Connection, id: bytes, files: Iterable[FileInfo]
    ):
        sql, params = sql_insert_file_info(self.file_info_table, id, files)
        db.execute_many(conn, sql, params)

    def select_imported_file_infos(
        self, conn: sqlite3.Connection, id: bytes
    ) -> List[sqlite3.Row]:
        sql, params = sql_select_imported_file_infos(
            self.file_info_table, self.import_table, id
        )
        return db.select(conn, sql, params)


def sql_script_init_import_table(import_table: str) -> str:
    import_table_literal = db.name_literal(import_table)
    index_literal = db.name_literal(f"{import_table}_timestamp")
    return f"""
CREATE TABLE IF NOT EXISTS {import_table_literal}
    ( `id` BYTES PRIMARY KEY
    , `timestamp` INT
    , `tags` TEXT
    )
;
CREATE INDEX IF NOT EXISTS {index_literal} ON {import_table_literal} (`timestamp`);
    """


def sql_script_init_file_info_table(*, file_info_table: str, import_table: str) -> str:
    file_info_table_literal = db.name_literal(file_info_table)
    import_table_literal = db.name_literal(import_table)
    digest_index_literal = db.name_literal(f"{file_info_table}_digest")
    file_name_index_literal = db.name_literal(f"{file_info_table}_file_name")
    return f"""
CREATE TABLE IF NOT EXISTS {file_info_table_literal}
    ( `digest` BYTES NOT NULL
    , `file_name` TEXT NOT NULL
    , `created` INT NOT NULL
    , `modified` INT NOT NULL
    , `size` INT NOT NULL
    , `archived` TINYINT NOT NULL
    , `tags` TEXT NULL
    , `import_id` BYTES NOT NULL
    , FOREIGN KEY(`import_id`) REFERENCES {import_table_literal}(`id`)
    )
;
CREATE INDEX IF NOT EXISTS {digest_index_literal} ON {file_info_table_literal} (`digest`);
CREATE INDEX IF NOT EXISTS {file_name_index_literal} ON {file_info_table_literal} (`file_name`);
    """


def sql_insert_import(
    import_table: str, id: bytes, timestamp: int, tags: Dict[str, str],
) -> Tuple[str, Iterable[Any]]:
    import_table_literal = db.name_literal(import_table)
    return (
        f"""
INSERT INTO {import_table_literal} (`id`,`timestamp`, `tags`)
    VALUES (?, ?, ?)
;
    """,
        (id, timestamp, serialized_tags(tags)),
    )


def sql_delete_file_info(file_info_table: str, id: bytes) -> Tuple[str, Iterable[Any]]:
    file_info_table_literal = db.name_literal(file_info_table)
    return (
        f"""
DELETE FROM {file_info_table_literal} WHERE `import_id` = ? 
    """,
        (id,),
    )


def sql_insert_file_info(
    file_info_table: str, id: bytes, files: Iterable[FileInfo]
) -> Tuple[str, Iterable[Iterable[Any]]]:
    file_info_table_literal = db.name_literal(file_info_table)
    return (
        f"""
INSERT INTO {file_info_table_literal} 
    ( `digest`
    , `file_name`
    , `created`
    , `modified`
    , `size`
    , `archived`
    , `tags`
    , `import_id`
    )  VALUES
    ( ?, ?, ?, ?, ?, ?, ?, ? )
;
    """,
        (
            (
                f.digest,
                f.file_name,
                f.created,
                f.modified,
                f.size,
                1 if f.archived else 0,
                serialized_tags(f.metadata),
                id,
            )
            for f in files
        ),
    )


def sql_select_imported_file_infos(
    file_info_table: str, import_table: str, id: bytes
) -> Tuple[str, Iterable[Any]]:
    import_table_literal = db.name_literal(import_table)
    file_info_table_literal = db.name_literal(file_info_table)
    return (
        f"""
SELECT a.*
     , b.`timestamp` AS `import_timestamp`
     , b.`tags` as `import_tags`
FROM {file_info_table_literal} AS a 
    INNER JOIN {import_table_literal} AS b
    ON a.`import_id` = b.`id`
WHERE b.`id` = ?
;
    """,
        (id,),
    )


def serialized_tags(tags: Dict[str, str]) -> str:
    """ 
    Note: colons used as delimiters because they are not allowed in file paths, 
    which is where these tags are ultimately sourced from
    """
    if len(tags) == 0:
        return ""
    return (
        "::" + "::".join([f"{k.lower().strip()}:{v}" for (k, v) in tags.items()]) + "::"
    )


def deserialized_tags(tag_string: str) -> Dict[str, str]:
    def _deserialize_tag(tag: str) -> Tuple[str, str]:
        parts = tag.split(":")
        if len(parts) != 2:
            raise ValueError(f"Bad tag format: {tag}")
        return (parts[0], parts[1])

    tags = tag_string.split("::")
    return dict([_deserialize_tag(tag) for tag in tags if len(tag) > 0])
