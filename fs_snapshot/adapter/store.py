from dataclasses import dataclass
from logging import Logger
import sqlite3
from time import time
from uuid import uuid4
from typing import Optional, Iterable, Any, Tuple, Dict, List

from . import db
from ..model import file_info


class StoreFileImportNotFound(Exception):
    pass


class StoreFileImportCompareNoChanges(Exception):
    pass


""" 
Note: forward-slashes used as delimiters because they are not allowed in file
paths in either *nix or Windows - which is where these tags are ultimately
sourced from. 
"""
TAG_DELIMITER = "/"
TAG_VALUE_DELIMITER = ":"


@dataclass
class FileImport:
    id: bytes
    timestamp: float
    name: str
    tags: Dict[str, str]
    files: List[file_info.FileInfo]


class Store:
    def __init__(self, *, import_table: str, file_info_table: str, logger: Logger):
        self.import_table = import_table
        self.file_info_table = file_info_table
        self.logger = logger

    @db.log_errors
    def create_import(
        self, conn: sqlite3.Connection, name: str, tags: Dict[str, str] = {}
    ) -> bytes:
        id = self.get_import_id()
        with conn:
            self.init_tables(conn)
            ts = time()
            self.init_import(conn, id, ts, name, tags)
            self.delete_file_infos(conn, id)
        return id

    @db.log_errors
    def import_files(
        self, conn: sqlite3.Connection, id: bytes, files: Iterable[file_info.FileInfo]
    ):
        with conn:
            self.insert_file_infos(conn, id, files)

    @db.log_errors
    def fetch_file_import_compare_latest(
        self, conn: sqlite3.Connection, id: bytes, compare_digests: bool = False,
    ) -> Tuple[bytes, List[file_info.CompareStates]]:
        latest_id = self.fetch_latest_import_id(conn, id)
        if latest_id is None:
            raise StoreFileImportNotFound(id)  # should not reach
        if latest_id == id:
            raise StoreFileImportCompareNoChanges(id)

        rows = self.select_file_import_compare(
            conn, prev_id=id, next_id=latest_id, compare_digests=compare_digests
        )
        return (latest_id, [deserialized_file_info_compare(row) for row in rows])

    def fetch_file_import(self, conn: sqlite3.Connection, id: bytes) -> FileImport:
        rows = self.select_imported_file_infos(conn, id)
        file_import = deserialized_file_import(rows)
        if file_import is None:
            raise StoreFileImportNotFound(id)
        return file_import

    def fetch_import(self, conn: sqlite3.Connection, id: bytes) -> FileImport:
        sql, params = sql_fetch_import(self.import_table, id)
        try:
            row = db.select_one(conn, sql, params)
        except IndexError:
            raise StoreFileImportNotFound(id)
        file_import = deserialized_import(row)
        return file_import

    def fetch_latest_import_id(
        self, conn: sqlite3.Connection, id: bytes
    ) -> Optional[bytes]:
        file_import = self.fetch_import(conn, id)
        return self.fetch_latest_import_id_for_name(conn, file_import.name)

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
        name: str,
        tags: Dict[str, str] = {},
    ):
        sql, params = sql_insert_import(
            self.import_table, id, int(timestamp), name, tags
        )
        db.execute(conn, sql, params)

    def delete_file_infos(self, conn: sqlite3.Connection, id: bytes):
        sql, params = sql_delete_file_info(self.file_info_table, id)
        db.execute(conn, sql, params)

    def insert_file_infos(
        self, conn: sqlite3.Connection, id: bytes, files: Iterable[file_info.FileInfo]
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

    def fetch_latest_import_id_for_name(
        self, conn: sqlite3.Connection, name: str
    ) -> Optional[bytes]:
        sql, params = sql_select_latest_import_for_name(self.import_table, name)
        try:
            row = db.select_one(conn, sql, params)
            return bytes(row["id"])
        except IndexError:
            return None

    def select_file_import_compare(
        self,
        conn: sqlite3.Connection,
        *,
        prev_id: bytes,
        next_id: bytes,
        compare_digests: bool,
    ) -> List[sqlite3.Row]:
        sql, params = sql_select_file_import_compare(
            self.file_info_table,
            prev_id=prev_id,
            next_id=next_id,
            compare_digests=compare_digests,
        )
        return db.select(conn, sql, params)


# ------------------------------------------------------------------------------
# SQL
# ------------------------------------------------------------------------------


def sql_script_init_import_table(import_table: str) -> str:
    import_table_literal = db.name_literal(import_table)
    timestamp_index_literal = db.name_literal(f"{import_table}_timestamp")
    name_index_literal = db.name_literal(f"{import_table}_name")
    return f"""
CREATE TABLE IF NOT EXISTS {import_table_literal}
    ( `id` BYTES PRIMARY KEY
    , `timestamp` INT
    , `name` TEXT
    , `tags` TEXT
    )
;
CREATE INDEX IF NOT EXISTS {timestamp_index_literal} ON {import_table_literal} (`timestamp`);
CREATE INDEX IF NOT EXISTS {name_index_literal} ON {import_table_literal} (`name`);
    """


def sql_script_init_file_info_table(*, file_info_table: str, import_table: str) -> str:
    file_info_table_literal = db.name_literal(file_info_table)
    import_table_literal = db.name_literal(import_table)
    digest_index_literal = db.name_literal(f"{file_info_table}_digest")
    dir_name_index_literal = db.name_literal(f"{file_info_table}_dir_name")
    base_name_index_literal = db.name_literal(f"{file_info_table}_base_name")
    file_group_index_literal = db.name_literal(f"{file_info_table}_file_group")
    file_type_index_literal = db.name_literal(f"{file_info_table}_file_type")

    return f"""
CREATE TABLE IF NOT EXISTS {file_info_table_literal}
    ( `digest` BYTES NOT NULL
    , `dir_name` TEXT NOT NULL
    , `base_name` TEXT NOT NULL
    , `created` INT NOT NULL
    , `modified` INT NOT NULL
    , `size` INT NOT NULL
    , `archived` TINYINT NOT NULL
    , `file_group` TEXT NULL
    , `file_type` TEXT NULL
    , `tags` TEXT NULL
    , `import_id` BYTES NOT NULL
    , FOREIGN KEY(`import_id`) REFERENCES {import_table_literal}(`id`)
    )
;
CREATE INDEX IF NOT EXISTS {digest_index_literal} ON {file_info_table_literal} (`digest`);
CREATE INDEX IF NOT EXISTS {dir_name_index_literal} ON {file_info_table_literal} (`dir_name`);
CREATE INDEX IF NOT EXISTS {base_name_index_literal} ON {file_info_table_literal} (`base_name`);
CREATE INDEX IF NOT EXISTS {file_group_index_literal} ON {file_info_table_literal} (`file_group`);
CREATE INDEX IF NOT EXISTS {file_type_index_literal} ON {file_info_table_literal} (`file_type`);
    """


def sql_insert_import(
    import_table: str, id: bytes, timestamp: int, name: str, tags: Dict[str, str],
) -> Tuple[str, Iterable[Any]]:
    import_table_literal = db.name_literal(import_table)
    return (
        f"""
INSERT INTO {import_table_literal} (`id`,`timestamp`, `name`, `tags`)
    VALUES (?, ?, ?, ?)
;
    """,
        (id, timestamp, name, serialized_tags(tags)),
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
    file_info_table: str, id: bytes, files: Iterable[file_info.FileInfo]
) -> Tuple[str, Iterable[Iterable[Any]]]:
    file_info_table_literal = db.name_literal(file_info_table)
    return (
        f"""
INSERT INTO {file_info_table_literal} 
    ( `digest`
    , `dir_name`
    , `base_name`
    , `created`
    , `modified`
    , `size`
    , `archived`
    , `file_group`
    , `file_type`
    , `tags`
    , `import_id`
    )  VALUES
    ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
;
    """,
        (
            (
                f.digest,
                f.dir_name,
                f.base_name,
                f.created,
                f.modified,
                f.size,
                1 if f.archived else 0,
                f.file_group,
                f.file_type,
                serialized_tags(f.metadata),
                id,
            )
            for f in files
        ),
    )


def sql_fetch_import(import_table: str, id: bytes) -> Tuple[str, Iterable[Any]]:
    import_table_literal = db.name_literal(import_table)
    return (
        f"""
SELECT `id`, `timestamp`, `name`, `tags`
FROM {import_table_literal}
WHERE `id` = ?
    """,
        (id,),
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
     , b.`name` AS `import_name`
     , b.`tags` as `import_tags`
FROM {file_info_table_literal} AS a 
    INNER JOIN {import_table_literal} AS b
    ON a.`import_id` = b.`id`
WHERE b.`id` = ?
;
    """,
        (id,),
    )


def sql_select_latest_import_for_name(
    import_table: str, name: str
) -> Tuple[str, Iterable[Any]]:
    import_table_literal = db.name_literal(import_table)
    return (
        f"""
SELECT `id`, `timestamp`, `name`, `tags`
FROM {import_table_literal}
WHERE `name` = ?
ORDER BY `timestamp` DESC
LIMIT 1
;
    """,
        (name,),
    )


def sql_select_latest_import_for_tags(
    import_table: str, tags: Dict[str, str]
) -> Tuple[str, Iterable[Any]]:
    import_table_literal = db.name_literal(import_table)
    where_literal = " AND ".join(["tags LIKE ?" for _ in range(len(tags))])
    if len(where_literal) == 0:
        where_literal = "`tags` IS NULL OR LENGTH(`tags`) = 0"
    return (
        f"""
SELECT `id`, `timestamp`, `name`, `tags`
FROM {import_table_literal}
WHERE {where_literal}
ORDER BY `timestamp` DESC
LIMIT 1
;
    """,
        [
            f"%{TAG_DELIMITER}{serialized_tag(k,v)}{TAG_DELIMITER}%"
            for (k, v) in tags.items()
        ],
    )


def sql_select_file_import_compare(
    file_info_table: str, *, prev_id: bytes, next_id: bytes, compare_digests: bool,
):
    file_info_table_literal = db.name_literal(file_info_table)

    contents_match_expr = (
        "prev.`digest` = next.`digest`"
        if compare_digests
        else "prev.`size` = next.`size` AND prev.`modified` = next.`modified`"
    )
    contents_full_match_expr = (
        "prev.`digest` = full_matches.`digest`"
        if compare_digests
        else "prev.`size` = full_matches.`size` AND prev.`modified` = full_matches.`modified`"
    )
    contents_full_match_not_in_expr = (
        "full_matches.`digest` IS NULL"
        if compare_digests
        else "full_matches.`size` IS NULL AND full_matches.`modified` IS NULL"
    )

    return (
        f"""
WITH `full_matches` (`digest`, `size`, `modified`) AS (
    SELECT prev.`digest`, prev.`size`, prev.`modified`
    FROM {file_info_table_literal} AS prev
        INNER JOIN {file_info_table_literal} AS next
            ON ({contents_match_expr}) 
            AND (prev.`dir_name` = next.`dir_name` AND prev.`base_name` = next.`base_name`)
    WHERE prev.`import_id` = ?
    AND next.`import_id` = ?
)
    /* IN PREV ONLY */
SELECT prev.`digest` AS `digest_prev`
     , prev.`dir_name` AS `dir_name_prev`
     , prev.`base_name` AS `base_name_prev`
     , prev.`created` AS `created_prev`
     , prev.`modified` AS `modified_prev`
     , prev.`size` AS `size_prev`
     , prev.`archived` AS `archived_prev`
     , prev.`file_group` AS `file_group_prev`
     , prev.`file_type` AS `file_type_prev`
     , prev.`tags` AS `tags_prev`
     , prev.`import_id` AS `import_id_prev`
     , next.`digest` AS `digest_next`
     , next.`dir_name` AS `dir_name_next`
     , next.`base_name` AS `base_name_next`
     , next.`created` AS `created_next`
     , next.`modified` AS `modified_next`
     , next.`size` AS `size_next`
     , next.`archived` AS `archived_next`
     , next.`file_group` AS `file_group_next`
     , next.`file_type` AS `file_type_next`
     , next.`tags` AS `tags_next`
     , next.`import_id` AS `import_id_next`
     , 0 AS __copied__
FROM {file_info_table_literal} AS prev 
    LEFT JOIN {file_info_table_literal} AS next
        ON ({contents_match_expr}) 
        OR (prev.`dir_name` = next.`dir_name` AND prev.`base_name` = next.`base_name`)
WHERE next.`dir_name` IS NULL
  AND prev.`import_id` = ? 
  AND next.`import_id` = ?

UNION
    /* IN BOTH, RENAMED */
SELECT prev.`digest` AS `digest_prev`
     , prev.`dir_name` AS `dir_name_prev`
     , prev.`base_name` AS `base_name_prev`
     , prev.`created` AS `created_prev`
     , prev.`modified` AS `modified_prev`
     , prev.`size` AS `size_prev`
     , prev.`archived` AS `archived_prev`
     , prev.`file_group` AS `file_group_prev`
     , prev.`file_type` AS `file_type_prev`
     , prev.`tags` AS `tags_prev`
     , prev.`import_id` AS `import_id_prev`
     , next.`digest` AS `digest_next`
     , next.`dir_name` AS `dir_name_next`
     , next.`base_name` AS `base_name_next`
     , next.`created` AS `created_next`
     , next.`modified` AS `modified_next`
     , next.`size` AS `size_next`
     , next.`archived` AS `archived_next`
     , next.`file_group` AS `file_group_next`
     , next.`file_type` AS `file_type_next`
     , next.`tags` AS `tags_next`
     , next.`import_id` AS `import_id_next`
     , 0 AS __copied__
FROM {file_info_table_literal} AS prev 
    INNER JOIN {file_info_table_literal} AS next
        ON ({contents_match_expr}) 
        AND NOT (prev.`dir_name` = next.`dir_name` AND prev.`base_name` = next.`base_name`)
    LEFT OUTER JOIN `full_matches` 
        ON {contents_full_match_expr}
WHERE prev.`import_id` = ? 
  AND next.`import_id` = ?
  AND ({contents_full_match_not_in_expr})

UNION
    /* IN BOTH, COPIED */
SELECT prev.`digest` AS `digest_prev`
     , prev.`dir_name` AS `dir_name_prev`
     , prev.`base_name` AS `base_name_prev`
     , prev.`created` AS `created_prev`
     , prev.`modified` AS `modified_prev`
     , prev.`size` AS `size_prev`
     , prev.`archived` AS `archived_prev`
     , prev.`file_group` AS `file_group_prev`
     , prev.`file_type` AS `file_type_prev`
     , prev.`tags` AS `tags_prev`
     , prev.`import_id` AS `import_id_prev`
     , next.`digest` AS `digest_next`
     , next.`dir_name` AS `dir_name_next`
     , next.`base_name` AS `base_name_next`
     , next.`created` AS `created_next`
     , next.`modified` AS `modified_next`
     , next.`size` AS `size_next`
     , next.`archived` AS `archived_next`
     , next.`file_group` AS `file_group_next`
     , next.`file_type` AS `file_type_next`
     , next.`tags` AS `tags_next`
     , next.`import_id` AS `import_id_next`
     , 1 AS __copied__
FROM {file_info_table_literal} AS prev 
    INNER JOIN {file_info_table_literal} AS next
        ON ({contents_match_expr}) 
        AND NOT (prev.`dir_name` = next.`dir_name` AND prev.`base_name` = next.`base_name`)
    INNER JOIN `full_matches` 
        ON {contents_full_match_expr}
WHERE prev.`import_id` = ? 
  AND next.`import_id` = ?

UNION
    /* IN BOTH, MODIFIED */
SELECT prev.`digest` AS `digest_prev`
     , prev.`dir_name` AS `dir_name_prev`
     , prev.`base_name` AS `base_name_prev`
     , prev.`created` AS `created_prev`
     , prev.`modified` AS `modified_prev`
     , prev.`size` AS `size_prev`
     , prev.`archived` AS `archived_prev`
     , prev.`file_group` AS `file_group_prev`
     , prev.`file_type` AS `file_type_prev`
     , prev.`tags` AS `tags_prev`
     , prev.`import_id` AS `import_id_prev`
     , next.`digest` AS `digest_next`
     , next.`dir_name` AS `dir_name_next`
     , next.`base_name` AS `base_name_next`
     , next.`created` AS `created_next`
     , next.`modified` AS `modified_next`
     , next.`size` AS `size_next`
     , next.`archived` AS `archived_next`
     , next.`file_group` AS `file_group_next`
     , next.`file_type` AS `file_type_next`
     , next.`tags` AS `tags_next`
     , next.`import_id` AS `import_id_next`
     , 0 AS __copied__
FROM {file_info_table_literal} AS prev 
    INNER JOIN {file_info_table_literal} AS next
        ON (prev.`dir_name` = next.`dir_name` AND prev.`base_name` = next.`base_name`) 
        AND NOT ({contents_match_expr}) 
WHERE prev.`import_id` = ? 
  AND next.`import_id` = ?

UNION
    /* IN BOTH, NO CHANGE */
SELECT prev.`digest` AS `digest_prev`
     , prev.`dir_name` AS `dir_name_prev`
     , prev.`base_name` AS `base_name_prev`
     , prev.`created` AS `created_prev`
     , prev.`modified` AS `modified_prev`
     , prev.`size` AS `size_prev`
     , prev.`archived` AS `archived_prev`
     , prev.`file_group` AS `file_group_prev`
     , prev.`file_type` AS `file_type_prev`
     , prev.`tags` AS `tags_prev`
     , prev.`import_id` AS `import_id_prev`
     , next.`digest` AS `digest_next`
     , next.`dir_name` AS `dir_name_next`
     , next.`base_name` AS `base_name_next`
     , next.`created` AS `created_next`
     , next.`modified` AS `modified_next`
     , next.`size` AS `size_next`
     , next.`archived` AS `archived_next`
     , next.`file_group` AS `file_group_next`
     , next.`file_type` AS `file_type_next`
     , next.`tags` AS `tags_next`
     , next.`import_id` AS `import_id_next`
     , 0 AS __copied__
FROM {file_info_table_literal} AS prev 
    INNER JOIN {file_info_table_literal} AS next
        ON ({contents_match_expr}) 
        AND (prev.`dir_name` = next.`dir_name` AND prev.`base_name` = next.`base_name`)
WHERE prev.`import_id` = ? 
  AND next.`import_id` = ?

UNION
    /* IN NEXT ONLY */
SELECT prev.`digest` AS `digest_prev`
     , prev.`dir_name` AS `dir_name_prev`
     , prev.`base_name` AS `base_name_prev`
     , prev.`created` AS `created_prev`
     , prev.`modified` AS `modified_prev`
     , prev.`size` AS `size_prev`
     , prev.`archived` AS `archived_prev`
     , prev.`file_group` AS `file_group_prev`
     , prev.`file_type` AS `file_type_prev`
     , prev.`tags` AS `tags_prev`
     , prev.`import_id` AS `import_id_prev`
     , next.`digest` AS `digest_next`
     , next.`dir_name` AS `dir_name_next`
     , next.`base_name` AS `base_name_next`
     , next.`created` AS `created_next`
     , next.`modified` AS `modified_next`
     , next.`size` AS `size_next`
     , next.`archived` AS `archived_next`
     , next.`file_group` AS `file_group_next`
     , next.`file_type` AS `file_type_next`
     , next.`tags` AS `tags_next`
     , next.`import_id` AS `import_id_next`
     , 0 AS __copied__
FROM {file_info_table_literal} AS next 
    LEFT JOIN {file_info_table_literal} AS prev
        ON ({contents_match_expr}) 
        OR (next.`dir_name` = prev.`dir_name` AND next.`base_name` = prev.`base_name`)
WHERE prev.`dir_name` IS NULL
  AND prev.`import_id` = ?
  AND next.`import_id` = ?
;
        """,
        (
            prev_id,
            next_id,
            prev_id,
            next_id,
            prev_id,
            next_id,
            prev_id,
            next_id,
            prev_id,
            next_id,
            prev_id,
            next_id,
            prev_id,
            next_id,
        ),
    )


# ------------------------------------------------------------------------------
# Codecs
# ------------------------------------------------------------------------------


def deserialized_import(row: sqlite3.Row) -> FileImport:
    id = bytes(row["id"])
    ts = float(row["timestamp"])
    name = str(row["name"])
    tags = deserialized_tags(str(row["tags"]))
    return FileImport(id=id, timestamp=ts, name=name, tags=tags, files=[],)


def deserialized_file_import(rows: List[sqlite3.Row]) -> Optional[FileImport]:
    if len(rows) == 0:
        return None
    id = bytes(rows[0]["id"])
    ts = float(rows[0]["import_timestamp"])
    name = str(rows[0]["name"])
    tags = deserialized_tags(str(rows[0]["import_tags"]))
    return FileImport(
        id=id,
        timestamp=ts,
        name=name,
        tags=tags,
        files=[deserialized_file_info(row) for row in rows],
    )


def deserialized_file_info(
    row: sqlite3.Row, field_suffix: str = ""
) -> file_info.FileInfo:
    return file_info.FileInfo(
        digest=bytes(row["digest" + field_suffix]),
        dir_name=str(row["dir_name" + field_suffix]),
        base_name=str(row["base_name" + field_suffix]),
        created=float(row["created" + field_suffix]),
        modified=float(row["modified" + field_suffix]),
        size=int(row["size" + field_suffix]),
        archived=True if int(row["archived" + field_suffix]) == 1 else False,
        file_group=str(row["file_group" + field_suffix]),
        file_type=str(row["file_type" + field_suffix]),
        metadata=deserialized_tags(str(row["tags" + field_suffix])),
    )


def deserialized_file_info_compare(row: sqlite3.Row) -> file_info.CompareStates:
    if row["digest_prev"] is None and row["digest_next"] is not None:
        return file_info.NewOnly(new=deserialized_file_info(row, field_suffix="_next"))
    elif row["digest_prev"] is not None and row["digest_next"] is not None:
        is_copy = True if int(row["__copied__"]) == 1 else False
        return file_info.OriginalAndNew(
            original=deserialized_file_info(row, field_suffix="_prev"),
            new=deserialized_file_info(row, field_suffix="_next"),
            is_copy=is_copy,
        )
    elif row["digest_prev"] is not None and row["digest_next"] is None:
        return file_info.OriginalOnly(
            original=deserialized_file_info(row, field_suffix="_next"),
        )
    else:
        raise ValueError("Unrecognized query results")  # should not reach


def serialized_tags(tags: Dict[str, str]) -> str:
    if len(tags) == 0:
        return ""
    return (
        TAG_DELIMITER
        + TAG_DELIMITER.join([serialized_tag(k, tags[k]) for k in sorted(tags.keys())])
        + TAG_DELIMITER
    )


def serialized_tag(key: str, value: str) -> str:
    return f"{key.lower().strip()}{TAG_VALUE_DELIMITER}{value}"


def deserialized_tags(tag_string: str) -> Dict[str, str]:
    tags = tag_string.split(TAG_DELIMITER)
    return dict([deserialize_tag(tag) for tag in tags if len(tag) > 0])


def deserialize_tag(tag: str) -> Tuple[str, str]:
    parts = tag.split(TAG_VALUE_DELIMITER)
    if len(parts) != 2:
        raise ValueError(f"Bad tag format: {tag}")
    return (parts[0], parts[1])
