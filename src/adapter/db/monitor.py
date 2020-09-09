import sqlite3
from time import time
from uuid import uuid4
from typing import Optional, Iterable, Any, Tuple

from adapter import db
from model.study_file import StudyFile


class Monitor:
    def __init__(self, *, import_table: str, study_file_table: str):
        self.import_table = import_table
        self.study_file_table = study_file_table

    def get_import_id(self):
        return uuid4().bytes

    def import_study_files(
        self,
        conn: sqlite3.Connection,
        files: Iterable[StudyFile],
        id: Optional[bytes] = None,
    ) -> bytes:
        if id is None:
            id = uuid4().bytes
        ts = time()
        with conn:
            self.init_import(conn, id, ts)
            self.delete_file_info(conn, id)
            self.insert_file_info(conn, id, files)
        return id

    def init_tables(self, conn: sqlite3.Connection):
        self.init_import_table(conn)
        self.init_study_file_table(conn)

    def init_import_table(self, conn: sqlite3.Connection):
        sql = sql_script_init_import_table(self.import_table)
        db.execute_script(conn, sql)

    def init_study_file_table(self, conn: sqlite3.Connection):
        sql = sql_script_init_study_file_table(
            study_file_table=self.study_file_table, import_table=self.import_table,
        )
        db.execute_script(conn, sql)

    def init_import(self, conn: sqlite3.Connection, id: bytes, timestamp: float):
        sql, params = sql_insert_import(self.import_table, id, int(timestamp))
        db.execute(conn, sql, params)

    def delete_file_info(self, conn: sqlite3.Connection, id: bytes):
        sql, params = sql_delete_file_info(self.study_file_table, id)
        db.execute(conn, sql, params)

    def insert_file_info(
        self, conn: sqlite3.Connection, id: bytes, files: Iterable[StudyFile]
    ):
        sql, params = sql_insert_file_info(self.study_file_table, id, files)
        db.execute_many(conn, sql, params)


def sql_script_init_import_table(import_table: str) -> str:
    import_table_literal = db.name_literal(import_table)
    index_literal = db.name_literal(f"{import_table}_timestamp")
    return f"""
CREATE TABLE IF NOT EXISTS {import_table_literal}
    ( `id` BYTES PRIMARY KEY
    , `timestamp` INT
    )
;
CREATE INDEX IF NOT EXISTS {index_literal} ON {import_table_literal} (`timestamp`);
    """


def sql_script_init_study_file_table(
    *, study_file_table: str, import_table: str
) -> str:
    study_file_table_literal = db.name_literal(study_file_table)
    import_table_literal = db.name_literal(import_table)
    file_type_index_literal = db.name_literal(f"{study_file_table}_file_type")
    digest_index_literal = db.name_literal(f"{study_file_table}_digest")
    file_name_index_literal = db.name_literal(f"{study_file_table}_file_name")
    data_type_index_literal = db.name_literal(f"{study_file_table}_data_type")
    client_index_literal = db.name_literal(f"{study_file_table}_client")
    protocol_index_literal = db.name_literal(f"{study_file_table}_protocol")
    account_index_literal = db.name_literal(f"{study_file_table}_account")
    return f"""
CREATE TABLE IF NOT EXISTS {study_file_table_literal}
    ( `file_type` TEXT NOT NULL
    , `digest` BYTES NOT NULL
    , `file_name` TEXT NOT NULL
    , `created` INT NOT NULL
    , `modified` INT NOT NULL
    , `size` INT NOT NULL
    , `archived` TINYINT NOT NULL
    , `data_type` TEXT NOT NULL
    , `client` TEXT NULL
    , `protocol` TEXT NULL
    , `account` TEXT NULL
    , `import_id` BYTES NOT NULL
    , FOREIGN KEY(`import_id`) REFERENCES {import_table_literal}(`id`)
    )
;
CREATE INDEX IF NOT EXISTS {file_type_index_literal} ON {study_file_table_literal} (`file_type`);
CREATE INDEX IF NOT EXISTS {digest_index_literal} ON {study_file_table_literal} (`digest`);
CREATE INDEX IF NOT EXISTS {file_name_index_literal} ON {study_file_table_literal} (`file_name`);
CREATE INDEX IF NOT EXISTS {data_type_index_literal} ON {study_file_table_literal} (`data_type`);
CREATE INDEX IF NOT EXISTS {client_index_literal} ON {study_file_table_literal} (`client`);
CREATE INDEX IF NOT EXISTS {protocol_index_literal} ON {study_file_table_literal} (`protocol`);
CREATE INDEX IF NOT EXISTS {account_index_literal} ON {study_file_table_literal} (`account`);
    """


def sql_insert_import(
    import_table: str, id: bytes, timestamp: int
) -> Tuple[str, Iterable[Any]]:
    import_table_literal = db.name_literal(import_table)
    return (
        f"""
INSERT INTO {import_table_literal} (`id`,`timestamp`)
    VALUES (?, ?)
;
    """,
        (id, timestamp),
    )


def sql_delete_file_info(study_file_table: str, id: bytes) -> Tuple[str, Iterable[Any]]:
    study_file_table_literal = db.name_literal(study_file_table)
    return (
        f"""
DELETE FROM {study_file_table_literal} WHERE `import_id` = ? 
    """,
        (id,),
    )


def sql_insert_file_info(
    study_file_table: str, id: bytes, files: Iterable[StudyFile]
) -> Tuple[str, Iterable[Iterable[Any]]]:
    study_file_table_literal = db.name_literal(study_file_table)
    return (
        f"""
INSERT INTO {study_file_table_literal} 
    ( `file_type`
    , `digest`
    , `file_name`
    , `created`
    , `modified`
    , `size`
    , `archived`
    , `data_type`
    , `client`
    , `protocol`
    , `account`
    , `import_id`
    )  VALUES
    ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
;
    """,
        (
            (
                f.file_type.name,
                f.file.digest,
                f.file.file_name,
                f.file.created,
                f.file.modified,
                f.file.size,
                1 if f.file.archived else 0,
                f.data_type,
                f.client,
                f.protocol,
                f.account,
                id,
            )
            for f in files
        ),
    )
