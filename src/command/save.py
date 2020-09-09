from threading import Thread

from adapter.db.monitor import Monitor
from adapter import db
from adapter import filesys
from model.config import Config
from model.ert.study_file import SearchSpec

KEY_FILE_TYPE = "file_type"
KEY_DATA_TYPE = "data_type"


def main(
    config: Config, spec: SearchSpec,
):
    conn, monitor_db = connection_and_monitor_db(config)
    id = monitor_db.create_import(
        conn, {KEY_FILE_TYPE: spec.file_type.name, KEY_DATA_TYPE: spec.data_type.name}
    )

    threads = [
        Thread(target=save, args=(config, id, spec.root_dir, mp))
        for mp in spec.match_paths
    ]
    for th in threads:
        th.start()

    for th in threads:
        th.join()


def save(config: Config, id: bytes, root_dir: str, match_path: str):
    conn, monitor_db = connection_and_monitor_db(config)
    monitor_db.import_files(
        conn, id, filesys.search(root_dir, match_path, is_archived=has_archive_path),
    )


def connection_and_monitor_db(config: Config):
    conn = db.connect(config.monitor_db_file, config.monitor_db_log_file)
    monitor_db = Monitor(
        import_table=config.monitor_db_import_table,
        file_info_table=config.monitor_db_file_info_table,
    )
    return (conn, monitor_db)


def has_archive_path(metadata) -> bool:
    return "archive" in metadata
