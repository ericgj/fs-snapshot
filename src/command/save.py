from threading import Thread

from adapter.db.monitor import Monitor
from adapter import db
from adapter import filesys
from model.config import Config
from model.study_file import SearchSpec, StudyFile, FileType


def main(
    config: Config, spec: SearchSpec,
):
    conn, monitor_db = connection_and_monitor_db(config)
    monitor_db.init_tables(conn)
    id = monitor_db.get_import_id()

    threads = [
        Thread(target=save, args=(config, id, spec.file_type, spec.root_dir, mp))
        for mp in spec.match_paths
    ]
    for th in threads:
        th.start()

    for th in threads:
        th.join()


def save(
    config: Config, id: bytes, file_type: FileType, root_dir: str, match_path: str
):
    conn, monitor_db = connection_and_monitor_db(config)
    monitor_db.import_study_files(
        conn,
        (
            StudyFile.create_file(file_type, info)
            for info in filesys.search(root_dir, match_path)
        ),
        id=id,
    )


def connection_and_monitor_db(config: Config):
    conn = db.connect(config.monitor_db_file)
    monitor_db = Monitor(
        import_table=config.monitor_db_import_table,
        study_file_table=config.monitor_db_study_file_table,
    )
    return (conn, monitor_db)
