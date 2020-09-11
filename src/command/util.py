import logging

from adapter.db.monitor import Monitor
from adapter import db
from model.config import Config


def connect_monitor_db(config: Config):
    conn = db.connect(config.monitor_db_file, config.monitor_db_log_file)
    monitor_db = Monitor(
        import_table=config.monitor_db_import_table,
        file_info_table=config.monitor_db_file_info_table,
        logger=logging.getLogger(config.monitor_db_log_name),
    )
    return (conn, monitor_db)
