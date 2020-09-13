import logging

from ..adapter.store import Store
from ..adapter import db
from ..model.config import Config


def connect_store_db(config: Config):
    conn = db.connect(config.store_db_file, config.store_db_log_file)
    store_db = Store(
        import_table=config.store_db_import_table,
        file_info_table=config.store_db_file_info_table,
        logger=logging.getLogger(config.store_db_log_name),
    )
    return (conn, store_db)
