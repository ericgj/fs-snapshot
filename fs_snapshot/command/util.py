from ..adapter.store import Store
from ..adapter import db
from ..adapter import logging
from ..model.config import Config


def connect_store_db(config: Config):
    logger = logging.get_db_logger(config.store_db_log_name)
    conn = db.connect(config.store_db_file, logger)
    store_db = Store(
        import_table=config.store_db_import_table,
        file_info_table=config.store_db_file_info_table,
        logger=logger,
    )
    return (conn, store_db)


def connect(config: Config):
    logger = logging.get_db_logger(config.store_db_log_name)
    return db.connect(config.store_db_file, logger)
