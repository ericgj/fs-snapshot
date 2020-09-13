import json
import sys
from typing import Iterable, List, TextIO

from .util import connect_store_db
from ..adapter import logging
from ..model.config import Config
from ..model.file_info import Action, diff_all

LOGGER = logging.get_logger(__name__)


def main(
    config: Config, import_id: bytes,
):
    conn, store_db = connect_store_db(config)

    LOGGER.info(f"Start: from {import_id.hex()}")
    latest_id, compares = store_db.fetch_file_import_compare_latest(conn, import_id)

    actions: List[Action] = list(diff_all(compares))

    LOGGER.info(
        f"""End: from {import_id.hex()} to {latest_id.hex()} 
  {len(compares)} compared files
  {len(actions)} resulting actions"""
    )

    write_json(
        original_id=import_id, new_id=latest_id, actions=actions, target=sys.stdout
    )


def write_json(
    *, original_id: bytes, new_id: bytes, actions: Iterable[Action], target: TextIO
):
    json.dump(
        {
            "original_id": original_id.hex(),
            "new_id": new_id.hex(),
            "actions": [action.to_json() for action in actions],
        },
        fp=target,
        indent=4,
    )
