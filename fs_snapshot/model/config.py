from dataclasses import dataclass
import os.path
from typing import List, Dict


@dataclass
class Config:
    store_db_root_dir: str
    store_db_base_name: str
    store_db_import_table: str
    store_db_file_info_table: str

    @property
    def store_db_file(self):
        return os.path.join(self.store_db_root_dir, self.store_db_base_name)

    @property
    def store_db_log_file(self):
        return self.store_db_file + ".log"

    @property
    def store_db_log_name(self):
        return os.path.splitext(self.store_db_log_file)[0]


@dataclass
class SearchSpec:
    root_dir: str
    match_paths: List[str]
    metadata: Dict[str, str]
