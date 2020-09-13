from dataclasses import dataclass, field
import os.path
from typing import Optional, Union, List, Dict, Set


class NotArchived:
    pass


@dataclass
class ArchivedByMetadata:
    key: str
    values: Set[str]


ArchivedBy = Union[NotArchived, ArchivedByMetadata]


@dataclass
class Config:
    match_paths: List[str]
    root_dir: str = "."
    log_file: Optional[str] = None
    store_db_file: str = "fs-snapshot.sqlite"
    store_db_import_table: str = "__import__"
    store_db_file_info_table: str = "file_info"
    metadata: Dict[str, str] = field(default_factory=dict)
    archived_by: ArchivedBy = NotArchived()

    @property
    def store_db_log_file(self) -> str:
        return self.store_db_file + ".log"

    @property
    def store_db_log_name(self) -> str:
        return os.path.splitext(self.store_db_log_file)[0]

    @property
    def log_name(self) -> Optional[str]:
        return None if self.log_file is None else os.path.splitext(self.log_file)[0]

    def is_archived(self, metadata):
        if isinstance(self.archived_by, NotArchived):
            return False

        if isinstance(self.archived_by, ArchivedByMetadata):
            return metadata.get(self.archived_by.key, None) in self.archived_by.values

        return False
