from dataclasses import dataclass, field
import os.path
from typing import Optional, Union, Mapping, Sequence, Dict, Set


class NotArchived:
    def __str__(self):
        return "NotArchived()"


@dataclass
class ArchivedByMetadata:
    key: str
    values: Set[str]

    def __str__(self):
        return f"ArchivedByMetadata({self.key}, {self.values})"


ArchivedBy = Union[NotArchived, ArchivedByMetadata]


class NoCalc:
    def __str__(self):
        return "NoCalc()"


@dataclass
class CalcByMetadata:
    format: str

    def __str__(self):
        return f"CalcByMetadata({self.format})"


CalcBy = Union[NoCalc, CalcByMetadata]


@dataclass
class Config:
    name: str
    match_paths: Mapping[str, Sequence[str]]
    root_dir: str = "."
    log_file: Optional[str] = None
    store_db_file: str = "fs-snapshot.sqlite"
    store_db_import_table: str = "__import__"
    store_db_file_info_table: str = "file_info"
    multithread: bool = False  # NOTE: not set from config file currently
    compare_digests: bool = False
    metadata: Dict[str, str] = field(default_factory=dict)
    archived_by: ArchivedBy = NotArchived()
    file_group_by: CalcBy = NoCalc()
    minimum_size: int = 0

    @property
    def store_db_log_file(self) -> str:
        return self.store_db_file + ".log"

    @property
    def store_db_log_name(self) -> str:
        return os.path.splitext(self.store_db_log_file)[0]

    @property
    def log_name(self) -> Optional[str]:
        return None if self.log_file is None else os.path.splitext(self.log_file)[0]

    def is_archived(self, metadata: Dict[str, str]) -> bool:
        if isinstance(self.archived_by, NotArchived):
            return False

        if isinstance(self.archived_by, ArchivedByMetadata):
            return metadata.get(self.archived_by.key, None) in self.archived_by.values

        raise ValueError(f"Unknown ArchivedBy type: {type(self.archived_by)}")

    def file_group_from(self, metadata: Dict[str, str]) -> Optional[str]:
        if isinstance(self.file_group_by, NoCalc):
            return None

        if isinstance(self.file_group_by, CalcByMetadata):
            try:
                return self.file_group_by.format.format(**metadata)
            except KeyError:
                return None

        raise ValueError(f"Unknown CalcBy type: {type(self.file_group_by)}")
