from dataclasses import dataclass
from enum import Enum
from typing import Optional, List

from model.file_info import Digest


class FileType(Enum):
    Extract = "Extract"
    QC = "QC"
    Interim = "Interim"
    Final = "Final"


class DataType(Enum):
    ECG = "ECG"


@dataclass
class StudyFile:
    file_type: FileType
    data_type: DataType
    client: Optional[str]
    protocol: Optional[str]
    account: Optional[str]
    file_digest: Digest
    file_name: str
    file_created: float
    file_modified: float
    file_size: int
    file_archived: bool


@dataclass
class SearchSpec:
    file_type: FileType
    data_type: DataType
    root_dir: str
    match_paths: List[str]
