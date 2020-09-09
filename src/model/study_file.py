from dataclasses import dataclass, replace
from enum import Enum
from typing import Optional, List

from model.file_info import FileInfo


class FileType(Enum):
    Extract = "Extract"
    QC = "QC"
    Interim = "Interim"
    Final = "Final"


@dataclass
class StudyFile:
    file_type: FileType
    data_type: str
    client: Optional[str]
    protocol: Optional[str]
    account: Optional[str]
    file: FileInfo

    @classmethod
    def create_extract_file(cls, file_info: FileInfo) -> "StudyFile":
        return cls.create_file(FileType.Extract, file_info)

    @classmethod
    def create_qc_file(cls, file_info: FileInfo) -> "StudyFile":
        return cls.create_file(FileType.QC, file_info)

    @classmethod
    def create_interim_file(cls, file_info: FileInfo) -> "StudyFile":
        return cls.create_file(FileType.Interim, file_info)

    @classmethod
    def create_final_file(cls, file_info: FileInfo) -> "StudyFile":
        return cls.create_file(FileType.Final, file_info)

    @classmethod
    def create_file(cls, file_type: FileType, file_info: FileInfo) -> "StudyFile":
        return cls(
            file_type=file_type,
            data_type=file_info.metadata["data_type"],
            client=file_info.metadata.get("client", None),
            protocol=file_info.metadata.get("protocol", None),
            account=file_info.metadata.get("account", None),
            file=file_info,
        )

    def update(self, file_info: FileInfo) -> "StudyFile":
        return replace(
            self,
            data_type=file_info.metadata["data_type"],
            client=file_info.metadata.get("client", None),
            protocol=file_info.metadata.get("protocol", None),
            account=file_info.metadata.get("account", None),
            file=file_info,
        )


@dataclass
class SearchSpec:
    file_type: FileType
    root_dir: str
    match_paths: List[str]
