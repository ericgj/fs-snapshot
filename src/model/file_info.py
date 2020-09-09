from dataclasses import dataclass, replace
import os.path
from typing import Union, Optional, Iterable, Generator, Tuple, Dict

Digest = bytes


@dataclass
class FileInfo:
    digest: Digest
    file_name: str
    created: float
    modified: float
    size: int
    archived: bool
    metadata: Dict[str, str]

    @property
    def hexdigest(self):
        return self.digest.hex()

    @property
    def dir_name(self):
        return os.path.dirname(self.file_name)

    @property
    def base_name(self):
        return os.path.basename(self.file_name)


@dataclass
class Moved:
    dir_name: str
    metadata: Dict[str, str]


@dataclass
class Renamed:
    base_name: str
    metadata: Dict[str, str]


@dataclass
class Archived:
    dir_name: str
    metadata: Dict[str, str]


@dataclass
class Modified:
    modified: float
    digest: Digest


class NoChange:
    pass


Action = Union[Moved, Renamed, Archived, Modified, NoChange]


def diff_all(
    pairs: Iterable[Tuple[FileInfo, FileInfo]]
) -> Generator[Action, None, None]:
    for (a, b) in pairs:
        action = diff(a, b)
        if action is not None:
            yield action
    return


def diff(a: FileInfo, b: FileInfo) -> Optional[Action]:
    table = (a.digest == b.digest, a.file_name == b.file_name)
    if table == (False, False):
        return None  # should not reach in practice

    if table == (True, True):
        return NoChange()

    if table == (True, False):
        a_dir, a_name = a.dir_name, a.base_name
        b_dir, b_name = b.dir_name, b.base_name
        subtable = (a_dir == b_dir, a_name == b_name)

        if subtable == (True, True):
            return NoChange()  # should never reach

        if subtable == (False, True):
            if b.metadata.get("archive", None) is not None:
                return Archived(dir_name=b_dir, metadata=b.metadata)
            else:
                return Moved(dir_name=b_dir, metadata=b.metadata)

        if subtable == (True, False):
            return Renamed(base_name=b_name, metadata=b.metadata)

        if subtable == (False, False):
            if b.metadata.get("archive", None) is not None:
                return Archived(dir_name=b_dir, metadata=b.metadata)
            else:
                return Moved(dir_name=b_dir, metadata=b.metadata)

    if table == (False, True):
        return Modified(modified=b.modified, digest=b.digest)

    return None  # unreachable


def update(file_info: FileInfo, action: Action) -> FileInfo:
    if isinstance(action, Moved):
        return replace(
            file_info,
            file_name=os.path.join(action.dir_name, file_info.base_name),
            metadata=action.metadata,
        )
    if isinstance(action, Renamed):
        return replace(
            file_info,
            file_name=os.path.join(file_info.dir_name, action.base_name),
            metadata=action.metadata,
        )
    if isinstance(action, Archived):
        return replace(
            file_info,
            file_name=os.path.join(action.dir_name, file_info.base_name),
            archived=True,
            metadata=action.metadata,
        )

    if isinstance(action, Modified):
        return replace(file_info, modified=action.modified, digest=action.digest)

    raise ValueError(f"Unknown action type: {type(action)}")
