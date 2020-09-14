from dataclasses import dataclass, replace
import os.path
from typing import Union, Optional, Iterable, Generator, Dict

Digest = bytes


@dataclass
class FileInfo:
    digest: Digest
    dir_name: str
    base_name: str
    created: float
    modified: float
    size: int
    archived: bool
    metadata: Dict[str, str]

    @property
    def hexdigest(self):
        return self.digest.hex()

    @property
    def file_name(self):
        """ 
        Note: this is os.sep specific, so it assumes the same os created the
        record as is reading the file_name
        """
        return os.path.join(self.dir_name, self.base_name)

    def to_json(self):
        return {
            "$type": self.__class__.__name__,
            "digest": self.hexdigest,
            "dir_name": self.dir_name,
            "base_name": self.base_name,
            "file_name": self.file_name,
            "created": self.created,
            "modified": self.modified,
            "size": self.size,
            "archived": self.archived,
            "metadata": self.metadata,
        }


@dataclass
class NewOnly:
    new: FileInfo


@dataclass
class OriginalOnly:
    original: FileInfo


@dataclass
class OriginalAndNew:
    original: FileInfo
    new: FileInfo
    is_copy: bool


CompareStates = Union[NewOnly, OriginalOnly, OriginalAndNew]


@dataclass
class Created:
    new: FileInfo

    def to_json(self):
        return {
            "$type": self.__class__.__name__,
            "new": self.new.to_json(),
        }


@dataclass
class Removed:
    original: FileInfo

    def to_json(self):
        return {
            "$type": self.__class__.__name__,
            "original": self.original.to_json(),
        }


@dataclass
class Copied:
    original: FileInfo
    copy: FileInfo

    def to_json(self):
        return {
            "$type": self.__class__.__name__,
            "original": self.original.to_json(),
            "copy": self.copy.to_json(),
        }


@dataclass
class Moved:
    original: FileInfo
    dir_name: str
    metadata: Dict[str, str]

    def to_json(self):
        return {
            "$type": self.__class__.__name__,
            "original": self.original.to_json(),
            "dir_name": self.dir_name,
            "metadata": self.metadata,
        }


@dataclass
class Renamed:
    original: FileInfo
    base_name: str
    metadata: Dict[str, str]

    def to_json(self):
        return {
            "$type": self.__class__.__name__,
            "original": self.original.to_json(),
            "base_name": self.base_name,
            "metadata": self.metadata,
        }


@dataclass
class Archived:
    original: FileInfo
    dir_name: str
    metadata: Dict[str, str]

    def to_json(self):
        return {
            "$type": self.__class__.__name__,
            "original": self.original.to_json(),
            "dir_name": self.dir_name,
            "metadata": self.metadata,
        }


@dataclass
class Modified:
    original: FileInfo
    modified: float
    size: int
    digest: Digest

    def to_json(self):
        return {
            "$type": self.__class__.__name__,
            "original": self.original.to_json(),
            "modified": self.modified,
            "size": self.size,
            "digest": self.digest.hex(),
        }


Action = Union[Created, Removed, Copied, Moved, Renamed, Archived, Modified]


def diff_all(compare_states: Iterable[CompareStates]) -> Generator[Action, None, None]:
    for states in compare_states:
        action = diff(states)
        if action is not None:
            yield action


def diff(states: CompareStates) -> Optional[Action]:
    if isinstance(states, OriginalOnly):
        return Removed(original=states.original)

    if isinstance(states, NewOnly):
        return Created(new=states.new)

    if isinstance(states, OriginalAndNew):
        if states.is_copy:
            return Copied(original=states.original, copy=states.new)
        else:
            return diff_file_info(states.original, states.new)

    raise ValueError(f"Unknown statesStates type: {type(states)}")


def diff_file_info(a: FileInfo, b: FileInfo) -> Optional[Action]:
    table = (a.digest == b.digest, a.file_name == b.file_name)
    if table == (False, False):
        return None  # should not reach in practice

    if table == (True, True):
        return None

    if table == (True, False):
        a_dir, a_name = a.dir_name, a.base_name
        b_dir, b_name = b.dir_name, b.base_name
        subtable = (a_dir == b_dir, a_name == b_name)

        if subtable == (True, True):
            return None  # should never reach

        if subtable == (False, True):
            if b.archived:
                return Archived(original=a, dir_name=b_dir, metadata=b.metadata)
            else:
                return Moved(original=a, dir_name=b_dir, metadata=b.metadata)

        if subtable == (True, False):
            return Renamed(original=a, base_name=b_name, metadata=b.metadata)

        if subtable == (False, False):
            if b.archived:
                return Archived(original=a, dir_name=b_dir, metadata=b.metadata)
            else:
                return Moved(original=a, dir_name=b_dir, metadata=b.metadata)

    if table == (False, True):
        return Modified(original=a, modified=b.modified, size=b.size, digest=b.digest)

    return None  # unreachable


def update(file_info: FileInfo, action: Action) -> FileInfo:
    if isinstance(action, Moved):
        return replace(file_info, dir_name=action.dir_name, metadata=action.metadata,)
    if isinstance(action, Renamed):
        return replace(file_info, base_name=action.base_name, metadata=action.metadata,)
    if isinstance(action, Archived):
        return replace(
            file_info,
            dir_name=action.dir_name,
            archived=True,
            metadata=action.metadata,
        )

    if isinstance(action, Modified):
        return replace(
            file_info, digest=action.digest, modified=action.modified, size=action.size,
        )

    # Note: these three actions should not actually be pushed through this function

    if isinstance(action, Created):
        return file_info

    if isinstance(action, Removed):
        return file_info

    if isinstance(action, Copied):
        return file_info

    raise ValueError(f"Unknown action type: {type(action)}")
