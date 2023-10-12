from functools import reduce
import hashlib
import os
import os.path
from glob import iglob
import re
from typing import Optional, Sequence, Mapping, Tuple, Dict, Callable, Generator

from ..model.file_info import FileInfo, Digest
from ..util import re_
from ..adapter.logging import get_logger

REGEXP_VAR = re.compile("(\\{[a-zA-Z0-9_]+\\})", flags=re.I + re.A)
REGEXP_VAR_OR_GLOB = re.compile("(\\{[a-zA-Z0-9_]+\\}|\\*\\*|\\*)", flags=re.I + re.A)

LOGGER = get_logger(__name__)


class Matcher:
    def __init__(self, expr: str):
        self._expr = compiled_match_expr(expr)
        self.glob = REGEXP_VAR.sub("*", expr)

    def match(self, dir: str) -> Optional[Dict[str, str]]:
        matches = re.match(self._expr, dir)
        if matches is None:
            return None
        return matches.groupdict()


def search(
    root_dir: str,
    match_paths: Mapping[str, Sequence[str]],
    gather_digests: bool = False,
    is_archived: Optional[Callable[[Dict[str, str]], bool]] = None,
    calc_file_group: Optional[Callable[[Dict[str, str]], Optional[str]]] = None,
) -> Generator[FileInfo, None, None]:
    matchers = {
        file_type: [
            Matcher(os.path.join(root_dir, match_path))
            for match_path in match_paths[file_type]
        ]
        for file_type in match_paths
    }
    for fname in iglob(os.path.join(root_dir, "**"), recursive=True):
        if not os.path.isfile(fname):
            continue
        pair = match_file_type_and_metadata(fname, matchers)
        if pair is None:
            LOGGER.debug(f"Unmatched file: {fname}")
            yield fetch_file_info(
                fname,
                gather_digests=False,
                archived=False,
                file_group=None,
                file_type=None,
                metadata={},
            )
        else:
            file_type, metadata = pair
            LOGGER.debug(f"Matched {file_type} file: {fname}")
            yield fetch_file_info(
                fname,
                gather_digests=gather_digests,
                archived=(False if is_archived is None else is_archived(metadata)),
                file_group=(
                    None if calc_file_group is None else calc_file_group(metadata)
                ),
                file_type=file_type,
                metadata=metadata,
            )


def match_file_type_and_metadata(
    fname: str, match_paths: Mapping[str, Sequence[Matcher]]
) -> Optional[Tuple[str, Dict[str, str]]]:
    def _match_metadata(matchers: Sequence[Matcher]) -> Optional[Dict[str, str]]:
        try:
            return next(
                m for matcher in matchers if (m := matcher.match(fname)) is not None
            )
        except StopIteration:
            return None

    try:
        return next(
            (ft, m)
            for ft in match_paths
            if (m := _match_metadata(match_paths[ft])) is not None
        )
    except StopIteration:
        return None


def fetch_file_info(
    fname: str,
    gather_digests: bool = False,
    archived: bool = False,
    file_group: Optional[str] = None,
    file_type: Optional[str] = None,
    metadata: Dict[str, str] = {},
) -> FileInfo:
    """Note: at most 8MB per chunk ~= 100 iterations/GB"""
    fstat = os.stat(fname)
    size = int(fstat.st_size)
    fdigest: Digest = b""
    if gather_digests:
        LOGGER.debug(f"Digest started: {fname}")
        fdigest = digest(fname, min(size, 2**23))
        LOGGER.debug(f"Digest ended: {fname}")

    return FileInfo(
        created=float(fstat.st_ctime),
        modified=float(fstat.st_mtime),
        size=size,
        dir_name=os.path.dirname(fname),
        base_name=os.path.basename(fname),
        digest=fdigest,
        archived=archived,
        file_group=file_group,
        file_type=file_type,
        metadata=metadata,
    )


def digest(fname: str, chunk_size: int) -> Digest:
    h = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.digest()


# Matcher helpers


def compiled_match_expr(expr: str) -> re.Pattern:
    def _build_segment(segment, pair):
        pre, s = pair
        return "".join(
            [segment, re.escape(pre), "" if len(s) == 0 else _parse_var_or_glob(s)]
        )

    def _parse_var_or_glob(s: str) -> str:
        if s == "**":
            return ".+"
        elif s == "*":
            return f"[^{re.escape(os.sep)}]+"
        else:
            return f"(?P<{s[1:-1]}>[^{re.escape(os.sep)}]+)"

    def _parse_segment(s: str) -> str:
        subsegments = re_.find_match_segments(REGEXP_VAR_OR_GLOB, s)
        if len(subsegments) == 0:  # no match in segment, parse as literal
            return re.escape(s)
        else:
            return reduce(_build_segment, subsegments, "")

    segments = expr.split(os.sep)
    parsed_expr = re.escape(os.sep).join(
        [_parse_segment(segment) for segment in segments]
    )
    return re.compile(
        parsed_expr,
        flags=re.A + re.I,
    )
