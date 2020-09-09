import hashlib
import os
import os.path
from glob import glob
import re
from typing import Optional, Dict, Callable, Generator

from model.file_info import FileInfo, Digest

REGEXP_VAR = re.compile("(\{[a-zA-Z0-9_]+\}|\*\*|\*)", flags=re.I + re.A)


def search(
    root_dir: str,
    match_path: str,
    is_archived: Optional[Callable[[Dict[str, str]], bool]] = None,
) -> Generator[FileInfo, None, None]:
    matcher = Matcher(os.path.join(root_dir, match_path))
    for fname in glob(matcher.glob):
        metadata = matcher.match(fname)
        if metadata is not None:
            yield fetch_file_info(
                fname,
                archived=(False if is_archived is None else is_archived(metadata)),
                metadata=metadata,
            )


def fetch_file_info(
    fname: str, archived: bool = False, metadata: Dict[str, str] = {}
) -> FileInfo:
    fstat = os.stat(fname)
    fdigest = digest(fname)
    return FileInfo(
        created=float(fstat.st_ctime),
        modified=float(fstat.st_mtime),
        size=int(fstat.st_size),
        file_name=fname,
        digest=fdigest,
        archived=archived,
        metadata=metadata,
    )


def digest(fname: str) -> Digest:
    h = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(1024), b""):
            h.update(chunk)
    return h.digest()


class Matcher:
    def __init__(self, expr: str):
        self._expr = compiled_match_expr(expr)
        self.glob = REGEXP_VAR.sub("*", expr)

    def match(self, dir: str) -> Optional[Dict[str, str]]:
        matches = re.match(self._expr, dir)
        if matches is None:
            return None
        return matches.groupdict()


def compiled_match_expr(expr: str) -> re.Pattern:
    def _parse_var_or_glob(s: str) -> str:
        if s == "**":
            return ".+"
        elif s == "*":
            return f"[^{re.escape(os.sep)}]+"
        else:
            return f"(?P<{s[1:-1]}>[^{re.escape(os.sep)}]+)"

    def _parse_segment(s: str) -> str:
        matches = re.search(REGEXP_VAR, s)
        if matches is None:
            return re.escape(s)
        return "".join(
            [
                re.escape(matches.string[: matches.start(1)])
                + _parse_var_or_glob(m)
                + re.escape(matches.string[matches.end(1) :])
                for m in matches.groups()
            ]
        )

    segments = expr.split(os.sep)
    return re.compile(
        re.escape(os.sep).join([_parse_segment(segment) for segment in segments]),
        flags=re.A + re.I,
    )
