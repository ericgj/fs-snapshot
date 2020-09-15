from configparser import ConfigParser
import shlex
from typing import Optional, List, Dict, Set

from ..model.config import (
    Config,
    ArchivedBy,
    ArchivedByMetadata,
    CalcBy,
    CalcByMetadata,
)

DEFAULT_SECTION = "fs-snapshot"


def parse_file(file_name: str) -> Dict[str, Config]:
    p = ConfigParser(default_section=DEFAULT_SECTION)
    p.read([file_name])
    return parse_sections(p)


def parse_sections(p: ConfigParser) -> Dict[str, Config]:
    return dict([(name, parse_section(dict(p[name]))) for name in p.sections()])


def parse_section(section: Dict[str, str]) -> Config:
    options = [
        ("match_paths", parse_string_list(section.get("match_paths", ""))),
        ("root_dir", parse_string(section.get("root_dir", ""))),
        ("log_file", parse_string(section.get("log_file", ""))),
        ("store_db_file", parse_string(section.get("store_db_file", ""))),
        (
            "store_db_import_table",
            parse_string(section.get("store_db_import_table", "")),
        ),
        (
            "store_db_file_info_table",
            parse_string(section.get("store_db_file_info_table", "")),
        ),
        ("metadata", parse_string_dict(section.get("metadata", ""))),
        ("archived_by", parse_archived_by(section.get("archived_by", ""))),
        ("file_group_by", parse_calc(section.get("file_group_by", ""))),
        ("file_type_by", parse_calc(section.get("file_type_by", ""))),
    ]
    return Config(**dict([(k, v) for (k, v) in options if v is not None]))  # type: ignore


def parse_string(s: str) -> Optional[str]:
    if len(s) == 0:
        return None
    return s.strip()


def parse_string_list(s: str) -> Optional[List[str]]:
    if len(s) == 0:
        return None
    ret: List[str] = []
    for line in s.split("\n"):
        pval = parse_string(line)
        if pval is not None:
            ret.append(pval)
    return ret


def parse_string_dict(s: str) -> Optional[Dict[str, str]]:
    lines = parse_string_list(s)
    if lines is None:
        return None
    ret: Dict[str, str] = {}
    for line in lines:
        pair = line.split("=")
        if len(pair) != 2:
            raise ValueError(f"Cannot parse as dict value: '{line}'")
        key = parse_string(pair[0])
        value = parse_string(pair[1])
        if key is None or value is None:
            raise ValueError(f"Cannot parse as dict value: '{line}'")
        ret[key] = value
    return ret


def parse_archived_by(s: str) -> Optional[ArchivedBy]:
    line = parse_string(s)
    if line is None:
        return None
    parts = shlex.split(line)
    if parts[0] == "has-metadata":
        return parse_archived_has_metadata(parts[1:])
    # TODO others
    raise ValueError(f"Cannot parse archived directive: '{line}'")


def parse_archived_has_metadata(args: List[str]) -> ArchivedBy:
    if len(args) != 2:
        raise ValueError(
            f"Cannot parse archived has-metadata directive: '{', '.join(args)}'"
        )
    key_param = args[0]
    values_param: Set[str] = set()
    for v in args[1].split(","):
        pval = parse_string(v)
        if pval is not None:
            values_param.add(pval)
    return ArchivedByMetadata(key=key_param, values=values_param)


def parse_calc(s: str) -> Optional[CalcBy]:
    line = parse_string(s)
    if line is None:
        return None
    parts = shlex.split(line)
    if parts[0] == "from-metadata":
        return parse_calc_from_metadata(parts[1:])
    # TODO others
    raise ValueError(f"Cannot parse calc directive: '{line}'")


def parse_calc_from_metadata(args: List[str]) -> CalcBy:
    if len(args) != 1:
        raise ValueError(
            f"Cannot parse calc from-metadata directive: '{', '.join(args)}'"
        )
    format_param = args[0]
    return CalcByMetadata(format=format_param)
