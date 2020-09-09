from functools import reduce
import re
from typing import Optional, Tuple, List


def find_match_segments(expr: re.Pattern, s: str) -> List[Tuple[str, str]]:
    def _pairs_to_segments(a, b):
        a_span = None if a is None else a.span()
        b_span = None if b is None else b.span()
        pre = ""
        if a_span is None and b_span is None:
            pre = ""
        elif a_span is None:
            pre = s[: b_span[0]]
        elif b_span is None:
            pre = s[a_span[1] :]
        else:
            pre = s[a_span[1] : b_span[0]]
        m = "" if b is None else b.group(0)
        return (pre, m)

    return [_pairs_to_segments(a, b) for (a, b) in find_match_pairs(expr, s)]


def find_match_pairs(
    expr: re.Pattern, s: str
) -> List[Tuple[Optional[re.Match], Optional[re.Match]]]:
    def _paired(acc, match):
        pairs, last = acc
        pairs.append((last, match))
        return (pairs, match)

    def _finalize(
        acc: Tuple[
            List[Tuple[Optional[re.Match], Optional[re.Match]]], Optional[re.Match]
        ]
    ) -> List[Tuple[Optional[re.Match], Optional[re.Match]]]:
        pairs, last = acc
        if last is not None:
            pairs.append((last, None))
        return pairs

    empty_list: List[Tuple[Optional[re.Match], Optional[re.Match]]] = []
    empty_match: Optional[re.Match] = None
    return _finalize(reduce(_paired, re.finditer(expr, s), (empty_list, empty_match)))
