from functools import reduce
import re
from typing import Optional, Tuple, List


def find_match_segments(expr: re.Pattern, s: str) -> List[Tuple[str, str]]:
    """
    Returns a list of `(pre-match, match)` strings representing each 
    non-matching and matching segment of the original string, i.e. segmenting 
    the entire original string into pairs of unmatched bits and matched bits. 
    Note match _groups_ are ignored, this deals just with the whole match.
    """

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
    """
    Returns each pair of matches of expr in s, padded with a None at the
    start and None at the end:  [(None, m1), (m1, m2), (m2, m3), (m3, None)]
    You can then use this to process each non-matching and matching segment
    of the original string in order (see `find_match_segments` above).
    There's probably a way to do this with re.Match objects themselves 
    somehow, but they are quite poorly documented...
    """

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
