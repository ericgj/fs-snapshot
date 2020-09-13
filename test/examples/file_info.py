from dataclasses import replace
from datetime import timedelta
import os
from typing import Optional, Iterable, Callable, Dict

import hypothesis.strategies as hyp

from fs_snapshot.model.file_info import FileInfo, Digest


def list_of_examples(
    current_time: float, *, min_size: int = 1, max_size: int = None,
) -> hyp.SearchStrategy:
    return hyp.lists(
        hyp.tuples(digests(), file_names(),),
        unique=True,
        min_size=min_size,
        max_size=max_size,
    ).flatmap(
        lambda pairs: (
            hyp.tuples(
                *[
                    examples(current_time, digest=digest, file_name=file_name,)
                    for (digest, file_name) in pairs
                ]
            ).map(list)
        )
    )


def split_list_of_examples(
    current_time: float, *, min_size: int = 1, max_size: int = None,
) -> hyp.SearchStrategy:
    return (
        hyp.integers(min_value=min_size, max_value=max_size)
        .flatmap(
            lambda n: hyp.tuples(
                hyp.just(n),
                list_of_examples(
                    current_time,
                    min_size=n,
                    max_size=(None if max_size is None else n + max_size),
                ),
            )
        )
        .map(lambda pair: (pair[1][: pair[0]], pair[1][pair[0] :]))
    )


def examples(
    current_time: float,
    *,
    digest: Optional[Digest] = None,
    file_name: Optional[str] = None,
    archived: Optional[bool] = False,
    metadata: Optional[Dict[str, str]] = None,
) -> hyp.SearchStrategy:
    return hyp.builds(
        FileInfo,
        digest=digests() if digest is None else hyp.just(digest),
        file_name=file_names(max_depth=10)
        if file_name is None
        else hyp.just(file_name),
        created=times_near(current_time, before=timedelta(weeks=2)),
        modified=times_near(current_time, before=timedelta(weeks=1)),
        size=hyp.integers(min_value=1, max_value=(2 ** 16)),
        archived=hyp.booleans() if archived is None else hyp.just(archived),
        metadata=sample_metadata(["client", "protocol", "account"])
        if metadata is None
        else hyp.just(metadata),
    )


def with_changes(
    fn: Callable[[int, FileInfo], hyp.SearchStrategy]
) -> Callable[[Iterable[FileInfo]], hyp.SearchStrategy]:
    def _with_changes(original_files: Iterable[FileInfo],) -> hyp.SearchStrategy:
        return hyp.tuples(
            hyp.just(original_files),
            hyp.tuples(*[fn(i, f) for (i, f) in enumerate(original_files)]).map(list),
        )

    return _with_changes


def with_copies(
    fn: Callable[[int, FileInfo], hyp.SearchStrategy]
) -> Callable[[Iterable[FileInfo]], hyp.SearchStrategy]:
    def _with_copies(original_files: Iterable[FileInfo],) -> hyp.SearchStrategy:
        return hyp.tuples(
            hyp.just(original_files),
            hyp.tuples(*[fn(i, f) for (i, f) in enumerate(original_files)]).map(list),
        ).map(lambda pair: (pair[0], [f for f in pair[1] if f is not None]))

    return _with_copies


def then_was_moved(original: FileInfo,) -> hyp.SearchStrategy:
    return hyp.tuples(
        dir_names(max_depth=10).filter(lambda s: s != original.dir_name),
        sample_metadata(["client", "protocol", "account"]),
    ).map(
        lambda pair: replace(
            original,
            file_name=os.path.join(pair[0], original.base_name),
            metadata=pair[1],
        )
    )


def then_was_renamed(original: FileInfo,) -> hyp.SearchStrategy:
    return hyp.tuples(
        file_base_names().filter(lambda s: s != original.base_name),
        sample_metadata(["client", "protocol", "account"]),
    ).map(
        lambda pair: replace(
            original,
            file_name=os.path.join(original.dir_name, pair[0]),
            metadata=pair[1],
        )
    )


def then_was_archived(original: FileInfo,) -> hyp.SearchStrategy:
    return hyp.tuples(
        dir_names(max_depth=10).filter(lambda s: s != original.dir_name),
        sample_metadata(["client", "protocol", "account"]),
    ).map(
        lambda pair: replace(
            original,
            file_name=os.path.join(
                os.path.join(original.dir_name, pair[0]), original.base_name
            ),
            metadata=pair[1],
            archived=True,
        )
    )


def then_was_modified(original: FileInfo,) -> hyp.SearchStrategy:
    return hyp.tuples(
        digests().filter(lambda b: b != original.digest),
        hyp.timedeltas().filter(lambda td: td.total_seconds() > 0),
        hyp.integers(min_value=1, max_value=(2 ** 16)),
        sample_metadata(["client", "protocol", "account"]),
    ).map(
        lambda state: replace(
            original,
            digest=state[0],
            modified=original.modified + state[1].total_seconds(),
            size=state[2],
            metadata=state[3],
        )
    )


def digests() -> hyp.SearchStrategy:
    return hyp.binary(min_size=16, max_size=16)


def dir_names(*, min_depth: int = 0, max_depth: int = 5,) -> hyp.SearchStrategy:
    return hyp.lists(file_path_segments(), min_size=min_depth, max_size=max_depth).map(
        lambda path_segments: os.sep.join(path_segments)
    )


def file_names(*, min_depth: int = 0, max_depth: int = 5,) -> hyp.SearchStrategy:
    return hyp.lists(
        file_path_segments(), min_size=min_depth, max_size=max_depth
    ).flatmap(
        lambda path_segments: file_base_names().map(
            lambda base_name: os.sep.join(path_segments + [base_name])
        )
    )


def file_path_segments(
    *, min_size: int = 1, max_size: Optional[int] = None
) -> hyp.SearchStrategy:
    return hyp.text(
        min_size=min_size,
        max_size=max_size,
        alphabet=hyp.characters(
            whitelist_categories=["Lu", "Ll", "Nd", "P"],
            blacklist_characters=["/", "\\", "<", ">", ":", '"', "|", "?", "*"],
        ),
    )


def file_base_names(
    *, min_size: int = 1, max_size: Optional[int] = None
) -> hyp.SearchStrategy:
    return file_path_segments(min_size=min_size, max_size=max_size).flatmap(
        lambda base_name: file_path_segments(min_size=1, max_size=10).map(
            lambda ext: ".".join([base_name, ext])
        )
    )


def times_near(
    t: float,
    *,
    before: timedelta = timedelta(minutes=1),
    after: timedelta = timedelta(minutes=0),
) -> hyp.SearchStrategy:
    return hyp.floats(
        min_value=t - before.total_seconds(), max_value=t + after.total_seconds(),
    )


def sample_metadata(fields: Iterable[str],) -> hyp.SearchStrategy:
    return hyp.fixed_dictionaries(dict((field, metadata_values()) for field in fields))


def metadata_values(
    *, min_size: int = 1, max_size: Optional[int] = None
) -> hyp.SearchStrategy:
    return hyp.text(
        min_size=min_size,
        max_size=max_size,
        alphabet=hyp.characters(
            whitelist_categories=["Lu", "Ll", "Nd", "P"],
            blacklist_characters=["/", "\\", "<", ">", ":", '"', "|", "?", "*"],
        ),
    )
