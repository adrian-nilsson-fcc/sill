import inspect
from datetime import UTC, datetime, timedelta
from functools import wraps
from itertools import chain


def _chunk_dates(
    start: datetime, end: datetime | None = None, *, chunk_size: timedelta | None = None
):
    """
    Yield consecutive date ranges between start and end, chunked by chunk_size.

    :param start: Start datetime.
    :param end: End datetime (defaults to now if None).
    :param chunk_size: Size of each chunk (defaults to 4 weeks if None).
    :raises ValueError: If start is after end.
    :yields: Tuple of (chunk_start, chunk_end) datetimes.
    """
    chunk_size = chunk_size or timedelta(weeks=4)
    end = end or datetime.now(UTC)

    if start > end:
        raise ValueError(f"{end=} is later than or equal to {start=}")

    current_start = start
    while current_start < end:
        current_end = min(current_start + chunk_size, end)
        yield (current_start, current_end)
        current_start = current_end


def batched(f, start_arg: str, end_arg: str):
    """
    Decorator to batch requests over time intervals.

    The decorated function is expected to use a start and end datetime
    parameters, named start_arg and end_arg respectively. The decorate function is
    then called repeatedly for each chunked interval, and the results will be
    concatenated before returning.

    :param f: Function to decorate.
    :param start_arg: Name of the start datetime parameter in the decorated function.
    :param end_arg: Name of the end datetime parameter in the decorated function.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        sig = inspect.signature(f)
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()

        start = bound.arguments[start_arg]
        end = bound.arguments.get(end_arg)

        def batch_iter():
            for start_, end_ in _chunk_dates(start, end):
                bound.arguments[start_arg] = start_
                bound.arguments[end_arg] = end_
                yield f(*bound.args, **bound.kwargs)  # yields response data

        return list(chain(*batch_iter()))

    return wrapper
