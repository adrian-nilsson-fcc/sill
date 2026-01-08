import warnings
from datetime import UTC, datetime, timedelta
from functools import wraps
from itertools import chain


def _saturating_add(dt: datetime, delta: timedelta) -> datetime:
    """
    Adds a timedelta to a datetime, saturating at datetime.max on overflow.

    :param dt: The original datetime.
    :param delta: The timedelta to add.
    :return: The resulting datetime, or datetime.max if overflow occurs.
    """
    try:
        return dt + delta
    except OverflowError:
        warnings.warn(
            f"Overflow when adding {delta} to {dt}; returning datetime.max instead."
        )
        return datetime.max.replace(tzinfo=dt.tzinfo)


def _chunk_dates(
    start: datetime, end: datetime | None = None, *, chunk_size: timedelta
):
    """
    Yield consecutive date ranges between start and end, chunked by chunk_size.

    :param start: Start datetime.
    :param end: End datetime (defaults to now if None).
    :param chunk_size: Size of each chunk
    :raises ValueError: If start is after end.
    :yields: Tuple of (chunk_start, chunk_end) datetimes.
    """
    end = end or datetime.now(UTC)

    if start > end:
        raise ValueError(f"{end=} is later than or equal to {start=}")

    if chunk_size > (end - start):
        warnings.warn(
            "Batching is unnecessary since the chunk size covers the entire requested interval."
        )

    current_start = start
    while current_start < end:
        new_end = _saturating_add(current_start, chunk_size)

        current_end = min(new_end, end)
        yield (current_start, current_end)
        current_start = current_end


def batched(start_arg: str, end_arg: str, *, chunk_size: timedelta):
    """
    Decorator to batch requests over time intervals.

    The decorated function is expected to use a start and end datetime
    parameters, named start_arg and end_arg respectively. The decorate function is
    then called repeatedly for each chunked interval, and the results will be
    concatenated before returning.

    :param start_arg: Name of the start datetime parameter in the decorated function.
    :param end_arg: Name of the end datetime parameter in the decorated function.
    """

    def decorator_batched(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            start = datetime.fromisoformat(kwargs["json"][start_arg])
            end = datetime.fromisoformat(kwargs["json"][end_arg])

            def batch_iter():
                for start_, end_ in _chunk_dates(start, end, chunk_size=chunk_size):
                    kwargs["json"][start_arg] = start_.isoformat()
                    kwargs["json"][end_arg] = end_.isoformat()
                    yield f(*args, **kwargs)  # yields a Response

            chained = list(chain(batch_iter()))

            return chained

        return wrapper

    return decorator_batched
