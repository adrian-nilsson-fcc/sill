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

    if chunk_size == timedelta(0):
        raise ValueError("chunk_size must be greater than zero")

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
            start, end = _extract_interval(start_arg, end_arg, kwargs=kwargs)

            def batch_iter():
                request_json = kwargs["request_kwargs"].setdefault("json", {})
                for start_, end_ in _chunk_dates(start, end, chunk_size=chunk_size):
                    request_json[start_arg] = start_.isoformat()
                    request_json[end_arg] = end_.isoformat()

                    resp = f(*args, **kwargs)
                    yield resp

            responses = list(chain(batch_iter()))
            return responses

        return wrapper

    return decorator_batched


def _extract_interval(
    start_arg: str,
    end_arg: str,
    *,
    kwargs: dict,
) -> tuple[datetime, datetime]:
    request_kwargs = kwargs.get("request_kwargs", None)

    if request_kwargs is None:
        raise ValueError(
            "Start and end times must be passed in the 'request_kwargs' argument, but it was not found."
        )

    start = None
    end = None
    if (json_kwarg := request_kwargs.get("json", None)) is not None:
        start = json_kwarg.get(start_arg, None)
        end = json_kwarg.get(end_arg, None)

    if start is None and end is None:
        # For now, only look in json (no support for query params yet)
        raise ValueError(f"Expected start argument '{start_arg}' not found in kwargs")

    return _to_datetime(start), _to_datetime(end)


def _to_datetime(dt: str | datetime) -> datetime:
    """Helper to convert a possibly already serialized datetime (i.e., a timestamp string) to a datetime object."""
    match dt:
        case str():
            return datetime.fromisoformat(dt)
        case datetime():
            return dt
        case _:
            raise TypeError(f"Expected str or datetime, got {type(dt)}")
