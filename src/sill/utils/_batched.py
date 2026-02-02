import functools
import inspect
import logging
from datetime import UTC, datetime, timedelta
from functools import wraps
from itertools import chain

logger = logging.getLogger(__name__)


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
        logger.warning(
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
        raise ValueError(f"{start=} is later than or equal to {end=}")

    if chunk_size > (end - start):
        logger.debug(
            "Batching is unnecessary since the chunk size covers the entire requested interval."
        )

    current_start = start
    while current_start < end:
        new_end = _saturating_add(current_start, chunk_size)

        current_end = min(new_end, end)
        yield (current_start, current_end)
        current_start = current_end


def batched(start_arg: str, end_arg: str, *, chunk_size: timedelta, how: str):
    """
    Decorator to batch requests over time intervals.

    The decorated function is expected to use a start and end datetime
    parameters, named start_arg and end_arg respectively. The decorated
    function is then called repeatedly for each chunked interval, and the
    wrapper returns a list containing one response per chunk, in call order.

    :param start_arg: Name of the start datetime parameter in the decorated function.
    :param end_arg: Name of the end datetime parameter in the decorated function.
    :param chunk_size: Size of each chunk as a timedelta.
    :param how: How the parameters are passed; either 'json' for JSON body or 'query' for query parameters.
    """

    if how == "json":
        key = "json"
    elif how == "query":
        key = "params"
    else:
        raise ValueError(f"Unsupported 'how' value: {how}; expected 'json' or 'query'")

    def decorator_batched(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            start, end = _extract_interval(f, key, start_arg, end_arg, **kwargs)

            def batch_iter():
                for start_, end_ in _chunk_dates(start, end, chunk_size=chunk_size):
                    bound = _bind_args(f, *args, **kwargs)
                    _modify_signature(
                        f, bound.arguments, key, start_, start_arg, end_, end_arg
                    )
                    resp = f(*bound.args, **bound.kwargs)
                    yield resp

            responses = list(chain(batch_iter()))
            return responses

        return wrapper

    return decorator_batched


def _bind_args(f, *args, **kwargs):
    sig = inspect.signature(
        f, follow_wrapped=False
    )  # We want to inspect the get/post wrapper's signature, so don't follow wrapped
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()

    return bound


def _modify_signature(
    f,
    mut_params: dict,
    param_key: str,
    start: datetime,
    start_arg: str,
    end: datetime | None = None,
    end_arg: str | None = None,
) -> None:
    """
    Modifies the mutable parameters dictionary to update the start and (optionally) end datetime arguments.
    :param f: The decorated function.
    :param mut_params: Mutable parameters dictionary from the bound function.
    :param param_key: The key under which the parameters are passed (e.g., 'json' for JSON body or 'params' for query parameters).
    :param start: New start datetime.
    :param start_arg: Name of the start datetime parameter.
    :param end: New end datetime (optional).
    :param end_arg: Name of the end datetime parameter (optional).
    :raises ValueError: If the start_arg is not found in any of the expected places in the mutable parameters.
    """
    new_start = start.isoformat()
    new_end = end.isoformat() if end is not None else None

    def modify(d: dict):
        d[start_arg] = new_start
        if new_end is not None:
            d[end_arg] = new_end

    match f._method:
        case "GET":
            # GET requests only accept requests.request arguments as extra kwargs; so look in the given key
            mut_args = _find_request_kwarg(mut_params, key=param_key)
            modify(mut_args)
        case "POST":
            user_parameters = inspect.signature(f).parameters

            if mut_params.get("request_kwargs") is not None:
                # request_kwargs["{param_key}"] needs to be updated if it exists since it takes precedence over direct kwargs
                req_kwargs = _find_request_kwarg(mut_params, key=param_key)

                has_batch_request_kwargs = (
                    start_arg in req_kwargs or end_arg in req_kwargs
                )
                if has_batch_request_kwargs:
                    modify(req_kwargs)
            elif start_arg in user_parameters:
                modify(mut_params["kwargs"])
        case _:
            raise ValueError(f"Unsupported method: {f._method}")

    return


def _find_request_kwarg(d: dict, key) -> dict:
    request_kwargs = d.get("request_kwargs")
    if request_kwargs is None or key not in request_kwargs:
        raise ValueError(
            f"The signature has no place for batching parameters; expected to find them in request_kwargs['{key}']"
        )
    return request_kwargs[key]


def _extract_interval(
    f, key: str, start_arg: str, end_arg: str | None = None, *args, **kwargs
) -> tuple[datetime, datetime | None]:
    """
    Extract the start and end datetime arguments from the decorated function's parameters.
    Selects the appropriate extraction method based on the HTTP method, which is expected to be
    stored as metadata in the function's _method attribute.

    :param f: The decorated function.
    :key: The key under which the parameters are passed (e.g., 'json' for JSON body or 'params' for query parameters).
    :param start_arg: Name of the start datetime parameter.
    :param end_arg: Name of the end datetime parameter (optional).
    :param args: Positional arguments to the decorated function.
    :param kwargs: Keyword arguments to the decorated function.
    :return: The start and optional end datetime arguments as a tuple.
    :raises ValueError: If the start argument is not found or if the HTTP method is unsupported.
    """
    match m := f._method:
        case "GET":
            extract_func = _extract_interval_get
        case "POST":
            extract_func = functools.partial(_extract_interval_post, args=args, f=f)
        case _:
            raise ValueError(f"Unsupported method: {m}")

    start, end = extract_func(key, start_arg, end_arg, **kwargs)
    if start is None:
        raise ValueError(f"Expected start argument '{start_arg}' not found in kwargs")

    start = _to_datetime(start)
    end = _to_datetime(end) if end is not None else None

    return start, end


def _extract_interval_get(
    key: str, start_arg: str, end_arg: str | None = None, **kwargs
) -> tuple[datetime | None, datetime | None]:
    # GET requests only accept requests.request arguments in its extra kwargs, so look there for the key
    start = kwargs.get(key, {}).get(start_arg)
    end = kwargs.get(key, {}).get(end_arg)

    return start, end


def _extract_interval_post(
    key: str, start_arg: str, end_arg: str | None = None, *, args, f, **kwargs
) -> tuple[datetime | None, datetime | None]:
    # find start/end args regardless of whether they were passed positionally or as keywords
    bound = _bind_args(f, *args, **kwargs)
    start = bound.kwargs.get(start_arg)
    end = bound.kwargs.get(end_arg)

    # request_kwargs["json"] takes precedence over direct kwargs
    request_kwargs = bound.kwargs.get("request_kwargs")
    if request_kwargs is not None:
        # For now, only look in json (no support for query params yet)
        if (key_kwarg := request_kwargs.get(key)) is not None:
            start = key_kwarg.get(start_arg, start)
            end = key_kwarg.get(end_arg, end)

    return start, end


def _to_datetime(dt: str | datetime) -> datetime:
    """Helper to convert a possibly already serialized datetime (i.e., a timestamp string) to a datetime object."""
    match dt:
        case str():
            return datetime.fromisoformat(dt)
        case datetime():
            return dt
        case _:
            raise TypeError(f"Expected str or datetime, got {type(dt)}")
