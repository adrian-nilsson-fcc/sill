import json
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from itertools import chain, dropwhile, takewhile
from operator import itemgetter

import pytest
import requests
from hypothesis import assume, given
from hypothesis import strategies as st
from pydantic import TypeAdapter
from pydantic_core import to_jsonable_python
from pytest_httpserver import HTTPServer
from werkzeug import Request, Response

import sill

_now = datetime.now(tz=UTC)

ts_getter = itemgetter("ts")
st_observations = st.fixed_dictionaries(
    {
        "ts": st.datetimes(
            min_value=datetime(_now.year - 50, 1, 1),
            max_value=datetime(_now.year + 50, 1, 1),
        ),
        "value": st.floats(allow_nan=False),
    }
)
st_timeseries = st.lists(st_observations, unique_by=ts_getter, min_size=1).map(
    lambda tss: sorted(tss, key=ts_getter)
)


@contextmanager
def default_server():
    server = HTTPServer(
        host=HTTPServer.DEFAULT_LISTEN_HOST,
        port=HTTPServer.DEFAULT_LISTEN_PORT,
    )
    try:
        server.start()
        yield server
    finally:
        server.clean()


def handler_factory(data):
    def handler(request: Request) -> Response:
        request_json = request.get_json()
        if request_json.get("id") is None:
            resp = Response(
                json.dumps({"error": "Missing 'id' in request JSON body"}), status=400
            )
            return resp

        start = datetime.fromisoformat(request_json["start"])
        end = request_json.get("end")

        response_data = dropwhile(lambda o: o["ts"] < start, data)
        if end is not None:
            end = datetime.fromisoformat(end)
            response_data = takewhile(lambda o: o["ts"] <= end, response_data)

        response_data = list(response_data)
        resp = Response(
            json.dumps(to_jsonable_python(response_data)), mimetype="application/json"
        )
        return resp

    return handler


def make_request_json(ts) -> dict:
    request_payload = {"id": "uuid", "start": ts[0]["ts"]}
    if len(ts) > 1:
        request_payload["end"] = ts[-1]["ts"]

    return to_jsonable_python(request_payload)


def history_endpoint(base_url: str, path: str):
    api = sill.API(url=base_url)

    @api.get(path=path)
    def get_history(resp):
        return resp

    return get_history


def history_batched_get(base_url: str, path: str, chunk_size: timedelta):
    api = sill.API(url=base_url)

    @sill.utils.batched(start_arg="start", end_arg="end", chunk_size=chunk_size)
    @api.get(path=path)
    def get_history(resp):
        return resp

    return get_history


def history_batched_post(base_url: str, path: str, chunk_size: timedelta):
    api = sill.API(url=base_url)

    @sill.utils.batched(start_arg="start", end_arg="end", chunk_size=chunk_size)
    @api.post(path=path)
    def get_history(asset_id: str, start: str, end: str | None = None):
        return {"id": asset_id, "start": start, "end": end}

    return get_history


def history_batched_post_no_capture(base_url: str, path: str, chunk_size: timedelta):
    api = sill.API(url=base_url)

    @sill.utils.batched(start_arg="start", end_arg="end", chunk_size=chunk_size)
    @api.post(path=path)
    def get_history(asset_id: str):
        return {"id": asset_id}

    return get_history


def call_endpoint(
    endpoint, url: str, json_payload: dict, chunk_size: timedelta
) -> requests.Response | None:
    fetch_history = endpoint(url, path="history", chunk_size=chunk_size)

    # fetch_history expects an "asset_id" parameter, but the server expects "id"
    fetch_kwargs = json_payload.copy()
    id_ = fetch_kwargs.pop("id")
    fetch_kwargs["asset_id"] = id_

    if endpoint is history_batched_get:
        resp = fetch_history(json=json_payload)
    elif endpoint is history_batched_post:
        resp = fetch_history(**fetch_kwargs)
    elif endpoint is history_batched_post_no_capture:
        resp = fetch_history(asset_id=None, request_kwargs={"json": json_payload})
    else:
        raise ValueError("Unsupported endpoint")

    return resp


@given(ts=st_timeseries)
def test_batched_server(ts, make_httpserver):
    server = make_httpserver
    handler = handler_factory(ts)
    request_json = make_request_json(ts)

    try:
        server.expect_request("/history", method="GET").respond_with_handler(handler)
        get_history = history_endpoint(server.url_for(""), path="history")

        resp = get_history(json=request_json)
        resp_values = TypeAdapter(list[dict[str, datetime | float]]).validate_json(
            resp.content
        )
        assert resp_values == ts
    finally:
        server.clear()


@pytest.mark.parametrize(
    "endpoint",
    [history_batched_get, history_batched_post, history_batched_post_no_capture],
)
@given(
    ts=st_timeseries.filter(lambda ts: len(ts) > 1),
    n_chunks=st.integers(min_value=2, max_value=10),
)
def test_batched_request(ts, n_chunks, endpoint, make_httpserver):
    server = make_httpserver
    handler = handler_factory(ts)
    history_json = make_request_json(ts)

    chunk_size = (ts[-1]["ts"] - ts[0]["ts"]) / n_chunks
    assume(chunk_size > timedelta(0))
    try:
        for method in ["GET", "POST"]:
            server.expect_request("/history", method=method).respond_with_handler(
                handler
            )

        resp = call_endpoint(
            endpoint, server.url_for(""), history_json, chunk_size=chunk_size
        )
        assert (
            len(resp) >= n_chunks and len(resp) <= n_chunks + 1
        )  # rounding can cause this to be off-by-one

        resp_validator = TypeAdapter(list[dict[str, datetime | float]])
        resp_json = [
            resp_validator.validate_json(r.content) for r in resp if r.content != b"[]"
        ]

        resp_values = list(chain.from_iterable(resp_json))
        assert resp_values == ts
    finally:
        server.clear()


@given(
    ts=st_timeseries.filter(lambda ts: len(ts) > 1),
    n_chunks=st.integers(min_value=2, max_value=10),
)
def test_batched_post_expected_error(ts, n_chunks, make_httpserver):
    server = make_httpserver
    handler = handler_factory(ts)
    history_json = make_request_json(ts)

    # fetch_history expects an "asset_id" parameter, but the server expects "id"
    fetch_kwargs = history_json.copy()
    id_ = fetch_kwargs.pop("id")
    fetch_kwargs["asset_id"] = id_

    chunk_size = (ts[-1]["ts"] - ts[0]["ts"]) / n_chunks
    assume(chunk_size > timedelta(0))

    endpoint = history_batched_post
    try:
        server.expect_request("/history", method="POST").respond_with_handler(handler)
        fetch_history = endpoint(
            server.url_for(""), path="history", chunk_size=chunk_size
        )

        with pytest.raises(requests.exceptions.HTTPError) as e:
            # Overwrites the returned JSON from fetch_history with empty dict; causes server to error
            fetch_history(**fetch_kwargs, request_kwargs={"json": {}})
        assert e.value.response.status_code == 400

        with pytest.raises(TypeError):
            # does not match the signature of history_batched_post.get_history (missing start/end)
            fetch_history(asset_id="", request_kwargs={"json": history_json})

    finally:
        server.clear()


def test_ts_regression(httpserver):
    data = [{"ts": datetime(2000, 1, 1, 0, 0), "value": 0.0}]

    handler = handler_factory(data)
    request_json = make_request_json(data)

    httpserver.expect_request("/history", method="GET").respond_with_handler(handler)
    get_history = history_endpoint(httpserver.url_for(""), path="history")

    resp = get_history(json=request_json)
    resp_values = TypeAdapter(list[dict[str, datetime | float]]).validate_json(
        resp.content
    )

    assert resp_values == data
