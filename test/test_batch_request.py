import json
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from itertools import chain, dropwhile, takewhile
from operator import itemgetter

from hypothesis import given
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
        start = datetime.fromisoformat(request_json["from"])
        end = request_json.get("to")

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


def history_endpoint(base_url: str, path: str):
    api = sill.API(url=base_url)

    @api.get(path=path)
    def get_history(resp):
        return resp

    return get_history


def history_batched_endpoint(base_url: str, path: str, chunk_size: timedelta):
    api = sill.API(url=base_url)

    @sill.utils.batched(start_arg="from", end_arg="to", chunk_size=chunk_size)
    @api.get(path=path)
    def get_history(resp):
        return resp

    return get_history


@given(ts=st_timeseries)
def test_batched_server(ts, make_httpserver):
    server = make_httpserver
    handler = handler_factory(ts)

    request_payload = {"from": ts[0]["ts"]}
    if len(ts) > 1:
        request_payload["to"] = ts[-1]["ts"]

    try:
        server.expect_request("/history", method="GET").respond_with_handler(handler)
        get_history = history_endpoint(server.url_for(""), path="history")

        resp = get_history(json=to_jsonable_python(request_payload))
        resp_values = TypeAdapter(list[dict[str, datetime | float]]).validate_json(
            resp.content
        )
        assert resp_values == ts
    finally:
        server.clear()


@given(
    ts=st_timeseries.filter(lambda ts: len(ts) > 1),
    n_chunks=st.integers(min_value=2, max_value=10),
)
def test_batched_request(ts, n_chunks, make_httpserver):
    server = make_httpserver
    handler = handler_factory(ts)

    request_payload = {"from": ts[0]["ts"], "to": ts[-1]["ts"]}
    chunk_size = (ts[-1]["ts"] - ts[0]["ts"]) / n_chunks

    try:
        server.expect_request("/history", method="GET").respond_with_handler(handler)
        get_history = history_batched_endpoint(
            server.url_for(""), path="history", chunk_size=chunk_size
        )

        resp = get_history(json=to_jsonable_python(request_payload))
        assert (
            len(resp) >= n_chunks - 1 and len(resp) <= n_chunks + 1
        )  # rounding can cause this to be off-by-one

        resp_validator = TypeAdapter(list[dict[str, datetime | float]])
        resp_json = [
            resp_validator.validate_json(r.content) for r in resp if r.content != b"[]"
        ]

        resp_values = list(chain.from_iterable(resp_json))
        assert resp_values == ts
    finally:
        server.clear()


def test_ts_regression(httpserver):
    data = [{"ts": datetime(2000, 1, 1, 0, 0), "value": 0.0}]

    handler = handler_factory(data)
    request_payload = {"from": data[0]["ts"]}

    httpserver.expect_request("/history", method="GET").respond_with_handler(handler)
    get_history = history_endpoint(httpserver.url_for(""), path="history")

    resp = get_history(json=to_jsonable_python(request_payload))
    resp_values = TypeAdapter(list[dict[str, datetime | float]]).validate_json(
        resp.content
    )

    assert resp_values == data
