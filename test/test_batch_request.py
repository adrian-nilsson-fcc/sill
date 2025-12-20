import json
from contextlib import contextmanager
from datetime import datetime
from itertools import dropwhile, takewhile
from operator import itemgetter

from hypothesis import given
from hypothesis import strategies as st
from pydantic import TypeAdapter
from pydantic_core import to_jsonable_python
from pytest_httpserver import HTTPServer
from werkzeug import Request, Response

import sill

ts_getter = itemgetter("ts")
st_observations = st.fixed_dictionaries(
    {
        "ts": st.datetimes(),
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

    @api.get(path="history")
    def get_history(resp):
        return resp

    return get_history


@given(ts=st_timeseries)
def test_batched_request_full(ts, make_httpserver):
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
