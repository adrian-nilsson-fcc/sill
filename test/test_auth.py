import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from hypothesis import given, settings
from hypothesis import strategies as st

from sill import BaseAuthToken, BearerTokenMiddleware


@st.composite
def token_dict_strategy(draw) -> dict[str]:
    valid_until_keys = st.sampled_from(["valid_until", "validUntil", "ValidUntil"])

    token = str(draw(st.uuids()))
    valid_until_key = draw(valid_until_keys)
    valid_until_val = draw(st.datetimes())

    return {"token": token, valid_until_key: valid_until_val}


@given(token_dict_strategy())
def test_validate_token(token):
    print(f"{token=}")
    BaseAuthToken.model_validate(token)


@given(n_threads=st.integers(min_value=2, max_value=8))
@settings(deadline=None)
def test_bearer_token_refresh_is_mutexed(n_threads):
    """Ensure concurrent requests trigger only one token refresh.

    This test launches multiple workers at the same time and asserts that only a
    single `get_token()` call occurs. All workers should reuse the refreshed
    token rather than racing to fetch their own.
    """

    class CountingTokenEndpoint:
        def __init__(self) -> None:
            self.call_count = 0
            self._lock = threading.Lock()

        def get_token(self) -> BaseAuthToken:
            with self._lock:
                self.call_count += 1

            return BaseAuthToken(
                token=str(uuid4()),
                valid_until=datetime.now(timezone.utc) + timedelta(minutes=5),
            )

    endpoint = CountingTokenEndpoint()
    middleware = BearerTokenMiddleware(token_parser=endpoint)
    barrier = threading.Barrier(n_threads)

    def worker():
        barrier.wait()
        return middleware.process_request()

    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        results = list(executor.map(lambda _: worker(), range(n_threads)))

    assert endpoint.call_count == 1, "multiple token refreshes occurred concurrently"
    assert all(
        result["headers"]["Authorization"].startswith("Bearer ") for result in results
    )
