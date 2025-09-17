import functools
import logging
from dataclasses import dataclass, field
from functools import wraps

import requests

logger = logging.getLogger(__name__)


@dataclass
class API:
    url: str
    middleware: list = field(default_factory=list)

    def _prepare_request(self, method: str, **kwargs) -> requests.PreparedRequest:
        """
        Build a Request by applying all middleware
        """
        req = requests.Request(method, **kwargs)
        req = functools.reduce(
            lambda acc, f: f.process_request(acc), self.middleware, req
        )
        return req.prepare()

    def get(self, path, **requests_kwargs):
        def decorator_get(f):
            @wraps(f)
            def wrapper_get(*, params: dict | None = None, **kwargs):
                formatted_path = path.format(**kwargs)
                url = self.url + formatted_path

                prepared_req = self._prepare_request(
                    "get", url=url, params=params, **requests_kwargs
                )
                logger.debug(f"{prepared_req.headers=}")
                with requests.Session() as session:
                    resp = session.send(prepared_req)

                resp.raise_for_status()

                return f(resp)

            return wrapper_get

        return decorator_get

    def post(self, path, **requests_kwargs):
        def decorator_post(f):
            @wraps(f)
            def wrapper_post(*args, **kwargs):
                url = self.url + path
                post_json = f(*args, **kwargs)
                logger.debug(f"Posting to {url} with data: {post_json}")
                logger.debug(f"Request kwargs: {requests_kwargs}")

                prepared_req = self._prepare_request(
                    "post", url=url, json=post_json, **requests_kwargs
                )
                with requests.Session() as session:
                    resp = session.send(prepared_req)

                resp.raise_for_status()
                return resp.json()

            return wrapper_post

        return decorator_post
