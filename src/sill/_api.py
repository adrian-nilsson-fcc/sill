import functools
import logging
from dataclasses import dataclass, field
from functools import wraps

import requests

logger = logging.getLogger(__name__)


@dataclass
class API:
    url: str
    middleware: list[object] = field(default_factory=list)

    def _apply_request_middleware(self, **request_kwargs) -> dict[str]:
        """
        Apply all middleware with a process_request method

        :param **request_kwargs: arguments to requests.request that middleware may alter
        """
        request_altering_middleware = filter(
            lambda m: hasattr(m, "process_request") and callable(m.process_request),
            self.middleware,
        )

        req_kwargs = functools.reduce(
            lambda acc, f: f.process_request(**acc),
            request_altering_middleware,
            request_kwargs,
        )
        return req_kwargs

    def get(self, path: str, **request_glob_kwargs):
        def decorator_get(f):
            @wraps(f)
            def wrapper_get(*, path_format: dict[str] | None = None, **request_kwargs):
                formatted_path = (
                    path if path_format is None else path.format(**path_format)
                )
                url = self.url + formatted_path

                request_glob_kwargs["method"] = "GET"
                request_glob_kwargs["url"] = url
                # mimic requests' behavior
                request_glob_kwargs.setdefault("allow_redirects", True)

                # middleware may alter the endpoint-specific request arguments
                after_middleware_kwargs = self._apply_request_middleware(
                    **request_glob_kwargs
                )
                logger.debug(
                    f"request headers: {after_middleware_kwargs.get('headers')}"
                )

                # call-site arguments has highest precedence
                request_kwargs = after_middleware_kwargs | request_kwargs
                resp = requests.request(**request_kwargs)

                resp.raise_for_status()

                return f(resp)

            return wrapper_get

        return decorator_get

    def post(self, path, **request_glob_kwargs):
        def decorator_post(f):
            @wraps(f)
            def wrapper_post(*args, request_kwargs: dict[str], **kwargs):
                url = self.url + path
                request_glob_kwargs["method"] = "POST"
                request_glob_kwargs["url"] = url

                post_json = f(*args, **kwargs)
                logger.debug(f"Posting to {url} with data: {post_json}")

                # middleware may alter any endpoint-specific request arguments
                after_middleware_kwargs = self._apply_request_middleware(
                    json=post_json, **request_glob_kwargs
                )

                # call-site arguments has highest precedence
                request_kwargs = after_middleware_kwargs | request_kwargs
                logger.debug(f"final request kwargs: {request_glob_kwargs}")
                resp = requests.request(**request_kwargs)

                resp.raise_for_status()

                return resp.json()

            return wrapper_post

        return decorator_post
