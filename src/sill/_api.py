from dataclasses import dataclass
from functools import wraps
import json
import logging
import tomllib
from pathlib import Path
from typing import Self

import requests

from . import _auth


logger = logging.getLogger(__name__)


@dataclass
class API:
    cert: Path | str
    token: _auth.Token | None
    url: str
    user: dict[str, str]

    @classmethod
    def from_config(cls, config_path: str) -> Self:
        conf = tomllib.loads(Path(config_path).read_text())

        cert = Path(conf["auth"]["cert"])
        if not cert.is_file():
            raise ValueError(f"{cert} is not a file")
        elif cert.suffix != ".pem":
            raise ValueError(
                f"Expected the cert to be a .pem file, found {cert.suffix}"
            )

        credentials_file = Path(conf["auth"]["credentials"])
        credentials = json.loads(credentials_file.read_bytes())
        missing_keys = {"username", "password"} - set(credentials.keys())
        if len(missing_keys) != 0:
            raise ValueError(
                f"The user credentials file {credentials_file} is missing required keys: {missing_keys}"
            )

        api = cls(
            cert=cert,
            token=None,
            url=conf["api"]["url"],
            user=credentials,
        )

        token_file: str = conf["auth"].get("token")
        if token_file is not None:
            token_file = Path(token_file)
            api._token_file = token_file
            if token_file.is_file():
                api.token = _auth.Token.from_file(token_file)

        return api

    def _refresh_token(self) -> None:
        logger.debug("Requesting a fresh auth token")
        new_token = _auth.request_token(self.url, self.user, self.cert)
        self.token = new_token
        if hasattr(self, "_token_file"):
            self._token_file.write_text(new_token.to_json())

    def _auth_kwargs(self) -> dict[str, str]:
        if self.token is None or self.token.is_valid() == False:
            self._refresh_token()

        return _auth.auth_kwargs(self.cert, self.token.token)

    def get(self, path, **requests_kwargs):
        def decorator_get(f):
            @wraps(f)
            def wrapper_get():
                url = self.url + path
                request_kwargs = self._auth_kwargs() | requests_kwargs
                resp = requests.get(url, **request_kwargs)
                resp.raise_for_status()

                return f(resp)

            return wrapper_get

        return decorator_get

    def post(self, path, **requests_kwargs):
        def decorator_post(f):
            @wraps(f)
            def wrapper_post(*args, **kwargs):
                url = self.url + path
                request_kwargs = self._auth_kwargs() | requests_kwargs

                post_json = f(*args, **kwargs)
                logger.debug(f"Posting to {url} with data: {post_json}")
                logger.debug(f"Request kwargs: {request_kwargs}")

                resp = requests.post(url, json=post_json, **request_kwargs)
                resp.raise_for_status()

                return resp.json()

            return wrapper_post

        return decorator_post
