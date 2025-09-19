import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Self

import requests
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class BaseAuthToken(BaseModel):
    token: str
    valid_until: datetime

    @classmethod
    def from_file(cls, json_file: Path | str) -> Self:
        file = Path(json_file)
        new = cls.model_validate_json(file.read_text())
        logger.debug(f"read token from {file.name}: {new}")

        return new


@dataclass
class TokenEndpoint(ABC):
    endpoint: str

    @abstractmethod
    def to_base_token(self, resp) -> BaseAuthToken:
        raise NotImplementedError()

    def request_token(self) -> requests.Response:
        return requests.get(self.endpoint)

    def get_token(self) -> BaseAuthToken:
        resp = self.request_token()
        return self.to_base_token(resp)


class BearerTokenMiddleware:
    def __init__(
        self, token_parser: TokenEndpoint, token_file: Path | str | None = None
    ):
        self.token_parser = token_parser

        self.token_file = None
        if token_file is not None:
            self.token_file: Path = Path(token_file)

        # read a token from file, if it exists
        self.token_model = None
        if self.token_file and self.token_file.is_file():
            self.token_model = BaseAuthToken.from_file(self.token_file)

    def is_valid(self) -> bool:
        valid = self.token_model.valid_until > datetime.now(timezone.utc)
        if not valid:
            logger.info(f"token expired: expired on {self.token_model.valid_until}")

        return valid

    def process_request(self, **request_kwargs) -> dict[str]:
        if self.token_model is None or not self.is_valid():
            self._refresh_token()

        logger.debug("Adding auth token to request header")
        request_kwargs.setdefault("headers", {})
        request_kwargs["headers"]["Authorization"] = f"Bearer {self.token_model.token}"

        return request_kwargs

    def _refresh_token(self) -> None:
        logger.info("requesting a new auth token")
        new_token = self.token_parser.get_token()

        # update (create or overwrite) token on file, if set
        if self.token_file:
            self.token_file.write_text(new_token.model_dump_json())

        self.token_model = new_token
