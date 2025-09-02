from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Self

from pydantic import BaseModel
import requests

logger = logging.getLogger(__name__)


class BaseAuthToken(BaseModel):
    token: str
    valid_until: datetime

    @classmethod
    def from_file(cls, json_file: Path | str) -> Self:
        file = Path(json_file)
        new = cls.model_validate_json(file.read_text())
        logger.debug(f"read token from {file.stem}: {new}")

        return new


@dataclass
class TokenEndpoint(ABC):
    endpoint: str

    @abstractmethod
    def to_base_token(self, resp) -> BaseAuthToken:
        raise NotImplemented()

    def request_token(self) -> requests.Response:
        return requests.get(self.endpoint)

    def get_token(self) -> BaseAuthToken:
        resp = self.request_token()
        return self.to_base_token(resp)


class BaseAuthTokenMiddleware:
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

    def is_valid(self):
        return self.token_model.valid_until > datetime.now(timezone.utc)

    def process_request(self, req: requests.Request) -> requests.Request:
        if self.token_model is None or self.is_valid() == False:
            self._refresh_token()

        logger.debug("Adding auth token to request header")
        req.headers["Authorization"] = f"Bearer {self.token_model.token}"
        return req

    def _refresh_token(self) -> None:
        new_token = self.token_parser.get_token()

        # update (create or overwrite) token on file, if set
        if self.token_file:
            self.token_file.write_text(new_token.model_dump_json())

        self.token_model = new_token
