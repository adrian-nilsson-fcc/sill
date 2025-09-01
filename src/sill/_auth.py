from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Self

import requests


logger = logging.getLogger(__name__)


@dataclass
class Token:
    token: str
    valid_until: datetime

    def is_valid(self):
        return self.valid_until > datetime.now(timezone.utc)

    @classmethod
    def from_file(cls, json_file: Path | str) -> Self:
        file = Path(json_file)
        new = cls.from_json(json.loads(file.read_text()))
        logger.debug(f"Token from {file.stem}: {new}")
        new.__token_file = file

        return new

    @classmethod
    def from_json(cls, json: dict[str, str]) -> Self:
        logger.debug(f"Token validity: {json['validUntil']}")
        valid_until_dt = datetime.fromisoformat(json["validUntil"])

        return cls(
            token=json["token"],
            valid_until=valid_until_dt,
        )

    def to_json(self) -> str:
        return json.dumps(
            {
                "token": self.token,
                "validUntil": self.valid_until.isoformat(),
            }
        )


def request_token(url, user: dict, cert_file: Path) -> Token:
    endpoint = url + "auth/"
    response = requests.get(endpoint, json=user, cert=str(cert_file))
    response.raise_for_status()

    return Token.from_json(response.json())


def auth_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def auth_kwargs(cert_file: Path | str, token: str) -> dict[str, str]:
    return {
        "cert": str(cert_file),
        "headers": auth_header(token),
    }
