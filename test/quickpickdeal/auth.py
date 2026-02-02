from datetime import datetime

from pydantic import BaseModel, Field

from sill import BaseAuthToken, TokenEndpoint


class QuickPickDealToken(BaseModel):
    token: str = Field(validation_alias="Token")
    created: datetime = Field(validation_alias="Created")
    expires: datetime = Field(validation_alias="Expires")


class QuickPickDealResponse(BaseModel):
    success: bool = Field(validation_alias="Success")
    error: str | None = Field(validation_alias="Error")
    data: QuickPickDealToken = Field(validation_alias="Data")


class QuickPickDealTokenEndpoint(TokenEndpoint):
    def to_base_token(self, resp):
        resp.raise_for_status()
        resp_token = QuickPickDealResponse(**resp.json()).data
        return BaseAuthToken(token=resp_token.token, valid_until=resp_token.expires)
