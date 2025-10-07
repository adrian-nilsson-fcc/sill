from hypothesis import given
from hypothesis import strategies as st

from sill import BaseAuthToken


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
