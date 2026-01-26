import logging

import pytest
from quickpickdeal.api import create_customer, get_customer, list_customers

pytest.skip("Skipping this test file", allow_module_level=True)

logging.basicConfig(level=logging.DEBUG)


def test_list_customers():
    resp = list_customers()
    assert resp["Error"] is None


def test_get_customer():
    resp = get_customer(params={"id": 2})
    assert resp["Error"] is None


def test_create_customer():
    resp = create_customer(
        email="test@company.org", first_name="Jessie", last_name="Doe"
    )

    assert resp["Success"]
    assert resp["Error"] is None
