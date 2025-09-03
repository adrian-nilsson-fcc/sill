import logging

logging.basicConfig(level=logging.DEBUG)

from quickpickdeal.api import *


def test_list_customers():
    resp = list_customers()
    assert resp["Error"] is None


def test_get_customer():
    resp = get_customer(params={"id": 2})
    assert resp["Error"] is None
