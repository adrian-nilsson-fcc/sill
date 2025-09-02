import logging

logging.basicConfig(level=logging.DEBUG)

from quickpickdeal.api import *


def test_list_customers():
    resp = list_customers()
    assert resp["Error"] is None


def test_list_customer():
    resp = list_customer()
    assert resp["Error"] is None
