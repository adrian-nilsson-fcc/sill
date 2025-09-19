from sill import API, BearerTokenMiddleware

from .auth import QuickPickDealTokenEndpoint

BASE_URL = "https://www.quickpickdeal.com/api/"

token_endpoint = QuickPickDealTokenEndpoint(BASE_URL + "Auth/GetBearerToken")

quickpickdeal = API(
    url=BASE_URL,
    middleware=[BearerTokenMiddleware(token_endpoint, token_file="test_token.json")],
)


@quickpickdeal.get(path="Customer/GetAllCustomers")
def list_customers(resp):
    return resp.json()


@quickpickdeal.get(path="Customer/GetCustomerById")
def get_customer(resp):
    """
    Expects query parameter 'id'
    """
    return resp.json()


@quickpickdeal.post(path="Customer/CreateCustomer")
def create_customer(email: str, first_name: str, last_name: str):
    return {"email": email, "FirstName": first_name, "LastName": "Doe"}


__all__ = ["list_customers", "get_customer"]
