from sill import API, BaseAuthTokenMiddleware

from .auth import QuickPickDealTokenEndpoint

BASE_URL = "https://www.quickpickdeal.com/api/"

token_endpoint = QuickPickDealTokenEndpoint(BASE_URL + "Auth/GetBearerToken")

quickpickdeal = API(
    url=BASE_URL,
    middleware=[BaseAuthTokenMiddleware(token_endpoint, token_file="test_token.json")],
)


@quickpickdeal.get(path="Customer/GetAllCustomers")
def list_customers(resp):
    return resp.json()


# TODO:  deal with query params
@quickpickdeal.get(path="Customer/GetCustomerById?id=2")
def list_customer(resp):
    return resp.json()
