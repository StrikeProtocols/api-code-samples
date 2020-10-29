from random import randint

from ..exchange_api.client import Client

from .custodians import select_enabled_custodian
from .symbols import get_symbols_supported_by_custodian


def create_and_onboard_customer(client: Client, name: str, custodian_id: str):
    customer = client.sandbox_create_customer(name, [custodian_id])
    client.request_customer_onboarding(
        customer['identifier'],
        customer['name'],
        custodian_id,
        customer['FIXAccountIdentifier'],
    )
    client.sandbox_accept_customer_onboarding_request(customer['identifier'])
    return customer


def test_customer_and_sandbox_methods(client: Client):
    custodian = select_enabled_custodian(client)
    symbols = get_symbols_supported_by_custodian(client, custodian['identifier'])

    customer = create_and_onboard_customer(client, f'Customer {randint(1, 1e9)}', custodian['identifier'])

    client.sandbox_create_customer_deposit(customer['identifier'], '100', symbols[0])
    client.sandbox_create_customer_deposit(customer['identifier'], '500', symbols[1])
    # cannot terminate this customer yet since it still has funds deposited
    client.sandbox_terminate_customer(customer['identifier'], expected_status_code=422)
    # simulate customer requesting a withdrawal, and approve this request
    client.sandbox_create_customer_withdrawal_request(customer['identifier'], [
        ('30', symbols[0]),
        ('150', symbols[1]),
    ])
    withdrawal_requests = client.list_customer_withdrawal_requests(customer['identifier'])
    client.process_customer_withdrawal_request(customer['identifier'], withdrawal_requests[0]['identifier'])
    # withdraw the rest of the funds for the client
    client.create_customer_withdrawal(customer['identifier'], [
        ('70', symbols[0]),
        ('350', symbols[1]),
    ])
    client.wait_for_customer_withdrawals_to_complete(customer['identifier'])
    # now we can safely terminate this customer
    client.sandbox_terminate_customer(customer['identifier'])
