import argparse
import time

from exchange_api.client import Client

from .custodians import test_custodians
from .customer import test_customer_and_sandbox_methods
from .settlement import test_settlement_plans_and_settlement
from .trades import test_trades
from .webhook import test_webhooks


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='prints out the jobs and transfers')
    parser.add_argument('--key', required=True, help="api key")
    parser.add_argument('--secret', required=True, help="api secret")
    parser.add_argument('--url', required=True, help="Strike exchange api url")
    parser.add_argument('--signing-key-file', required=True, help="Signing key that is used to sign settlement flows")
    parser.add_argument('--sandbox-url', required=False, help="Strike exchange api sandbox url")
    args = parser.parse_args()

    client = Client(args.key, args.secret, args.url, args.signing_key_file, sandbox_url=args.sandbox_url, debug=True)
    client.counter_nonce = int(time.time()) + 10000

    test_custodians(client)
    test_customer_and_sandbox_methods(client)
    test_webhooks(client)
    test_trades(client)
    test_settlement_plans_and_settlement(client)
    print("All tests completed successfully!")
