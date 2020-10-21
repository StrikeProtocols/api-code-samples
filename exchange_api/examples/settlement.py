from datetime import datetime
from random import randint

import pytz

from exchange_api.client import Client

from .custodians import select_enabled_custodian
from .customer import create_and_onboard_customer
from .symbols import get_symbols_supported_by_custodian
from .trades import submit_two_trades


def test_settlement_plans_and_settlement(client: Client):
    custodian = select_enabled_custodian(client)

    symbols = get_symbols_supported_by_custodian(client, custodian['identifier'])
    customer1 = create_and_onboard_customer(client, f'Customer {randint(1, 1e9)} For Settlement 1', custodian['identifier'])
    customer2 = create_and_onboard_customer(client, f'Customer {randint(1, 1e9)} For Settlement 2', custodian['identifier'])

    trade1, trade2 = submit_two_trades(client, f'trade_id1_{randint(1, 1e9)}', f'trade_id2_{randint(1, 1e9)}', customer1['identifier'], symbols)
    trade3, trade4 = submit_two_trades(client, f'trade_id1_{randint(1, 1e9)}', f'trade_id2_{randint(1, 1e9)}', customer2['identifier'], symbols)

    settlement_plan1 = client.create_settlement_plan(
        custodian['identifier'],
        [trade1['identifier'], trade3['identifier']])

    client.modify_trades_in_settlement_plan(
        settlement_plan1['identifier'],
        add_trades=[trade2['identifier'], trade4['identifier']]
    )

    client.remove_customer_from_settlement_plan(settlement_plan1['identifier'], customer1['identifier'])
    assert set(client.get_settlement_plan(settlement_plan1['identifier'])['tradeIdentifiers']) == \
           {trade3['identifier'], trade4['identifier']}
    client.remove_customer_from_settlement_plan(settlement_plan1['identifier'], customer2['identifier'])
    assert client.get_settlement_plan(settlement_plan1['identifier'])['tradeIdentifiers'] == []
    # can now cancel the settlement plan since there are no trades in it
    client.cancel_settlement_plan(settlement_plan1['identifier'])

    settlement_plan2 = client.create_settlement_plan(
        custodian['identifier'],
        [trade1['identifier'], trade2['identifier'], trade3['identifier'], trade4['identifier']])

    timestamp_before_settlement = datetime.now(pytz.utc)

    # this should fail since neither of the counterparties is funded
    client.request_settlement(settlement_plan2, expected_status_code=422)

    for symbol in symbols[:2]:
        # fund counterparties
        client.sandbox_create_customer_deposit(customer1['identifier'], '1000', symbol)
        client.sandbox_create_customer_deposit(customer2['identifier'], '1000', symbol)
        # fund venue
        client.sandbox_create_custodian_deposit(custodian['identifier'], '1000', symbol)
    settlement = client.request_settlement(settlement_plan2)

    # notice that settlement identifier matches identifier of the settlement plan that it originated from
    assert settlement['identifier'] == settlement_plan2['identifier']

    assert len(client.list_settlements(from_dt=timestamp_before_settlement)) == 1

    # settlement plan does not exist anymore since it was converted to settlement
    client.get_settlement_plan(settlement_plan2['identifier'], expected_status_code=404)
