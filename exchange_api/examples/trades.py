from datetime import datetime, timedelta
from decimal import Decimal
from random import randint

import pytz

from exchange_api.client import Client

from .custodians import select_enabled_custodian
from .customer import create_and_onboard_customer
from .symbols import get_symbols_supported_by_custodian


def submit_two_trades(client, trade_id1, trade_id2, counterparty_customer_id, symbols):
    return (client.submit_trade(
        trade_id=trade_id1,
        side='Buy',
        base_symbol=symbols[0],
        term_symbol=symbols[1],
        dealt='10',
        rate='5',
        counter='50',
        counterparty_id=counterparty_customer_id,
        liquidity_indicator=None,
        venue_fee='0',
        venue_fee_symbol=None,
        notes=None,
        execution_date=datetime.now(pytz.utc) - timedelta(days=1),
    ),  client.submit_trade(
        trade_id=trade_id2,
        side='Sell',
        base_symbol=symbols[0],
        term_symbol=symbols[1],
        dealt='40',
        rate='6',
        counter='240',
        counterparty_id=counterparty_customer_id,
        liquidity_indicator=None,
        venue_fee='0',
        venue_fee_symbol=None,
        notes=None,
        execution_date=datetime.now(pytz.utc),
    ))


def test_trades(client: Client):
    custodian = select_enabled_custodian(client)

    symbols = get_symbols_supported_by_custodian(client, custodian['identifier'])
    customer = create_and_onboard_customer(client, f'Customer For Trades {randint(1, 1e9)}', custodian['identifier'])

    start_time = datetime.now(pytz.utc)
    trade1, trade2 = submit_two_trades(client, f'trade_id{randint(1, 1e9)}', f'trade_id{randint(1, 1e9)}', customer['identifier'], symbols)

    assert len(client.list_trades(from_dt=(start_time))['trades']) == 1

    client.update_trade(trade2, dealt='30')

    assert Decimal(client.get_trade(trade2['identifier'])['dealt']) == Decimal('30')

    client.cancel_trade(trade1['identifier'])
    client.cancel_trade(trade2['identifier'])

    # trades are still listed but now with `Canceled` status
    assert all([
        trade['status'] == 'Canceled'
        for trade in client.list_trades()['trades'] if trade['identifier'] in (trade1['identifier'], trade2['identifier'])])

