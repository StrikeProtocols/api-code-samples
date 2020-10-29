from datetime import datetime
from decimal import Decimal
from time import time, sleep

import pytz

from ..exchange_api.client import Client
from ..exchange_api.models import WithdrawalDestinationType, SymbolType, CustodianStatus, TransferStatus

from .symbols import get_symbols_supported_by_custodian


def select_enabled_custodian(client: Client):
    return next(custodian for custodian in client.list_custodians()
                if custodian['status'] == CustodianStatus.Enabled.name)


def wait_for_deposit_to_complete(client: Client, custodian_id: str, from_dt: datetime, timeout_seconds: int = 90):
    start = time()
    while True:
        deposit = client.list_custodian_deposits(custodian_id, from_dt=from_dt)[0]
        if deposit['status'] == TransferStatus.Completed.name:
            break
        if time() - start > timeout_seconds:
            raise TimeoutError(f'Timed out waiting for deposit to complete. Current deposit: {deposit}')
        sleep(0.5)


def wait_for_withdrawal_to_complete(client: Client, custodian_id: str, from_dt: datetime, timeout_seconds: int = 90):
    start = time()
    while True:
        withdrawal = client.list_custodian_withdrawals(custodian_id, from_dt=from_dt)[0]
        if withdrawal['status'] == TransferStatus.Completed.name:
            break
        if time() - start > timeout_seconds:
            raise TimeoutError(f'Timed out waiting for withdrawal to complete. Current withdrawal: {withdrawal}')
        sleep(0.5)


def assert_custodian_balance_change(client: Client, custodian, symbol: str, delta: str):
    try:
        old_balance = next(balance['amount'] for balance in custodian['balance']
                           if balance['symbol'] == symbol)
    except StopIteration:
        old_balance = 0
    current_balance = next(balance['amount'] for balance in client.get_custodian(custodian['identifier'])['balance']
                           if balance['symbol'] == symbol)
    assert Decimal(current_balance) == Decimal(old_balance) + Decimal(delta)


def test_custodians(client: Client):
    custodian = select_enabled_custodian(client)
    symbols = get_symbols_supported_by_custodian(client, custodian['identifier'], symbol_type=SymbolType.Asset)

    # use this to retrieve deposit instructions for all symbols for this custodian
    client.get_custodian_deposit_instructions(custodian['identifier'])

    timestamp_before_deposit = datetime.now(pytz.utc)
    assert len(client.list_custodian_deposits(custodian['identifier'], from_dt=timestamp_before_deposit)) == 0
    client.sandbox_create_custodian_deposit(custodian['identifier'], '300', symbols[0])
    assert len(client.list_custodian_deposits(custodian['identifier'], from_dt=timestamp_before_deposit)) == 1

    wait_for_deposit_to_complete(client, custodian['identifier'], timestamp_before_deposit)
    assert_custodian_balance_change(client, custodian, symbols[0], '300')

    withdrawal_destination = client.create_withdrawal_destination(
        custodian['identifier'], 'New Withdrawal Destination', WithdrawalDestinationType.Crypto,
        symbol=symbols[0], wallet_address='test_wallet_address')
    assert client.get_custodian_withdrawal_destination(
        custodian['identifier'], withdrawal_destination['identifier']
    )['address'] == 'test_wallet_address'

    timestamp_before_withdrawal = datetime.now(pytz.utc)
    assert len(client.list_custodian_withdrawals(custodian['identifier'], from_dt=timestamp_before_withdrawal)) == 0
    client.request_custodian_withdrawal(
        custodian['identifier'], withdrawal_destination['identifier'], '100', symbols[0])
    assert len(client.list_custodian_withdrawals(custodian['identifier'], from_dt=timestamp_before_withdrawal)) == 1

    wait_for_withdrawal_to_complete(client, custodian['identifier'], timestamp_before_withdrawal)
    assert_custodian_balance_change(client, custodian, symbols[0], '200')
