from base64 import b64encode
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from time import time, sleep
from typing import List, Tuple, Optional, Dict, Union
import os
import re
from uuid import uuid4

from ecdsa import SigningKey, util as ecdsa_util
import pytz
import requests

from .models import WithdrawalDestinationType, WireTransferTargetInfo, TransferStatus


class UnexpectedStatusCode(Exception):
    def __init__(self, message, status_code, json):
        self.message = message
        self.status_code = status_code
        self.json = json


request_type_dict = {
    'GET': requests.get,
    'POST': requests.post,
    'PATCH': requests.patch,
    'DELETE': requests.delete,
}


class Client:
    def __init__(self, key, secret, url, signing_key_file, sandbox_url=None, venue_id=None, api_version='v1', debug=False):
        self.key = key
        self.secret = secret
        self.counter_nonce = 1
        self.url = url
        self.sandbox_url = sandbox_url
        self.api_version = api_version
        self.debug = debug
        self.quanta = Decimal('0.' + '0' * 18)
        self.signing_key = SigningKey.from_pem(open(signing_key_file).read(), hashlib.sha256)
        # if venue id is not supplied, just get it from the current user endpoint
        self.venue_id = venue_id if venue_id else self.get_api_key()['venueIdentifier']

    @staticmethod
    def urljoin(*args):
        return os.path.join(*[a.strip('/') for a in args if a is not None])

    @staticmethod
    def format_boolean(b: Optional[bool]):
        return None if b is None else ('true' if b else 'false')

    @staticmethod
    def format_date(date: Optional[Union[datetime, str]]):
        if date is None or type(date) == str:
            return date
        return date.astimezone(pytz.utc).isoformat(timespec='milliseconds')

    @staticmethod
    def date_from_string(date: str):
        return datetime.strptime(''.join(date.rsplit(':', 1)), '%Y-%m-%dT%H:%M:%S.%f%z')

    def compute_trade_hash(
            self,
            venue_id: str,
            counterparty_id: str,
            trade_id: str,
            side: str,
            base_symbol: str,
            term_symbol: str,
            dealt: str,
            rate: str,
            counter: str,
            execution_date: datetime
    ):
        content = "|".join([
            venue_id,
            counterparty_id,
            trade_id,
            side,
            base_symbol,
            term_symbol,
            str(Decimal(dealt).quantize(self.quanta)),
            str(Decimal(rate).quantize(self.quanta)),
            str(Decimal(counter).quantize(self.quanta)),
            execution_date.isoformat(timespec='milliseconds')])
        trade_hash = hashlib.sha256(str.encode(content)).hexdigest()

        return trade_hash

    def sign(self, to_sign):
        return b64encode(self.signing_key.sign(
            to_sign.encode(), hashfunc=hashlib.sha256, sigencode=ecdsa_util.sigencode_der)).decode()

    def get_digest(self, timestamp, nonce, request_type, route, params, j, idempotent_id):
        params_str = '' if not params else '&'.join(f'{k}={v}' for k, v in params.items() if v is not None)
        json_str = '' if not j else json.dumps(j)
        unencoded_digest = f'{self.key}|{self.secret}|{timestamp}|{nonce}|{request_type}|{route}|' \
                           f'{params_str}|{json_str}|{idempotent_id or ""}'
        return hashlib.sha256(str.encode(unencoded_digest)).hexdigest()

    def get_headers(self, request_type, route, params=None, data=None):
        headers = {'Accept': 'application/json'}

        if request_type != 'GET':
            headers['Content-Type'] = 'application/json'
            headers['X-Idempotency-ID'] = str(uuid4())

        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        nonce = self.counter_nonce
        self.counter_nonce += 1

        digest = self.get_digest(timestamp, nonce, request_type, route, params, data, headers.get('X-Idempotency-ID'))

        headers['Authorization'] = f'HMAC {self.key}|{timestamp}|{nonce}|{digest}'

        return headers

    def url_and_route(self, route_in, sandbox=False):
        route = '/' + self.urljoin(self.api_version, 'sandbox' if sandbox else None, route_in)
        return self.urljoin(self.sandbox_url if sandbox and self.sandbox_url else self.url, route), route

    def process_response(self, response, expected_status_code):
        content = None if not response.text else response.json()
        if self.debug:
            print('\nResponse:')
            print(f'<<< status code: {response.status_code}')
            print(f'<<< content: {content}\n')

        if response.status_code != expected_status_code:
            raise UnexpectedStatusCode(
                f'Got HTTP status {response.status_code} trying to {response.request.method} to {response.url}: {response.text}',
                response.status_code,
                content)

        return content

    def send_request_(self, request_type, route_in, params=None, data=None, sandbox=False, expected_status_code=200):
        url, route = self.url_and_route(route_in, sandbox)
        headers = self.get_headers(request_type, route, params=params, data=data)
        if self.debug:
            print('\nRequest:')
            print(f'>>> {request_type} {url}')
            print(f'>>> headers: {headers}')
            if params:
                print(f'>>> params: {params}')
            if data:
                print(f'>>> data: {data}')
        return self.process_response(
            response=request_type_dict[request_type](url, headers=headers, params=params, json=data),
            expected_status_code=expected_status_code)

    def send_request(self, request_type, route_in, params=None, data=None, sandbox=False, expected_status_code=200):
        try:
            return self.send_request_(request_type, route_in, params, data, sandbox, expected_status_code)
        except UnexpectedStatusCode as e:
            if e.status_code == 401 and e.json:
                match = re.search('The nonce is too low. The highest used nonce is (\d+)', e.json['errors'][0]['message'])
                if match:
                    self.counter_nonce = int(match.group(1)) + 1
                    return self.send_request_(request_type, route_in, params, data, sandbox, expected_status_code)
            raise e

    def get(self, route_in, params=None, expected_status_code=200):
        return self.send_request(
            'GET', route_in, params=params, expected_status_code=expected_status_code)

    def post(self, route_in, data=None, sandbox=False, expected_status_code=200):
        return self.send_request(
            'POST', route_in, data=data, sandbox=sandbox, expected_status_code=expected_status_code)

    def delete(self, route_in, sandbox=False, expected_status_code=204):
        return self.send_request(
            'DELETE', route_in, sandbox=sandbox, expected_status_code=expected_status_code)

    def patch(self, route_in, data, expected_status_code=200):
        return self.send_request(
            'PATCH', route_in, data=data, expected_status_code=expected_status_code)

    def wait_for_customer_withdrawals_to_complete(self, customer_id: str, timeout_seconds: int = 10):
        start = time()
        while True:
            withdrawals = self.list_customer_withdrawals(customer_id)
            if all([withdrawal['status'] == TransferStatus.Completed.name for withdrawal in withdrawals]):
                break
            if time() - start > timeout_seconds:
                raise TimeoutError(f'Timed out waiting for withdrawals. Current customer withdrawals: {withdrawals}')
        sleep(0.1)

    def sandbox_create_customer(
            self,
            name: str,
            allowed_custodians: List[str],
            domicile: str = 'US',
            fix_account_identifier: Optional[str] = None,
            **kwargs
    ):
        """ Usage: client.sandbox_create_customer(
            "Customer Name",
            ["custodian"],
            domicile="AU",                  # optional, defaults to US
            fix_account_identifier="FIX-ID" # optional
        )
        """
        return self.post('customers', data={
            'name': name,
            'allowedCustodians': allowed_custodians,
            'domicile': domicile,
            'FIXAccountIdentifier': fix_account_identifier
        }, sandbox=True, **kwargs)

    def sandbox_terminate_customer(self, customer_id: str, **kwargs):
        """ Usage: client.sandbox_terminate_customer("123456") """
        return self.delete(f'customers/{customer_id}', sandbox=True, **kwargs)

    def sandbox_activate_customer_onboarding_request(self, customer_id: str, **kwargs):
        """ Usage: client.sandbox_activate_customer_onboarding_request("123456") """
        return self.post(f'customers/{customer_id}/accept', sandbox=True, **kwargs)

    def sandbox_reject_customer_onboarding_request(self, customer_id: str, **kwargs):
        """ Usage: client.sandbox_reject_customer_onboarding_request("123456") """
        return self.post(f'customers/{customer_id}/reject', sandbox=True, **kwargs)

    def sandbox_create_customer_deposit(self, customer_id: str, amount: str, symbol: str, **kwargs):
        """ Usage: client.sandbox_create_customer_deposit(
            "123456", "123.45", "USD"
        )
        """
        return self.post(f'customers/{customer_id}/deposits', data={
            'amount': amount,
            'symbol': symbol,
        }, sandbox=True, **kwargs)

    def sandbox_create_customer_withdrawal_request(
            self,
            customer_id: str,
            requested_withdrawals: List[Tuple[str, str]],
            **kwargs
    ):
        """ Usage: client.sandbox_create_customer_withdrawal_request(
            "123456", [("123.45", "USD"), ("0.87654321", "XBT")]
        )
        """
        return self.post(f'customers/{customer_id}/withdrawal-requests', data={
            'requested': [{'amount': rw[0], 'symbol': rw[1]} for rw in requested_withdrawals]
        }, sandbox=True, **kwargs)

    def sandbox_create_custodian_deposit(self, custodian_id: str, amount: str, symbol: str, **kwargs):
        """ Usage: client.sandbox_create_custodian_deposit(
            "custodian", "123.45", "USD"
        )
        """
        return self.post(f'custodians/{custodian_id}/deposits', data={
            'amount': amount,
            'symbol': symbol,
        }, sandbox=True, **kwargs)

    def get_api_key(self, **kwargs):
        """ Usage: client.get_api_key() """
        return self.get('api-key', **kwargs)

    def list_symbols(self, **kwargs):
        """ Usage: client.list_symbols() """
        return self.get('symbols', **kwargs)

    def request_customer_onboarding(
            self,
            customer_id: str,
            name: str,
            custodian_id: str,
            fix_account_identifier: Optional[str] = None,
            **kwargs
    ):
        """ Usage: client.request_customer_onboarding(
            "123456",
            "Customer Name",
            "custodian",
            fix_account_identifier="FIX-ID" # optional
        )
        """
        return self.post(f'customers/{customer_id}/onboard', data={
            'name': name,
            'custodian': custodian_id,
            'FIXAccountIdentifier': fix_account_identifier
        }, **kwargs)

    def get_customer(self, customer_id: str, **kwargs):
        """ Usage: client.get_customer("123456") """
        return self.get(f'customers/{customer_id}', **kwargs)

    def list_customers(self, **kwargs):
        """ Usage: client.list_customers() """
        return self.get('customers', **kwargs)

    def change_customer(
            self,
            customer_id: str,
            custodian_id: Optional[str] = None,
            fix_account_identifier: Optional[str] = None,
            **kwargs
    ):
        """ Usage: client.change_customer(
            "123456",
            custodian_id="custodian",       # optional
            fix_account_identifier="FIX-ID" # optional
        )
        """
        return self.patch(f'customers/{customer_id}', data={
            'custodian': custodian_id,
            'FIXAccountIdentifier': fix_account_identifier,
        }, **kwargs)

    def list_customer_deposits(
            self,
            customer_id: str,
            from_dt: Optional[Union[datetime, str]] = None,
            to_dt: Optional[Union[datetime, str]] = None,
            **kwargs
    ):
        """ Usage: client.list_customer_deposits(
            "123456",
            from_dt=datetime.now(), # optional
            to_dt=datetime.now()    # optional
        )
        """
        return self.get(f'customers/{customer_id}/deposits', params={
            'from': self.format_date(from_dt),
            'to': self.format_date(to_dt),
        }, **kwargs)

    def list_customer_withdrawals(
            self,
            customer_id: str,
            from_dt: Optional[Union[datetime, str]] = None,
            to_dt: Optional[Union[datetime, str]] = None,
            **kwargs
    ):
        """ Usage: client.list_customer_withdrawals(
            "123456",
            from_dt=datetime.now(), # optional
            to_dt=datetime.now()    # optional
        )
        """
        return self.get(f'customers/{customer_id}/withdrawals', params={
            'from': self.format_date(from_dt),
            'to': self.format_date(to_dt)
        }, **kwargs)

    def list_customer_withdrawal_requests(self, customer_id: str, **kwargs):
        """ Usage: client.list_customer_withdrawal_requests("123456") """
        return self.get(f'customers/{customer_id}/withdrawal-requests', **kwargs)

    def process_customer_withdrawal_request(self, customer_id: str, withdrawal_request_id: str, **kwargs):
        """ Usage: client.process_customer_withdrawal_request(
            "123456", "6d8fa93e-fefc-47f7-8cf6-2d2b58a9a35c"
        )
        """
        return self.post(f'customers/{customer_id}/withdrawal-requests/{withdrawal_request_id}/process', **kwargs)

    def reject_customer_withdrawal_request(self, customer_id: str, withdrawal_request_id: str, **kwargs):
        """ Usage: client.reject_customer_withdrawal_request(
            "123456", "6d8fa93e-fefc-47f7-8cf6-2d2b58a9a35c"
        )
        """
        return self.delete(f'customers/{customer_id}/withdrawal-requests/{withdrawal_request_id}', **kwargs)

    def create_customer_withdrawal(
            self,
            customer_id: str,
            withdrawals: List[Tuple[str, str]],
            venue_withdrawal_id: Optional[str] = None,
            **kwargs
    ):
        """ Usage: client.create_customer_withdrawal(
            "123456",
            [("123.45", "USD"), ("0.87654321", "XBT")],
            "my-withdrawal-id" # optional
        )
        """
        return self.post(f'customers/{customer_id}/withdrawals', data={
            'venueWithdrawalIdentifier': venue_withdrawal_id or str(uuid4()),
            'withdrawal': [{'amount': w[0], 'symbol': w[1]} for w in withdrawals]
        }, **kwargs)

    def set_webhook_configuration(
            self,
            url: str,
            retries: int,
            retry_interval: int,
            notification_email: Optional[str] = None,
            **kwargs
    ):
        """ Usage: client.set_webhook_configuration(
            "https://my.webhook/endpoint",
            10,
            30,
            notification_email="webhook-errors@my.domain" # optional
        )
        """
        return self.post('webhook-config/', data={
            'url': url,
            'retries': retries,
            'retryInterval': retry_interval,
            'notificationEmail': notification_email,
        }, **kwargs)

    def get_webhook_configuration(self, **kwargs):
        """ Usage: client.get_webhook_configuration() """
        return self.get('webhook-config', **kwargs)

    def delete_webhook_configuration(self, **kwargs):
        """ Usage: client.delete_webhook_configuration() """
        return self.delete('webhook-config', **kwargs)

    def list_webhooks(
            self,
            from_sequence_number: int = None,
            from_dt: Optional[Union[datetime, str]] = None,
            to_dt: Optional[Union[datetime, str]] = None,
            undelivered: bool = None,
            **kwargs
    ):
        """ Usage: client.list_webhooks(
            from_sequence_number=1000,
            from_dt=datetime.now(),
            to_dt=datetime.now(),
            undelivered=True
        ) # all arguments are optional
        """
        return self.get('webhooks', params={
            'fromSequenceNumber': from_sequence_number,
            'from': self.format_date(from_dt),
            'to': self.format_date(to_dt),
            'undelivered': self.format_boolean(undelivered),
        }, **kwargs)

    def get_webhook(self, webhook_sequence_number: int, **kwargs):
        """ Usage: client.get_webhook(1000) """
        return self.get(f'webhooks/{webhook_sequence_number}', **kwargs)

    def mark_webhooks_as_delivered(self, delivered_webhooks: List[int], **kwargs):
        """ Usage: client.mark_webhooks_as_delivered([1000, 1001]) """
        return self.post(f'webhooks/delivered', data={
            'deliveredWebhooks': delivered_webhooks
        }, **kwargs)

    def list_trades(
            self,
            continuation_token: Optional[str] = None,
            from_dt: Optional[Union[datetime, str]] = None,
            to_dt: Optional[Union[datetime, str]] = None,
            counterparty_id: Optional[str] = None,
            **kwargs
    ):
        """ Usage: client.list_trades(
            continuation_token="continuation-token-from-previous-call",
            from_dt=datetime.now(),
            to_dt=datetime.now(),
            counterparty_id="123456"
        ) # all arguments are optional
        """
        return self.get('trades', params={
            'continuationToken': self.format_boolean(continuation_token),
            'from': self.format_date(from_dt),
            'to': self.format_date(to_dt),
            'counterpartyIdentifier': counterparty_id,
        }, **kwargs)

    def get_trade(self, trade_id: str, **kwargs):
        """ Usage: client.get_trade("my-trade-id") """
        return self.get(f'trades/{trade_id}', **kwargs)

    def submit_trade(
            self,
            trade_id: str,
            side: str,
            base_symbol: str,
            term_symbol: str,
            dealt: str,
            rate: str,
            counter: str,
            counterparty_id: str,
            execution_date: datetime,
            venue_fee: str,
            liquidity_indicator: Optional[str] = None,
            venue_fee_symbol: Optional[str] = None,
            notes: Optional[str] = None,
            **kwargs
    ):
        """ Usage: client.submit_trade(
            "my-trade-id",  # venue trade id
            "Buy",          # venue's trade side
            "XBT",          # base symbol
            "USD",          # term symbol
            "0.87654321",   # dealt (quantity)
            "13579.13",     # rate (price)
            "11902.6942",   # counter (notional amount, rounded to venue's specification)
            "123456",       # counterparty identifier (strike customer number)
            datetime.now(), # execution date
            "0.01",         # venue fee
            "Aggressive",   # liquidity indicator (optional)
            "USD",          # venue fee symbol (optional)
            "Trade notes",  # trade notes (optional)
        )
        """
        return self.post(f'trades', data={
            'identifier': trade_id,
            'side': side,
            'baseSymbol': base_symbol,
            'termSymbol': term_symbol,
            'dealt': dealt,
            'rate': rate,
            'counter': counter,
            'counterpartyIdentifier': counterparty_id,
            'liquidityIndicator': liquidity_indicator,
            'venueFee': venue_fee,
            'venueFeeSymbol': venue_fee_symbol,
            'notes': notes,
            'executionDate': self.format_date(execution_date),
            'tradeHash': self.compute_trade_hash(self.venue_id, counterparty_id, trade_id, side,
                                                 base_symbol, term_symbol, dealt, rate, counter, execution_date),
        }, **kwargs)

    def update_trade(
            self,
            original_trade: Dict[str, str],
            side: Optional[str] = None,
            base_symbol: Optional[str] = None,
            term_symbol: Optional[str] = None,
            dealt: Optional[str] = None,
            rate: Optional[str] = None,
            counter: Optional[str] = None,
            counterparty_id: Optional[str] = None,
            venue_fee: Optional[str] = None,
            venue_fee_symbol: Optional[str] = None,
            **kwargs
    ):
        """ Usage: client.update_trade(
            original_trade, # previous version of trade, as returned by get_trade()
            side="Buy",
            base_symbol="XBT",
            term_symbol="USD",
            dealt="0.87654321",
            rate="13579.13",
            counter="11902.6942",
            counterparty_id="123456",
            venue_fee="0.01",
            venue_fee_symbol="USD"
        ) # kwargs are all optional
        """
        return self.patch(f'trades/{original_trade["identifier"]}', data={
            'baseSymbol': base_symbol,
            'counter': counter,
            'counterpartyIdentifier': counterparty_id,
            'dealt': dealt,
            'rate': rate,
            'side': side,
            'termSymbol': term_symbol,
            'tradeHash': self.compute_trade_hash(
                venue_id=self.venue_id,
                counterparty_id=counterparty_id or original_trade['counterpartyIdentifier'],
                trade_id=original_trade['identifier'],
                side=side or original_trade['side'],
                base_symbol=base_symbol or original_trade['baseSymbol'],
                term_symbol=term_symbol or original_trade['termSymbol'],
                dealt=dealt or original_trade['dealt'],
                rate=rate or original_trade['rate'],
                counter=counter or original_trade['counter'],
                execution_date=self.date_from_string(original_trade['executionDate'])
            ),
            'venueFee': venue_fee,
            'venueFeeSymbol': venue_fee_symbol
        }, **kwargs)

    def cancel_trade(self, trade_id: str, **kwargs):
        """ Usage: client.cancel_trade("my-trade-id") """
        return self.delete(f'trades/{trade_id}', **kwargs)

    def create_settlement_plan(self, custodian_id: str, trade_ids: List[str], **kwargs):
        """ Usage: client.create_settlement_plan(
            "custodian",
            ["my-trade-id-1", "my-trade-id-2"]
        )
        """
        return self.post('settlement-plans', data={
            'custodian': custodian_id,
            'tradeIdentifiers': trade_ids,
        }, **kwargs)

    def list_settlement_plans(self, **kwargs):
        """ Usage: client.list_settlement_plans() """
        return self.get('settlement-plans', **kwargs)

    def get_settlement_plan(self, settlement_id: str, **kwargs):
        """ Usage: client.get_settlement_plan("VNUE-000001") """
        return self.get(f'settlement-plans/{settlement_id}', **kwargs)

    def cancel_settlement_plan(self, settlement_id: str, **kwargs):
        """ Usage: client.cancel_settlement_plan("VNUE-000001") """
        return self.delete(f'settlement-plans/{settlement_id}', **kwargs)

    def modify_trades_in_settlement_plan(
            self,
            settlement_id: str,
            add_trades: Optional[List[str]] = None,
            remove_trades: Optional[List[str]] = None,
            **kwargs
    ):
        """ Usage: client.modify_trades_in_settlement_plan(
            "VNUE-000001",
            add_trades=["my-trade-id-1"],   # optional
            remove_trades=["my-trade-id-2"] # optional
        )
        """
        return self.patch(f'settlement-plans/{settlement_id}/trades', data={
            'addTrades': add_trades or [],
            'removeTrades': remove_trades or [],
        }, **kwargs)

    def remove_customer_from_settlement_plan(self, settlement_id: str, customer_id: str, **kwargs):
        """ Usage: client.remove_customer_from_settlement_plan("VNUE-000001", "123456") """
        return self.delete(f'settlement-plans/{settlement_id}/customers/{customer_id}', **kwargs)

    def send_funding_requests_for_settlement_plan(self, settlement_id: str, **kwargs):
        """ Usage: client.send_funding_requests_for_settlement_plan("VNUE-000001") """
        return self.post(f'settlement-plans/{settlement_id}/funding-requests', **kwargs)

    def request_settlement(self, settlement_plan: Dict[str, str], **kwargs):
        """ Usage: client.request_settlement(
            settlement_plan # as returned from get_settlement_plan()
        )
        """
        return self.post(f'settlement-plans/{settlement_plan["identifier"]}/settle', data={
            'settlementHash': settlement_plan['settlementHash'],
            'signedSettlementFlowHash': self.sign(settlement_plan['flowHash']),
        }, **kwargs)

    def get_settlement(self, settlement_id: str, **kwargs):
        """ Usage: client.get_settlement("VNUE-000001") """
        return self.get(f'settlements/{settlement_id}', **kwargs)

    def list_settlements(
            self,
            from_dt: Optional[Union[datetime, str]] = None,
            to_dt: Optional[Union[datetime, str]] = None,
            **kwargs
    ):
        """ Usage: client.list_settlements(
            from_dt=datetime.now(), # optional
            to_dt=datetime.now()    # optional
        )
        """
        return self.get(f'settlements', params={
            'from': self.format_date(from_dt),
            'to': self.format_date(to_dt),
        }, **kwargs)

    def list_custodians(self, **kwargs):
        """ Usage: client.list_custodians() """
        return self.get('custodians', **kwargs)

    def get_custodian(self, custodian_id: str, **kwargs):
        """ Usage: client.get_custodian("custodian") """
        return self.get(f'custodians/{custodian_id}', **kwargs)

    def get_custodian_deposit_instructions(self, custodian_id: str, **kwargs):
        """ Usage: client.get_custodian_deposit_instructions("custodian") """
        return self.get(f'custodians/{custodian_id}/deposit-instructions', **kwargs)

    def list_custodian_deposits(
            self,
            custodian_id: str,
            from_dt: Optional[Union[datetime, str]] = None,
            to_dt: Optional[Union[datetime, str]] = None,
            **kwargs
    ):
        """ Usage: client.list_custodian_deposits(
            "custodian",
            from_dt=datetime.now(), # optional
            to_dt=datetime.now()    # optional
        )
        """
        return self.get(f'custodians/{custodian_id}/deposits', params={
            'from': self.format_date(from_dt),
            'to': self.format_date(to_dt),
        }, **kwargs)

    def list_custodian_withdrawal_destinations(self, custodian_id: str, **kwargs):
        """ Usage: client.list_custodian_withdrawal_destinations("custodian") """
        return self.get(f'custodians/{custodian_id}/withdrawal-destinations', **kwargs)

    def get_custodian_withdrawal_destination(self, custodian_id: str, withdrawal_destination_id: str, **kwargs):
        """ Usage: client.get_custodian_withdrawal_destination(
            "custodian", "60c5d562-6f7c-40f6-a810-f4e4a18846b6"
        )
        """
        return self.get(f'custodians/{custodian_id}/withdrawal-destinations/{withdrawal_destination_id}', **kwargs)

    def delete_withdrawal_destination(self, custodian_id: str, withdrawal_destination_id: str, **kwargs):
        """ Usage: client.delete_withdrawal_destination(
            "custodian", "60c5d562-6f7c-40f6-a810-f4e4a18846b6"
        )
        """
        return self.delete(f'custodians/{custodian_id}/withdrawal-destinations/{withdrawal_destination_id}', **kwargs)

    def create_withdrawal_destination(
            self,
            custodian_id: str,
            withdrawal_destination_name: str,
            withdrawal_destination_type: WithdrawalDestinationType,
            symbol: Optional[str] = None,  # must be specified for crypto
            wire_transfer_target_info: Optional[WireTransferTargetInfo] = None,  # must be specified for FIAT
            wallet_address: Optional[str] = None,
            destination_tag: Optional[str] = None,
            **kwargs
    ):
        """ Usage: client.create_withdrawal_destination(
            "custodian",
            "Bank Destination Label",
            WithdrawalDestinationType.USBank,
            wire_transfer_target_info=WireTransferTargetInfo(
                "Account Name",
                "40100410014", # account number
                "Bank Name",
                BankAccountType.checking,
                "200200211", # routing number
                "wire-reference"
            )
        )

        client.create_withdrawal_destination(
            "custodian",
            "XBT Destination Label",
            WithdrawalDestinationType.Crypto,
            symbol="XBT",
            wallet_address="bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq"
        )
        """
        return self.post(f'custodians/{custodian_id}/withdrawal-destinations', data={
            'name': withdrawal_destination_name,
            'destinationType': withdrawal_destination_type.name,
            'symbol': symbol,
            'wireTransferTargetInfo': wire_transfer_target_info.to_json() if wire_transfer_target_info else None,
            'walletAddress': wallet_address,
            'destinationTag': destination_tag,
        }, **kwargs)

    def request_custodian_withdrawal(
            self,
            custodian_id: str,
            destination_identifier: str,
            amount: str,
            symbol: str,
            venue_withdrawal_identifier: Optional[str] = None,
            wire_reference: Optional[str] = None,
            **kwargs
    ):
        """ Usage: client.request_custodian_withdrawal(
            "custodian",
            "2829fa04-d8dd-4c36-8bd2-80585e215623",
            "123.45",
            "USD",
            "my-withdrawal-identifier", # optional
            "wire-reference"            # optional
        )
        """
        return self.post(f'custodians/{custodian_id}/withdrawals', data={
            'venueWithdrawalIdentifier': venue_withdrawal_identifier,
            'amount': amount,
            'symbol': symbol,
            'destinationIdentifier': destination_identifier,
            'wireReference': wire_reference
        }, **kwargs)

    def list_custodian_withdrawals(
            self,
            custodian_id: str,
            from_dt: Optional[Union[datetime, str]] = None,
            to_dt: Optional[Union[datetime, str]] = None,
            **kwargs
    ):
        """ Usage: client.list_custodian_withdrawals(
            "custodian",
            from_dt=datetime.now(), # optional
            to_dt=datetime.now()    # optional
        )
        """
        return self.get(f'custodians/{custodian_id}/withdrawals', params={
            'from': self.format_date(from_dt),
            'to': self.format_date(to_dt),
        }, **kwargs)
