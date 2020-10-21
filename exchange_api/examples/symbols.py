from typing import Optional

from exchange_api.client import Client
from exchange_api.models import SymbolType


def get_symbols_supported_by_custodian(client: Client, custodian_id: str, symbol_type: Optional[SymbolType] = None):
    all_symbols = client.list_symbols()
    all_symbols_supported_by_custodian = []
    for symbol in all_symbols:
        supported_custodians = [c['custodianIdentifier'] for c in symbol['custodianSymbols']]
        if custodian_id in supported_custodians and (symbol_type is None or symbol_type.name == symbol['type']):
            all_symbols_supported_by_custodian.append(symbol['symbol'])
    return all_symbols_supported_by_custodian
