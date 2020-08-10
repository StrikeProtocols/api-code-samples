from decimal import Decimal
from datetime import datetime, timezone
from hashlib import sha256

quanta = Decimal("0.000000000000000000")


def compute_trade_hash(
        customer_identifier, counterparty_identifier, trade_identifier, side, base_symbol, term_symbol,
        dealt, rate, notional, execution_date):

    content = "|".join([
        customer_identifier,
        counterparty_identifier,
        trade_identifier,
        side,
        base_symbol,
        term_symbol,
        str(Decimal(dealt).quantize(quanta)),
        str(Decimal(rate).quantize(quanta)),
        str(Decimal(notional).quantize(quanta)),
        execution_date.isoformat(timespec='milliseconds')])

    trade_hash = sha256(str.encode(content)).hexdigest()

    return content, trade_hash


content, trade_hash = compute_trade_hash(
    customer_identifier="123456",
    counterparty_identifier="987654",
    trade_identifier="abc123",
    side="Sell",
    base_symbol="XBT",
    term_symbol="USD",
    dealt="12.345678",
    rate="11201.72",
    notional="138292.83",
    execution_date=datetime(
        year=2020, month=1, day=2, hour=3, minute=4, second=5, microsecond=678000, tzinfo=timezone.utc
    ))


assert content == \
    "123456|987654|abc123|Sell|XBT|USD|12.345678000000000000|11201.720000000000000000|138292.830000000000000000|2020-01-02T03:04:05.678+00:00"
assert trade_hash == \
    "1d0b0b4ab7a8bb2c28062323efae4e4270c478daf65bdffdb97f0c1c08287305"
