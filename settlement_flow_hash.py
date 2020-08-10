from decimal import Decimal
from hashlib import sha256

quanta = Decimal("0.000000000000000000")


def compute_settlement_flow_hash(
        settlement_plan_identifier, account_identifier, inflows, outflows):

    content_strings = []

    inflows.sort(key=lambda x: x['counterpartyCustodianIdentifier'])
    for inflow in inflows:
        content_strings.append("|".join([
            inflow['counterpartyCustodianIdentifier'],
            account_identifier,
            inflow['strikeSymbol'],
            str(Decimal(inflow['amount']).quantize(quanta))
        ]))

    outflows.sort(key=lambda x: x['counterpartyCustodianIdentifier'])
    for outflow in outflows:
        content_strings.append("|".join([
            account_identifier,
            outflow['counterpartyCustodianIdentifier'],
            outflow['strikeSymbol'],
            str(Decimal(outflow['amount']).quantize(quanta))
        ]))

    content_strings.append(settlement_plan_identifier)

    content = "|".join(content_strings)

    settlement_flow_hash = sha256(str.encode(content)).hexdigest()

    return content, settlement_flow_hash


content, settlement_flow_hash = compute_settlement_flow_hash(
    settlement_plan_identifier="sp-1",
    account_identifier="id-1",
    inflows=[
        {'counterpartyCustodianIdentifier': 'id-3', 'strikeSymbol': 'USD', 'amount': '1000'},
        {'counterpartyCustodianIdentifier': 'id-2', 'strikeSymbol': 'USD', 'amount': '500'}],
    outflows=[
        {'counterpartyCustodianIdentifier': 'id-3', 'strikeSymbol': 'XET', 'amount': '3'},
        {'counterpartyCustodianIdentifier': 'id-2', 'strikeSymbol': 'XET', 'amount': '1.5'}])


assert content == \
    "id-2|id-1|USD|500.000000000000000000|id-3|id-1|USD|1000.000000000000000000|id-1|id-2|XET|1.500000000000000000|id-1|id-3|XET|3.000000000000000000|sp-1"
assert settlement_flow_hash == \
    "89a0a34375dd34fdfb123d1a851e1324c49494131c02b04e51d194d38f30a39b"
