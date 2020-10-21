from enum import Enum
from typing import Optional


class SymbolType(Enum):
    Currency = 'Currency'
    Asset = 'Asset'


class WithdrawalDestinationType(Enum):
    USBank = 'USBank'
    InternationalBank = 'InternationalBank'
    Crypto = 'Crypto'
    Signet = 'Signet'


class BankAccountType(Enum):
    checking = 'checking'
    savings = 'savings'


class TransferStatus(Enum):
    Requested = 'Requested'
    Completed = 'Completed'
    Failed = 'Failed'


class CustodianStatus(Enum):
    Enabled = 'Enabled'
    Disabled = 'Disabled'


class Address:
    def __init__(self, street1: str, street2: str, city: str, region: str, postal_code: str, country: str):
        self.street1 = street1
        self.street2 = street2
        self.city = city
        self.region = region
        self.postal_code = postal_code
        self.country = country

    def to_json(self):
        return {
            'street1': self.street1,
            'street2': self.street2,
            'city': self.city,
            'region': self.region,
            'postalCode': self.postal_code,
            'country': self.country,
        }


class InternationalTransferMethodDetails:
    def __init__(
            self,
            swift_code: str,
            intermediary_bank_name: Optional[str] = None,
            intermediary_bank_reference: Optional[str] = None,
            intermediary_bank_address: Optional[Address] = None,
    ):
        self.swift_code = swift_code
        self.intermediary_bank_name = intermediary_bank_name
        self.intermediary_bank_reference = intermediary_bank_reference
        self.intermediary_bank_address = intermediary_bank_address

    def to_json(self):
        return {
            'swiftCode': self.swift_code,
            'intermediaryBankName': self.intermediary_bank_name,
            'intermediaryBankReference': self.intermediary_bank_reference,
            'intermediaryBankAddress': (self.intermediary_bank_address.to_json()
                                        if self.intermediary_bank_address else None),
        }


class BankTransferDetails:
    def __init__(
            self,
            bank_account_name: str,
            bank_account_number: str,
            bank_name: Optional[str] = None,
            bank_account_type: Optional[BankAccountType] = None,
            routing_number: Optional[str] = None,
            international_details: Optional[InternationalTransferMethodDetails] = None,
    ):
        self.bank_name = bank_name
        self.bank_account_name = bank_account_name
        self.bank_account_type = bank_account_type
        self.bank_account_number = bank_account_number
        self.routing_number = routing_number
        self.international_details = international_details

    def to_json(self):
        return {
            'bankName': self.bank_name,
            'bankAccountName': self.bank_account_name,
            'bankAccountType': self.bank_account_type.name if self.bank_account_type else None,
            'bankAccountNumber': self.bank_account_number,
            'routingNumber': self.routing_number,
            'internationalDetails': self.international_details.to_json() if self.international_details else None,
        }
