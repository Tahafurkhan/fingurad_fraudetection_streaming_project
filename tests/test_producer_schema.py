"""Contract validation for the producer's Kafka payload.

Whether this module ever catches a real drift is a bet on the schema staying
in sync with what producers actually send -- these tests are the part of
that bet that is checkable: given a payload that violates the contract in a
specific way, does validate_against_schema say so.
"""

from __future__ import annotations

import pytest

VALID_PAYLOAD = {
    "transaction_id": "TXN000001",
    "customer_id": "CUST0001",
    "card_number": "4111111111111111",
    "merchant_id": "MER0001",
    "merchant_name": "Test Merchant",
    "merchant_category": "Retail",
    "amount": 42.50,
    "currency": "USD",
    "transaction_type": "PURCHASE",
    "payment_channel": "ONLINE",
    "device_id": "DEV001",
    "city": "Springfield",
    "country": "USA",
    "transaction_timestamp": "2026-01-01T00:00:00Z",
    "is_international": False,
    "status": "APPROVED",
}


@pytest.fixture
def schema_module():
    """`schema` (src/producer/schema.py) has no pyspark dependency and no
    package prefix -- conftest puts src/producer on sys.path directly."""
    import schema

    return schema


def test_valid_payload_passes(schema_module):
    schema_module.validate_against_schema(VALID_PAYLOAD)  # must not raise


def test_payload_shape_matches_silver_parsing_schema(schema_module):
    """The contract and finguard.silver.transactions must describe the same
    wire shape.

    This is the test that gives the contract teeth. schema.py is only useful
    if it matches what silver actually parses -- if the two drift, the
    producer happily publishes payloads that silver reads as nulls. The
    silver schema is duplicated (Lakeflow loads that file non-importably),
    so nothing but a test can hold them together.
    """
    silver_fields = {
        "transaction_id", "customer_id", "card_number", "merchant_id",
        "merchant_name", "merchant_category", "amount", "currency",
        "transaction_type", "payment_channel", "device_id", "city",
        "country", "transaction_timestamp", "is_international", "status",
    }

    assert set(schema_module.TRANSACTION_SCHEMA["properties"]) == silver_fields


def test_missing_required_field_is_rejected(schema_module):
    payload = dict(VALID_PAYLOAD)
    del payload["transaction_id"]

    with pytest.raises(schema_module.SchemaValidationError, match="transaction_id"):
        schema_module.validate_against_schema(payload)


def test_multiple_missing_fields_are_all_named(schema_module):
    payload = dict(VALID_PAYLOAD)
    del payload["transaction_id"]
    del payload["amount"]

    with pytest.raises(schema_module.SchemaValidationError) as excinfo:
        schema_module.validate_against_schema(payload)

    assert "transaction_id" in str(excinfo.value)
    assert "amount" in str(excinfo.value)


def test_wrong_type_is_rejected(schema_module):
    payload = dict(VALID_PAYLOAD)
    payload["amount"] = "not-a-number"

    with pytest.raises(schema_module.SchemaValidationError, match="amount"):
        schema_module.validate_against_schema(payload)


def test_boolean_is_not_accepted_as_number(schema_module):
    """bool is a subclass of int in Python; the schema must not be fooled."""
    payload = dict(VALID_PAYLOAD)
    payload["amount"] = True

    with pytest.raises(schema_module.SchemaValidationError, match="amount"):
        schema_module.validate_against_schema(payload)


def test_int_amount_is_accepted_as_number(schema_module):
    """A whole-dollar amount arrives as an int, not a float -- must pass."""
    payload = dict(VALID_PAYLOAD)
    payload["amount"] = 100

    schema_module.validate_against_schema(payload)  # must not raise


def test_null_optional_field_is_allowed(schema_module):
    payload = dict(VALID_PAYLOAD)
    payload["merchant_name"] = None

    schema_module.validate_against_schema(payload)  # must not raise


def test_unknown_field_is_allowed(schema_module):
    """The schema is not closed -- forward compatibility means new fields
    from a newer producer version must not break an older consumer."""
    payload = dict(VALID_PAYLOAD)
    payload["a_field_from_the_future"] = "whatever"

    schema_module.validate_against_schema(payload)  # must not raise
