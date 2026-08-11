"""Versioned contract for the transaction payload published to Kafka.

Why a JSON Schema file instead of wiring Confluent Schema Registry directly:
Schema Registry needs a provisioned registry endpoint and its own credentials,
which is infrastructure this project does not have -- Confluent Cloud's
registry is a separate paid resource from the Kafka cluster itself. Faking a
connection to it would be worse than not having one.

What this closes instead: today a field rename in `Transaction` (models.py)
reaches Kafka with no warning, and the first sign of trouble is bronze
silently getting a differently-shaped payload. `validate_against_schema`
makes that a producer-side `SchemaValidationError` before the message is ever
sent, using the exact schema this file defines -- so the contract is real and
enforced, just checked in-process rather than by a registry service.

Migrating to Confluent Schema Registry later is additive, not a rewrite: the
schema below translates directly to an Avro schema (same field names, same
optionality), `confluent-kafka[avro]`'s `AvroSerializer` replaces
`json.dumps` at the call site in each producer_*.py, and `SCHEMA_VERSION`
becomes the registry's native version number instead of a field in the
payload.

WHY SCHEMA_VERSION IS NOT ON THE WIRE (yet)
-------------------------------------------
An earlier draft added `schema_version` to the published payload and to the
silver parsing schema. That is deliberately not done here. The payload
occupies columns 1-16 of finguard.silver.transactions, and that table sets
`delta.dataSkippingNumIndexedCols = 16` -- a budget tuned so statistics cover
every column through `transaction_timestamp`, which is a liquid-clustering
key. See the comment in silver/fingurad_silver.py: setting that budget to 12
once failed the pipeline outright with DELTA_CLUSTERING_COLUMN_MISSING_STATS.

Appending a 17th payload column would fall outside that budget. It would not
break anything today -- `schema_version` is not a clustering key and no
predicate filters on it -- but changing the physical layout of the busiest
table in the project to carry a field nothing reads yet is cost without
benefit, and it quietly erodes a setting someone tuned deliberately.

So the contract is enforced where it actually catches the bug: in the
producer, before publish. When a consumer genuinely needs to branch on
version, add the field and raise the stats budget to 17 in the same change,
so the coupling stays visible.
"""

from __future__ import annotations

from typing import Any

# Bumped whenever a field is added, removed, renamed, or changes type.
#
# Not currently written to the payload -- see the module docstring for why.
# It exists so the contract has a version to cite in a commit message or a
# migration note, and so the field can be added later without inventing a
# numbering scheme at the moment it is first needed.
SCHEMA_VERSION = 1

# The contract for a Transaction payload, expressed as a minimal JSON Schema.
# Deliberately hand-rolled rather than derived from the Transaction dataclass:
# deriving it would make the schema track the producer's internal
# representation automatically, which defeats the point -- the schema should
# change only when someone decides the wire contract changes, not whenever a
# dataclass field is renamed for internal reasons.
TRANSACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": [
        "transaction_id",
        "customer_id",
        "card_number",
        "merchant_id",
        "amount",
        "currency",
        "transaction_timestamp",
        "status",
    ],
    "properties": {
        "transaction_id": {"type": "string"},
        "customer_id": {"type": "string"},
        "card_number": {"type": "string"},
        "merchant_id": {"type": "string"},
        "merchant_name": {"type": "string"},
        "merchant_category": {"type": "string"},
        "amount": {"type": "number"},
        "currency": {"type": "string"},
        "transaction_type": {"type": "string"},
        "payment_channel": {"type": "string"},
        "device_id": {"type": "string"},
        "city": {"type": "string"},
        "country": {"type": "string"},
        "transaction_timestamp": {"type": "string"},
        "is_international": {"type": "boolean"},
        "status": {"type": "string"},
    },
}


class SchemaValidationError(ValueError):
    """Raised when a payload does not match TRANSACTION_SCHEMA.

    Deliberately a ValueError subclass rather than a bare exception: callers
    that already catch ValueError for payload problems (see
    utils.validate_json_payload's callers) keep working without a second
    except clause.
    """


def _check_type(value: Any, expected: str, field: str) -> None:
    type_map = {
        "string": str,
        "number": (int, float),
        "boolean": bool,
    }
    python_type = type_map[expected]
    # bool is a subclass of int in Python, so a boolean would pass a "number"
    # check unless explicitly excluded.
    if expected == "number" and isinstance(value, bool):
        raise SchemaValidationError(f"{field}: expected number, got boolean")
    if not isinstance(value, python_type):
        raise SchemaValidationError(
            f"{field}: expected {expected}, got {type(value).__name__}"
        )


def validate_against_schema(
    payload: dict[str, Any], schema: dict[str, Any] = TRANSACTION_SCHEMA
) -> None:
    """Raise SchemaValidationError on the first contract violation.

    Checked before the message is handed to the producer, so a broken payload
    never reaches Kafka -- the alternative is a malformed record sitting in
    the topic until someone notices bronze looks wrong.
    """
    missing = [f for f in schema["required"] if f not in payload]
    if missing:
        raise SchemaValidationError(
            f"missing required field(s): {', '.join(missing)}"
        )

    for field, value in payload.items():
        spec = schema["properties"].get(field)
        if spec is None:
            continue  # Unknown fields are allowed; this is not a closed schema.
        if value is None:
            continue  # Absence is covered by `required`; a present null is not this schema's concern.
        _check_type(value, spec["type"], field)
