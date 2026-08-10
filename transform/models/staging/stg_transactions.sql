-- Staging: transactions.
--
-- Kafka envelope fields are carried through deliberately. They are not
-- business data, but topic/partition/offset is what makes a row traceable back
-- to the exact message that produced it -- which is the difference between
-- "the number looks wrong" and "here is the message that caused it".

select
    transaction_id,
    customer_id,
    merchant_id,
    card_number,

    amount,
    currency,
    transaction_type,
    payment_channel,
    device_id,

    city                        as transaction_city,
    country                     as transaction_country,
    is_international,
    status                      as transaction_status,

    transaction_timestamp,
    cast(transaction_timestamp as date) as transaction_date,

    -- Kafka provenance.
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,

    bronze_ingestion_timestamp,
    silver_ingestion_timestamp

from {{ source('silver', 'transactions') }}
