# Credit Card Transaction Simulator

A production-oriented Python simulator that generates realistic credit card transactions and publishes them to a Kafka topic for downstream fraud detection workflows. This is the data-generation layer that feeds the FinGuard Lakehouse pipeline.

## Features

- Generates realistic customer and merchant datasets
- Simulates spending behavior that is consistent per customer
- Applies configurable fraud rules and scores
- Publishes JSON transactions to Confluent Cloud Kafka
- Logs delivery status, transaction metadata, and runtime stats
- Handles keyboard interrupts and graceful shutdown

## Installation

### 1. Create a virtual environment

```bash
python -m venv .venv
```

### 2. Activate the environment

On Windows:

```powershell
.\.venv\Scripts\Activate.ps1
```

On macOS/Linux:

```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

## Environment Configuration

Create a `.env` file in this folder (never commit it):

```env
BOOTSTRAP_SERVERS=your-bootstrap-servers:9092
API_KEY=your_api_key
API_SECRET=your_api_secret
TOPIC_NAME=credit_card_transactions
TRANSACTIONS_PER_SECOND=5
FRAUD_PERCENTAGE=0.08
TOTAL_CUSTOMERS=1000
TOTAL_MERCHANTS=200
RANDOM_SEED=42
```

## Running the Producers

Three producer entry points are provided:

```bash
python producer_normal.py           # Continuous stream of normal + naturally-flagged transactions
python producer_fraud_transaction.py  # Emits a single forced high-value fraud transaction
python producer_fraud_card.py         # Emits a single forced fraud transaction on a fixed test card
```

Each run will:

1. Generate customer and merchant datasets if they do not already exist (`data/customers.csv`, `data/merchants.csv`).
2. Start producing transactions to the configured Kafka topic on Confluent Cloud.
3. Log delivery status per message.

Use `consumer.py` to tail the topic locally for debugging:

```bash
python consumer.py
```

## Example Output

```text
INFO - Produced TXN0000001 | Amount=1450.50
INFO - Produced TXN0000002 | Amount=9500.00
```

## Project Architecture

- [config.py](config.py) loads configuration values from `.env` and validates them
- [models.py](models.py) defines dataclasses for customers, merchants, transactions, and stats
- [customer_generator.py](customer_generator.py) creates realistic customer records (segment, income, spend profile, card number with Luhn check digit)
- [merchant_generator.py](merchant_generator.py) creates merchant records (category, risk tier, blacklist flag)
- [transaction_generator.py](transaction_generator.py) simulates transaction behavior driven by customer/merchant profiles
- [fraud_engine.py](fraud_engine.py) evaluates fraud conditions (velocity, impossible travel, new device, blacklisted/high-risk merchant, card testing) and scores 0-100
- [producer_normal.py](producer_normal.py) / [producer_fraud_transaction.py](producer_fraud_transaction.py) / [producer_fraud_card.py](producer_fraud_card.py) publish messages to Kafka with idempotent delivery
- [consumer.py](consumer.py) is a debug consumer that tails the topic
- [utils.py](utils.py) contains shared helpers (ID generation, timestamping, JSON validation)

## Folder Explanation

- `data/` stores generated customer and merchant CSV files (gitignored)
- `.env` contains runtime secrets and Kafka configuration (gitignored, never commit)
- `requirements.txt` lists project dependencies

## Fraud Rules Modeled

| Rule | Trigger |
|---|---|
| HIGH_VALUE_TRANSACTION | Amount > 100,000 |
| IMPOSSIBLE_TRAVEL | Same customer, Delhi → London within 20 minutes |
| NEW_DEVICE | Transaction device ≠ customer's trusted device |
| HIGH_RISK_MERCHANT | Merchant risk tier = HIGH |
| BLACKLISTED_MERCHANT | Merchant is on blacklist |
| INTERNATIONAL_TRANSACTION | Cross-border transaction |
| VELOCITY_FRAUD | 5+ transactions from same customer within 30 seconds |
| CARD_TESTING | 3+ small transactions ($5-$20) within 60 seconds |

## Future Enhancements

- Add schema validation using JSON Schema or Pydantic
- Support multiple Kafka topics for fraud vs. clean traffic
- Persist transaction history for replay and testing
- Add Prometheus metrics for monitoring
