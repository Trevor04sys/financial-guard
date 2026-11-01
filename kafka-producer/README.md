# Kafka Transaction Producers

This directory contains the Python components responsible for generating synthetic credit card transaction events and publishing them to Confluent Cloud Kafka for the Financial Guard streaming pipeline.

## Overview

The producer application supports three transaction-generation scenarios:

| Producer | Purpose |
|---|---|
| `producer_normal.py` | Generates normal transaction events |
| `producer_fraud_transaction.py` | Generates transactions with simulated fraud scenarios |
| `producer_fraud_card.py` | Generates transactions associated with fraud-card scenarios |

The generated events are published to Kafka and consumed by the Databricks streaming pipeline.

## Supporting Modules

| File | Responsibility |
|---|---|
| `config.py` | Application and Kafka configuration |
| `fraud_engine.py` | Rule-based synthetic fraud scenario generation |
| `models.py` | Transaction data models |
| `utils.py` | Shared utility functions |
| `customer_generator.py` | Synthetic customer data generation |
| `merchant_generator.py` | Synthetic merchant data generation |
| `transaction_generator.py` | Synthetic transaction generation |
| `update_csv_email.py` | CSV email-related utility |
| `consumer.py` | Kafka consumer utility |

## Requirements

- Python 3.x
- Confluent Cloud Kafka cluster or compatible Kafka environment
- Kafka bootstrap server and valid client credentials

## Installation

From the repository root:

```bash
cd kafka-producer
python -m venv venv
```

Activate the environment on Windows:

```bash
venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Configure the required Kafka connection settings locally.

Use `.env.example` as a template if provided, and create your own `.env` file.

Never commit:
- Kafka API keys or secrets
- Gmail app passwords
- Database credentials
- Access tokens
- Private environment files

Use environment variables for local configuration and Databricks secrets for credentials used by the Databricks pipeline.

## Running the Producers

The producer scripts are intended to generate synthetic transaction events for the configured Kafka topic.

Before execution:

1. Configure the Kafka bootstrap server.
2. Configure the required authentication credentials.
3. Verify the Kafka topic exists.
4. Ensure the Python dependencies are installed.
5. Start the relevant producer script.

Example:

```bash
python producer_normal.py
```

Run the other producer scripts according to their configured scenario and requirements.

## Fraud Scenario Generation

The project uses rule-based logic to generate synthetic fraud scenarios, including:

- High-value transactions
- Impossible travel
- New device activity
- High-risk merchant activity
- International transactions
- Transaction velocity patterns
- Card-testing patterns

These rules are used for synthetic scenario generation and demonstration. They do not represent a trained machine learning model.

## Integration with Databricks

The transaction events published to Kafka are consumed by the Financial Guard Databricks pipeline.

The processing flow is:

```text
Python Producers
       |
       v
Confluent Cloud Kafka
       |
       v
Databricks Bronze
       |
       v
Databricks Silver
       |
       v
Databricks Gold
       |
       v
Alerts and Dashboard
```

## Important Notes

- All generated transaction data is synthetic.
- Kafka credentials must be supplied through secure configuration.
- The original development environment used a Confluent Cloud free trial. Reproduction requires an active Kafka environment.
- Producer behaviour and available configuration depend on the source code and environment settings.
