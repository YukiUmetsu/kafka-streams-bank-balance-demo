# Kafka Streams Bank Balance

Kafka Streams example that produces Avro `Transaction` events, aggregates them into per-user `UserBalance` records, and reads the results back from Kafka using Schema Registry.

## What this project does

This repo contains three small applications:

- `TransactionProducer`: produces random bank transactions to `bank-transactions`
- `UserBalanceStream`: aggregates transactions by user and writes running balances to `user-balance`
- `UserBalanceConsumer`: consumes the aggregated balances and prints them

Both input and output payloads use Avro schemas registered in Schema Registry.

## Architecture

```mermaid
flowchart TB
    P[TransactionProducer]
    IT[bank-transactions]
    S[UserBalanceStream]
    OT[user-balance]
    C[UserBalanceConsumer]
    SR[Schema Registry]
    UI[Kafbat UI]

    P -->|Avro Transaction| IT
    IT --> S
    S -->|Avro UserBalance| OT
    OT --> C

    P -.->|schema lookup/register| SR
    S -.->|schema lookup| SR
    C -.->|schema lookup| SR

    UI -.-> IT
    UI -.-> OT
    UI -.-> SR

    style P fill:#eff6ff,stroke:#1d4ed8,stroke-width:2px,color:#111827
    style C fill:#eff6ff,stroke:#1d4ed8,stroke-width:2px,color:#111827
    style IT fill:#ecfeff,stroke:#0f766e,stroke-width:4px,color:#111827
    style OT fill:#ecfeff,stroke:#0f766e,stroke-width:4px,color:#111827
    style S fill:#fef3c7,stroke:#b45309,stroke-width:3px,color:#111827
    style SR fill:#f9fafb,stroke:#9ca3af,stroke-width:1px,color:#4b5563
    style UI fill:#f9fafb,stroke:#9ca3af,stroke-width:1px,color:#4b5563

    linkStyle 0 stroke:#2563eb,stroke-width:4px
    linkStyle 1 stroke:#2563eb,stroke-width:4px
    linkStyle 2 stroke:#2563eb,stroke-width:4px
    linkStyle 3 stroke:#2563eb,stroke-width:4px
    linkStyle 4 stroke:#9ca3af,stroke-width:1.5px
    linkStyle 5 stroke:#9ca3af,stroke-width:1.5px
    linkStyle 6 stroke:#9ca3af,stroke-width:1.5px
    linkStyle 7 stroke:#9ca3af,stroke-width:1.5px
    linkStyle 8 stroke:#9ca3af,stroke-width:1.5px
    linkStyle 9 stroke:#9ca3af,stroke-width:1.5px
```

## Local access

After starting the local stack, open:

- Kafka UI: `http://localhost:8080`
- Schema Registry API: `http://localhost:8081`
- Kafka broker for local apps: `127.0.0.1:9092`

The Docker setup for these services is in [docker-compose.yml](/Users/yukiumetsu/Documents/projects/udemy/kafka-streams-code/own-code/bank-balance-stream/docker-compose.yml:1).

In Kafka UI, connect to the `local` cluster and inspect:

- topic `bank-transactions`
- topic `user-balance`
- registered Avro subjects in Schema Registry

## Why idempotence matters

This application updates balances. That is stateful and sensitive to duplicates.

If the producer retries a send and Kafka accepts the same logical event twice, or if the stream app reprocesses records after a failure without transactional guarantees, balances can be overstated. For money-like aggregates, that is the wrong failure mode.

Kafka gives us the primitives to avoid that:

- idempotent producer writes prevent duplicate records caused by producer retries
- Kafka Streams exactly-once processing keeps state-store updates and output-topic writes in one transactional unit

For this repo, the practical goal is:

1. a transaction should be written once to `bank-transactions` even when the producer retries
2. each input record should contribute once to the `user-balance` aggregate
3. the updated state and the emitted `user-balance` record should succeed or fail together

## Reliability configuration used here

### Producer requirements

The producer config lives in [TransactionProducer.java](/Users/yukiumetsu/Documents/projects/udemy/kafka-streams-code/own-code/bank-balance-stream/src/main/java/com/github/yukiumetsu/udemy/kafka/streams/TransactionProducer.java:76).

Important settings:

- `acks=all`
- `retries=3`
- `enable.idempotence=true`

Why these matter:

- `enable.idempotence=true` tells Kafka to deduplicate retry-driven resend attempts from the same producer session
- `acks=all` requires the broker leader to wait for all in-sync replicas before acknowledging a write
- `retries=3` lets the client retry transient failures instead of dropping records immediately

In practice, Kafka also requires compatible producer settings for idempotence. This code sets the two most visible ones explicitly:

- `acks=all`
- `enable.idempotence=true`

On a real multi-broker production cluster, you would also care about replication and ISR durability. This demo runs with replication factor `1`, which is fine for local development but not for production durability.

### Streams application requirements

The Kafka Streams config lives in [UserBalanceStream.java](/Users/yukiumetsu/Documents/projects/udemy/kafka-streams-code/own-code/bank-balance-stream/src/main/java/com/github/yukiumetsu/udemy/kafka/streams/UserBalanceStream.java:92).

Important setting:

- `processing.guarantee=exactly_once_v2`

Why it matters:

- Kafka Streams writes changelog/state updates and output records transactionally
- committed input offsets are coordinated with those writes
- on restart or failure, the app avoids double-applying already completed work

That is the key protection for this pipeline, because `UserBalanceStream` is not just forwarding records. It is maintaining a running aggregate per user.

### Broker-side support in local Docker

The local broker in [docker-compose.yml](/Users/yukiumetsu/Documents/projects/udemy/kafka-streams-code/own-code/bank-balance-stream/docker-compose.yml:19) includes transaction-log settings that allow transactional/idempotent workflows to run:

- `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`
- `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1`

Those values are reduced for a single-node local environment. In production, these should be sized for a real multi-broker cluster.

## Data model

### Input schema: `Transaction`

File: [src/main/avro/Transaction.avsc](src/main/avro/Transaction.avsc)

Fields:

- `name`: user name
- `amount`: Avro decimal with `precision=12`, `scale=2`
- `time`: Avro `timestamp-millis`

### Output schema: `UserBalance`

File: [src/main/avro/UserBalance.avsc](src/main/avro/UserBalance.avsc)

Fields:

- `name`: user name
- `amount`: running total balance for that user
- `time`: latest transaction timestamp seen for that user

## Processing logic

`UserBalanceStream` groups transactions by Kafka record key. The producer uses the user name as the key, so all transactions for the same user are aggregated together.

Aggregation logic:

1. decode current `UserBalance.amount`
2. decode incoming `Transaction.amount`
3. add the two values
4. compare timestamps
5. keep the later timestamp
6. emit a new `UserBalance`

Semantically:

```text
balance[name].amount = balance[name].amount + transaction.amount
balance[name].time   = max(balance[name].time, transaction.time)
```

## Important code paths

### `TransactionProducer`

File: [src/main/java/com/github/yukiumetsu/udemy/kafka/streams/TransactionProducer.java](src/main/java/com/github/yukiumetsu/udemy/kafka/streams/TransactionProducer.java)

Important parts:

- creates `bank-transactions` and `user-balance` if missing
- uses `KafkaAvroSerializer`
- writes specific Avro `Transaction` records
- generates random names from a fixed list
- generates random decimal amounts and uses current `Instant`

### `UserBalanceStream`

File: [src/main/java/com/github/yukiumetsu/udemy/kafka/streams/UserBalanceStream.java](src/main/java/com/github/yukiumetsu/udemy/kafka/streams/UserBalanceStream.java)

Important parts:

- configures `SpecificAvroSerde<Transaction>` and `SpecificAvroSerde<UserBalance>`
- reads `bank-transactions`
- builds a `KTable<String, UserBalance>`
- writes results to `user-balance`
- enables `exactly_once_v2`
- uses a shutdown hook and `CountDownLatch` for controlled lifecycle

### `UserBalanceConsumer`

File: [src/main/java/com/github/yukiumetsu/udemy/kafka/streams/UserBalanceConsumer.java](src/main/java/com/github/yukiumetsu/udemy/kafka/streams/UserBalanceConsumer.java)

Important parts:

- uses `KafkaAvroDeserializer`
- enables `specific.avro.reader=true`
- reads `UserBalance` as a generated Avro class
- converts Avro decimal bytes back to `BigDecimal`

## Dev environment setup

### Prerequisites

- Java 11+
- Maven 3.9+
- Docker and Docker Compose

### Start infrastructure

This project includes [docker-compose.yml](docker-compose.yml) for:

- Kafka broker in KRaft mode
- Schema Registry
- Kafbat UI

Start the stack:

```bash
docker compose up -d
```

Useful endpoints:

- Kafka broker: `localhost:9092`
- Schema Registry: `http://localhost:8081`
- Kafbat UI: `http://localhost:8080`

### Generate Avro classes

The schemas under `src/main/avro` generate Java classes into `target/generated-sources/avro`.

```bash
mvn clean generate-sources
```

### Compile the project

```bash
mvn compile
```

If IntelliJ does not recognize generated classes:

1. reload the Maven project
2. mark `target/generated-sources/avro` as generated sources if needed

## How to run the demo

This project is easiest to run from IntelliJ by launching the classes directly.

Run these in order:

1. `TransactionProducer`
2. `UserBalanceStream`
3. `UserBalanceConsumer`

Main classes:

- `com.github.yukiumetsu.udemy.kafka.streams.TransactionProducer`
- `com.github.yukiumetsu.udemy.kafka.streams.UserBalanceStream`
- `com.github.yukiumetsu.udemy.kafka.streams.UserBalanceConsumer`

You can also inspect topics and schemas in Kafbat UI at `http://localhost:8080`.

## Example event flow

### Example input records on `bank-transactions`

Avro on Kafka is binary, but logically the input data looks like this:

```json
[
  {
    "key": "alice",
    "value": {
      "name": "alice",
      "amount": 125.75,
      "time": "2026-05-18T15:00:00Z"
    }
  },
  {
    "key": "alice",
    "value": {
      "name": "alice",
      "amount": 20.25,
      "time": "2026-05-18T15:01:10Z"
    }
  },
  {
    "key": "bob",
    "value": {
      "name": "bob",
      "amount": 50.00,
      "time": "2026-05-18T15:02:00Z"
    }
  }
]
```

### Example aggregated output on `user-balance`

```json
[
  {
    "key": "alice",
    "value": {
      "name": "alice",
      "amount": 146.00,
      "time": "2026-05-18T15:01:10Z"
    }
  },
  {
    "key": "bob",
    "value": {
      "name": "bob",
      "amount": 50.00,
      "time": "2026-05-18T15:02:00Z"
    }
  }
]
```

## Technical details

### Serialization

- Producer value serializer: `KafkaAvroSerializer`
- Streams value serde: `SpecificAvroSerde`
- Consumer value deserializer: `KafkaAvroDeserializer`

### Decimal handling

Avro decimal logical types are stored as `bytes`, not as Java `BigDecimal` directly. This code converts:

- `BigDecimal -> ByteBuffer` when producing or emitting aggregate results
- `ByteBuffer -> BigDecimal` when aggregating or consuming

### Exactly-once processing

`UserBalanceStream` sets:

```java
StreamsConfig.PROCESSING_GUARANTEE_CONFIG = exactly_once_v2
```

This enables Kafka Streams exactly-once processing semantics for the topology.

### Topic layout

- `bank-transactions`
  - key: `String`
  - value: `Transaction`
- `user-balance`
  - key: `String`
  - value: `UserBalance`

## Current implementation notes

- `TransactionProducer` currently emits roughly one event every `50ms`
- the Docker Compose stack pre-creates `bank-transactions`
- `TransactionProducer` also attempts to create topics on startup
- the Maven shade plugin main class in `pom.xml` is still stale and should be updated before relying on a packaged fat jar

## Repo layout

```text
.
├── docker-compose.yml
├── pom.xml
├── src
│   └── main
│       ├── avro
│       │   ├── Transaction.avsc
│       │   └── UserBalance.avsc
│       └── java
│           └── com/github/yukiumetsu/udemy/kafka/streams
│               ├── TransactionProducer.java
│               ├── UserBalanceConsumer.java
│               └── UserBalanceStream.java
```

## Troubleshooting

### Generated Avro class is not found

Run:

```bash
mvn clean generate-sources
```

Then reload the Maven project in IntelliJ.

### Schema Registry connection errors

Verify:

- Kafka is running on `localhost:9092`
- Schema Registry is running on `http://localhost:8081`
- `docker compose ps` shows healthy containers

### `SpecificAvroSerde` errors at runtime

Explicit serde instances must be configured with:

```java
Map.of("schema.registry.url", "http://localhost:8081")
```

This is already done in `UserBalanceStream`.
