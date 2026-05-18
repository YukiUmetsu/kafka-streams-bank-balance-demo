package com.github.yukiumetsu.udemy.kafka.streams;

import com.github.yukiumetsu.kafka.avro.Transaction;
import com.github.yukiumetsu.kafka.avro.UserBalance;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class UserBalanceStream {
    private static final Logger LOG = LoggerFactory.getLogger(UserBalanceStream.class);
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String INPUT_TOPIC = "bank-transactions";
    private static final String OUTPUT_TOPIC = "user-balance";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final SpecificAvroSerde<UserBalance> USER_BALANCE_SERDE = new SpecificAvroSerde<>();
    private static final SpecificAvroSerde<Transaction> TRANSACTION_SERDE = new SpecificAvroSerde<>();

    public static void main(String[] args) {
        Map<String, String> serdeConfig = Map.of("schema.registry.url", SCHEMA_REGISTRY_URL);
        TRANSACTION_SERDE.configure(serdeConfig, false);
        USER_BALANCE_SERDE.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Transaction> transactionStream = builder.stream(INPUT_TOPIC);
        KTable<String, UserBalance> userBalanceTable = transactionStream
                .groupByKey(Grouped.with(Serdes.String(), TRANSACTION_SERDE))
                .aggregate(
                        () -> UserBalance.newBuilder()
                                .setName("")
                                .setAmount(toAmountBytes(BigDecimal.ZERO))
                                .setTime(Instant.EPOCH)
                                .build(),
                        UserBalanceStream::aggregateUserBalance,
                        Materialized.with(Serdes.String(), USER_BALANCE_SERDE)
                );
        userBalanceTable.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), USER_BALANCE_SERDE));

        KafkaStreams streams = new KafkaStreams(builder.build(), getNewStreamProperties());
        // Keep the main thread blocked until shutdown or a fatal stream error.
        // The latch starts at 1, `await()` waits, and `countDown()` releases it.
        CountDownLatch latch = new CountDownLatch(1);

        streams.setStateListener((newState, oldState) ->
                LOG.info("State transition: {} -> {}", oldState, newState));
        streams.setUncaughtExceptionHandler(exception -> {
            LOG.error("Stream thread failed", exception);
            latch.countDown();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            LOG.error("Kafka Streams terminated unexpectedly", e);
            System.exit(1);
        }
    }

    private static UserBalance aggregateUserBalance(String name, Transaction transaction, UserBalance userBalance) {
        BigDecimal currentAmount = fromBytesToDecimal(userBalance.getAmount());
        BigDecimal incomingAmount = fromBytesToDecimal(transaction.getAmount());
        Instant latestTime = userBalance.getTime().isAfter(transaction.getTime()) ? userBalance.getTime() : transaction.getTime();
        return UserBalance.newBuilder()
                .setName(name)
                .setAmount(toAmountBytes(currentAmount.add(incomingAmount)))
                .setTime(latestTime)
                .build();
    }

    private static Properties getNewStreamProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-balance-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        return props;
    }

    private static BigDecimal fromBytesToDecimal(ByteBuffer buffer) {
        return new Conversions.DecimalConversion().fromBytes(
                buffer,
                Transaction.getClassSchema().getField("amount").schema(),
                LogicalTypes.decimal(12, 2)
        );
    }

    public static ByteBuffer toAmountBytes(BigDecimal amount) {
        return new Conversions.DecimalConversion().toBytes(
                amount,
                UserBalance.getClassSchema().getField("amount").schema(),
                LogicalTypes.decimal(12, 2)
        );
    }
}
