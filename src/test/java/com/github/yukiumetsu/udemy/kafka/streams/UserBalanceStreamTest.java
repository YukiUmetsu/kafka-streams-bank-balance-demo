package com.github.yukiumetsu.udemy.kafka.streams;

import com.github.yukiumetsu.kafka.avro.Transaction;
import com.github.yukiumetsu.kafka.avro.UserBalance;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserBalanceStreamTest {

    @Test
    void streamPropertiesEnableExactlyOnceV2() {
        Properties props = UserBalanceStream.streamProperties();

        assertEquals("user-balance-stream", props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("127.0.0.1:9092", props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("exactly_once_v2", props.getProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        assertEquals("http://localhost:8081", props.getProperty("schema.registry.url"));
    }

    @Test
    void aggregateUserBalanceAddsAmountAndKeepsLatestTimestamp() {
        UserBalance current = UserBalance.newBuilder()
                .setName("alice")
                .setAmount(UserBalanceStream.toAmountBytes(new BigDecimal("10.00")))
                .setTime(Instant.parse("2024-01-01T00:00:00Z"))
                .build();
        Transaction incoming = TransactionProducer.newTransaction(
                "alice",
                new BigDecimal("2.50"),
                Instant.parse("2024-01-02T00:00:00Z")
        );

        UserBalance updated = UserBalanceStream.aggregateUserBalance("alice", incoming, current);

        assertEquals("alice", updated.getName());
        assertEquals(new BigDecimal("12.50"), decimal(updated));
        assertEquals(Instant.parse("2024-01-02T00:00:00Z"), updated.getTime());
    }

    @Test
    void topologyProducesRunningBalancesPerUser() {
        Properties props = UserBalanceStream.streamProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-balance-stream-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put("schema.registry.url", "mock://user-balance-stream-test");

        SpecificAvroSerde<Transaction> transactionSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<UserBalance> userBalanceSerde = new SpecificAvroSerde<>();
        Map<String, String> serdeConfig = Map.of("schema.registry.url", "mock://user-balance-stream-test");
        transactionSerde.configure(serdeConfig, false);
        userBalanceSerde.configure(serdeConfig, false);

        try (TopologyTestDriver driver = new TopologyTestDriver(UserBalanceStream.buildTopology(serdeConfig), props)) {
            TestInputTopic<String, Transaction> inputTopic = driver.createInputTopic(
                    "bank-transactions",
                    Serdes.String().serializer(),
                    transactionSerde.serializer()
            );
            TestOutputTopic<String, UserBalance> outputTopic = driver.createOutputTopic(
                    "user-balance",
                    Serdes.String().deserializer(),
                    userBalanceSerde.deserializer()
            );

            inputTopic.pipeInput("alice", TransactionProducer.newTransaction(
                    "alice",
                    new BigDecimal("5.00"),
                    Instant.parse("2024-01-01T00:00:00Z")
            ));
            inputTopic.pipeInput("alice", TransactionProducer.newTransaction(
                    "alice",
                    new BigDecimal("7.25"),
                    Instant.parse("2024-01-01T01:00:00Z")
            ));

            UserBalance first = outputTopic.readValue();
            UserBalance second = outputTopic.readValue();

            assertEquals(new BigDecimal("5.00"), decimal(first));
            assertEquals(new BigDecimal("12.25"), decimal(second));
            assertEquals(Instant.parse("2024-01-01T01:00:00Z"), second.getTime());
            assertTrue(outputTopic.isEmpty());
        }
    }

    private static BigDecimal decimal(UserBalance userBalance) {
        return UserBalanceStream.fromBytesToDecimal(userBalance.getAmount());
    }
}
