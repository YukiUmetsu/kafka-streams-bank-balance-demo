package com.github.yukiumetsu.udemy.kafka.streams;

import com.github.yukiumetsu.kafka.avro.Transaction;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransactionProducerTest {

    @Test
    void producerPropertiesEnableIdempotence() {
        Properties props = TransactionProducer.producerProperties();

        assertEquals("127.0.0.1:9092", props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("all", props.getProperty(ProducerConfig.ACKS_CONFIG));
        assertEquals(3, props.get(ProducerConfig.RETRIES_CONFIG));
        assertEquals("true", props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("http://localhost:8081", props.getProperty("schema.registry.url"));
    }

    @Test
    void newTransactionPreservesBusinessFields() {
        BigDecimal amount = new BigDecimal("123.45");
        Instant time = Instant.parse("2024-01-01T10:15:30Z");

        Transaction transaction = TransactionProducer.newTransaction("alice", amount, time);

        assertEquals("alice", transaction.getName());
        assertEquals(time, transaction.getTime());
        assertEquals(amount, fromAmountBytes(transaction.getAmount()));
    }

    @Test
    void newProducerRecordTargetsInputTopicAndUsesNameAsKey() {
        Transaction transaction = TransactionProducer.newTransaction(
                "bob",
                new BigDecimal("99.99"),
                Instant.parse("2024-02-01T00:00:00Z")
        );

        ProducerRecord<String, Transaction> record = TransactionProducer.newProducerRecord(transaction);

        assertEquals("bank-transactions", record.topic());
        assertEquals("bob", record.key());
        assertEquals(transaction, record.value());
    }

    private static BigDecimal fromAmountBytes(ByteBuffer amountBytes) {
        return new Conversions.DecimalConversion().fromBytes(
                amountBytes,
                Transaction.getClassSchema().getField("amount").schema(),
                LogicalTypes.decimal(12, 2)
        );
    }
}
