package com.github.yukiumetsu.udemy.kafka.streams;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.yukiumetsu.kafka.avro.Transaction;

public class TransactionProducer {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionProducer.class);

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String INPUT_TOPIC = "bank-transactions";
    private static final String OUTPUT_TOPIC = "user-balance";
    private static final int PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 1; // use 3 in a real 3-broker cluster
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final List<String> NAMES = List.of(
            "alice", "bob", "charlie", "david", "emma",
            "frank", "grace", "henry", "ivy", "jack"
    );


    public static void main(String[] args) {
        createTopicIfMissing(INPUT_TOPIC);
        createTopicIfMissing(OUTPUT_TOPIC);
        Producer<String, Transaction> producer = createProducer();
        produceRandomTransactions(producer);
    }

    private static void produceRandomTransactions(Producer<String, Transaction> producer) {
        try(producer) {
            while (true) {
                String name = NAMES.get(ThreadLocalRandom.current().nextInt(NAMES.size()));
                Transaction transaction = Transaction
                        .newBuilder()
                        .setName(name)
                        .setAmount(getRandomAmountBytes())
                        .setTime(Instant.now())
                        .build();
                ProducerRecord<String, Transaction> record = new ProducerRecord<>(INPUT_TOPIC, name, transaction);
                producer.send(record, (recordMetadata, e) -> {
                    if (e != null) {
                        LOG.error("Failed to send record for name={}", name, e);
                        return;
                    }
                    LOG.info("Produced record topic={} partition={} offset={}",
                            recordMetadata.topic(),
                            recordMetadata.partition(),
                            recordMetadata.offset());
                });
                Thread.sleep(50);
            }

        } catch (Exception e) {
            LOG.error("Producer loop failed", e);
        }
    }

    public static Producer<String, Transaction> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        return new KafkaProducer<>(props);
    }

    public static void createTopicIfMissing(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("topic is null");
        }

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try(AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic newTopic = new NewTopic(topic, PARTITIONS, REPLICATION_FACTOR);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (Exception e) {
            if (e instanceof TopicExistsException || e.getCause() instanceof TopicExistsException) {
                // topic already exists, so do nothing
                return;
            }
            LOG.error("Failed to create topic {}", topic, e);
        }
    }

    private static final LogicalTypes.Decimal AMOUNT_DECIMAL_TYPE =
            LogicalTypes.decimal(12, 2);

    private static final Conversions.DecimalConversion DECIMAL_CONVERSION =
            new Conversions.DecimalConversion();

    private static BigDecimal randomAmount() {
        double raw = ThreadLocalRandom.current().nextDouble(0.01, 1000.00);
        return BigDecimal.valueOf(raw).setScale(2, RoundingMode.HALF_UP);
    }

    public static ByteBuffer toAmountBytes(BigDecimal amount) {
        return DECIMAL_CONVERSION.toBytes(
                amount,
                Transaction.getClassSchema().getField("amount").schema(),
                AMOUNT_DECIMAL_TYPE
        );
    }

    private static ByteBuffer getRandomAmountBytes() {
        return toAmountBytes(randomAmount());
    }
}
