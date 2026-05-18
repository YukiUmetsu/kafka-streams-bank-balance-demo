package com.github.yukiumetsu.udemy.kafka.streams;

import com.github.yukiumetsu.kafka.avro.UserBalance;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class UserBalanceConsumer {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String TOPIC = "user-balance";
    private static final String GROUP_ID = "user-balance-consumer";

    public static void main(String[] args) {
        try (KafkaConsumer<String, UserBalance> consumer = new KafkaConsumer<>(getConsumerProperties())) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                ConsumerRecords<String, UserBalance> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, UserBalance> record : records) {
                    UserBalance userBalance = record.value();
                    BigDecimal amount = fromBytesToDecimal(userBalance.getAmount());

                    System.out.printf(
                            "key=%s, name=%s, amount=%s, time=%s, partition=%d, offset=%d%n",
                            record.key(),
                            userBalance.getName(),
                            amount,
                            userBalance.getTime(),
                            record.partition(),
                            record.offset()
                    );
                }
            }
        }
    }

    private static Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return props;
    }

    private static BigDecimal fromBytesToDecimal(ByteBuffer buffer) {
        return new Conversions.DecimalConversion().fromBytes(
                buffer,
                UserBalance.getClassSchema().getField("amount").schema(),
                LogicalTypes.decimal(12, 2)
        );
    }
}
