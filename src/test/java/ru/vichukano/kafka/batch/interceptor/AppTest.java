package ru.vichukano.kafka.batch.interceptor;

import io.github.vichukano.kafka.junit.extension.EnableKafkaQueues;
import io.github.vichukano.kafka.junit.extension.OutputQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


@EnableKafkaQueues
@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EmbeddedKafka(
    topics = {"topic-in", "topic-out"},
    brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"},
    partitions = 1
)
class AppTest {
    private static final String TOPIC_IN = "topic-in";
    private static final String TOPIC_OUT = "topic-out";
    @OutputQueue(topic = TOPIC_OUT, partitions = 1)
    private BlockingQueue<ConsumerRecord<String, String>> consumerRecords;

    @Test
    void shouldFilterRecordWithoutHeader() throws ExecutionException, InterruptedException, TimeoutException {
        final String messageIn = "hello world";
        try (var producer = producer()) {
            producer.send(new ProducerRecord<>(TOPIC_IN, messageIn)).get(5, TimeUnit.SECONDS);
        }
        ConsumerRecord<String, String> record = consumerRecords.poll(5, TimeUnit.SECONDS);
        Assertions.assertThat(record).isNull();
    }

    @Test
    void shouldPassRecord() throws ExecutionException, InterruptedException, TimeoutException {
        final String messageIn = "hello world";
        ProducerRecord<String, String> sendRecord = new ProducerRecord<>(TOPIC_IN, messageIn);
        sendRecord.headers().add(new RecordHeader("TEST", "TEST".getBytes()));
        try (var producer = producer()) {
            producer.send(sendRecord).get(5, TimeUnit.SECONDS);
        }
        ConsumerRecord<String, String> record = consumerRecords.poll(5, TimeUnit.SECONDS);
        Assertions.assertThat(record).isNotNull();
        Assertions.assertThat(record.value()).isEqualTo("hello world");
    }

    private KafkaProducer<String, String> producer() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
    }
}
