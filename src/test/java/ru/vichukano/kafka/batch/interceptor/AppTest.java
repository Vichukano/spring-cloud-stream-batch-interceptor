package ru.vichukano.kafka.batch.interceptor;

import io.github.vichukano.kafka.junit.extension.EnableKafkaQueues;
import io.github.vichukano.kafka.junit.extension.InputQueue;
import io.github.vichukano.kafka.junit.extension.OutputQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
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
    @InputQueue
    private BlockingQueue<ProducerRecord<String, String>> producerRecords;
    @OutputQueue(topic = TOPIC_OUT, partitions = 1)
    private BlockingQueue<ConsumerRecord<String, String>> consumerRecords;

    @Test
    void shouldFilterRecordWithoutHeader() throws InterruptedException {
        final String messageIn = "hello world";
        producerRecords.offer(new ProducerRecord<>(TOPIC_IN, messageIn), 5, TimeUnit.SECONDS);
        ConsumerRecord<String, String> record = consumerRecords.poll(5, TimeUnit.SECONDS);
        Assertions.assertThat(record).isNull();
    }

    @Test
    void shouldPassRecord() throws InterruptedException {
        final String messageIn = "hello world";
        ProducerRecord<String, String> sendRecord = new ProducerRecord<>(TOPIC_IN, messageIn);
        sendRecord.headers().add(new RecordHeader("TEST", "TEST".getBytes()));
        producerRecords.offer(sendRecord, 5, TimeUnit.SECONDS);
        ConsumerRecord<String, String> record = consumerRecords.poll(5, TimeUnit.SECONDS);
        Assertions.assertThat(record).isNotNull();
        Assertions.assertThat(record.value()).isEqualTo("hello world");
    }
}
