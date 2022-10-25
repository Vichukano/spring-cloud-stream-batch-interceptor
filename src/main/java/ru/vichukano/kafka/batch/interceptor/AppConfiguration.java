package ru.vichukano.kafka.batch.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.BatchInterceptor;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
@Slf4j(topic = "ru.vichukano.kafka.batch.interceptor")
public class AppConfiguration {

    @Bean
    public ListenerContainerCustomizer<AbstractMessageListenerContainer<String, String>> customizer(
        BatchInterceptor<String, String> customInterceptor
    ) {
        return (((container, destinationName, group) -> {
            container.setBatchInterceptor(customInterceptor);
            log.info("Container customized");
        }));
    }

    @Bean
    public BatchInterceptor<String, String> customInterceptor() {
        return (consumerRecords, consumer) -> {
            log.info("Origin records count: {}", consumerRecords.count());
            final Set<TopicPartition> partitions = consumerRecords.partitions();
            final Map<TopicPartition, List<ConsumerRecord<String, String>>> filteredByHeader
                = Stream.of(partitions).flatMap(Collection::stream)
                .collect(Collectors.toMap(
                    Function.identity(),
                    p -> Stream.ofNullable(consumerRecords.records(p))
                        .flatMap(Collection::stream)
                        .filter(r -> Objects.nonNull(r.headers().lastHeader("TEST")))
                        .collect(Collectors.toList())
                ));
            var filteredRecords = new ConsumerRecords<>(filteredByHeader);
            log.info("Filtered count: {}", filteredRecords.count());
            return filteredRecords;
        };
    }

}
