package ru.vichukano.kafka.batch.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import java.util.List;
import java.util.function.Function;

@Slf4j
@SpringBootApplication
public class App {

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Bean
    public Function<List<String>, String> processor() {
        return in -> {
            log.info("Message in: {}", in);
            final String out = String.join("", in);
            log.info("Message out: {}", out);
            return out;
        };
    }

}
