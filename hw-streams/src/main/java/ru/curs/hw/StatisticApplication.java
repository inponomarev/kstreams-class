package ru.curs.hw;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class StatisticApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(StatisticApplication.class).headless(true).run(args);
    }

}
