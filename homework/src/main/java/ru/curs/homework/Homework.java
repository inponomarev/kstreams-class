package ru.curs.homework;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class Homework {
    private Homework() {
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder(Homework.class).headless(false).run(args);
    }

}
