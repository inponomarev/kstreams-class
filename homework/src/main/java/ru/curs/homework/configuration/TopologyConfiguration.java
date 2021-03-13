package ru.curs.homework.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {
    public static final String BETTOR_AMOUNTS = "bettor-amounts";
    public static final String TEAM_AMOUNTS = "team-amounts";
    public static final String POSSIBLE_FRAUDS = "possible-frauds";

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {

        /*
        Необходимо создать топологию, которая имеет следующие три выходных топика:
           -- таблица, ключом которой является имя игрока,
           а значением -- сумма ставок, произведённых игроком
           -- таблица, ключом которой является имя команды,
            а значением -- сумма ставок на эту команду (ставки на "ничью" в подсчёте игнорируются)
           -- поток, ключом которого является имя игрока,
           а значениями -- подозрительные ставки.
           Подозрительными считаем ставки, произведённые в пользу команды
           в пределах одной секунды до забития этой командой гола.
         */
        Topology topology = streamsBuilder.build();
        System.out.println("==============================");
        System.out.println(topology.describe());
        System.out.println("==============================");
        // https://zz85.github.io/kafka-streams-viz/
        return topology;
    }
}
