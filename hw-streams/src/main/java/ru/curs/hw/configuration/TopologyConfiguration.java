package ru.curs.hw.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.*;
import ru.curs.hw.transformer.EventTransformer;

import java.time.Duration;

import static ru.curs.counting.model.TopicNames.*;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        KStream<String, Bet> inputBet = streamsBuilder
                .stream(BET_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(Bet.class))
                        .withTimestampExtractor((record, prevTmstp) -> ((Bet) record.value()).getTimestamp()));
        KStream<String, EventScore> inputEventScore = streamsBuilder
                .stream(EVENT_SCORE_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(EventScore.class))
                        .withTimestampExtractor((record, prevTmstp) -> ((EventScore) record.value()).getTimestamp()));

        //=====part-1=====
        KTable<String, Long> bettorSumTable = makeBettorSumTable(inputBet);
        bettorSumTable.toStream().to(GAIN_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        //=====part-2=====
        KTable<String, Long> commandSumTable = makeCommandSumTable(inputBet);
        commandSumTable.toStream().to(COMMAND_BET_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        //=====part-3=====
        KStream<String, Fraud> fraudStream = makeFraudStream(streamsBuilder, inputBet, inputEventScore);
        fraudStream.to(FRAUD_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Fraud.class)));

        Topology topology = streamsBuilder.build();
        System.out.println("==========================");
        System.out.println(topology.describe());
        System.out.println("==========================");
        return topology;
    }

    private KTable<String, Long> makeBettorSumTable(KStream<String, Bet> input) {
        return input.map((k, v) -> KeyValue.pair(v.getBettor(), Math.round(v.getAmount() * v.getOdds())))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())).reduce(Long::sum, Materialized.with(Serdes.String(), Serdes.Long()));
    }

    private KTable<String, Long> makeCommandSumTable(KStream<String, Bet> input) {
        return input.filter((k, v) -> v.getOutcome() != Outcome.D)
                .map((k, v) -> {
                    if (v.getOutcome() == Outcome.H) {
                        return KeyValue.pair(k.split("-|:")[0], Math.round(v.getAmount() * v.getOdds()));
                    } else {
                        return KeyValue.pair(k.split("-|:")[1], Math.round(v.getAmount() * v.getOdds()));
                    }
                }).groupByKey(Grouped.with(Serdes.String(), Serdes.Long())).reduce(Long::sum, Materialized.with(Serdes.String(), Serdes.Long()));
    }

    private KStream<String, Fraud> makeFraudStream(StreamsBuilder sb, KStream<String, Bet> inputBet, KStream<String, EventScore> inputEventScore) {
        KStream<String, EventScore> transformingEventScoreKey = new EventTransformer().transformStream(sb, inputEventScore);

        return inputBet.join(transformingEventScoreKey,
                (bet, event) -> Fraud.builder()
                        .bettor(bet.getBettor())
                        .outcome(bet.getOutcome())
                        .amount(bet.getAmount())
                        .match(bet.getMatch())
                        .odds(bet.getOdds())
                        .lag(event.getTimestamp() - bet.getTimestamp())
                        .build(),
                JoinWindows.of(Duration.ofSeconds(1)).before(Duration.ZERO),
                StreamJoined.with(Serdes.String(), new JsonSerde<>(Bet.class), new JsonSerde<>(EventScore.class)))
                .selectKey((k, v) -> v.getBettor());
    }
}
