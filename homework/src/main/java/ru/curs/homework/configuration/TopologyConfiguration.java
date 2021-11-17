package ru.curs.homework.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Fraud;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.homework.transformer.BetsOfBettorsTransformer;
import ru.curs.homework.transformer.BetsForTeamsTransformer;
import ru.curs.homework.transformer.BetsWinsTransformer;
import static ru.curs.counting.model.TopicNames.*;
import java.time.Duration;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {
    public static final String BETTOR_AMOUNTS = "bettor-amounts";
    public static final String TEAM_AMOUNTS = "team-amounts";
    public static final String POSSIBLE_FRAUDS = "possible-frauds";

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {

        /// Bets stream ///
        KStream<String, Bet> bets =
                streamsBuilder.stream(
                        BET_TOPIC,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Bet.class))
                        .withTimestampExtractor((record, previousTimestamp) -> ((Bet) record.value()).getTimestamp())
                );

        /// Scores stream ///
        KStream<String, EventScore> scores =
                streamsBuilder.stream(
                        EVENT_SCORE_TOPIC,
                        Consumed.with(Serdes.String(), new JsonSerde<>(EventScore.class))
                        .withTimestampExtractor((record, previousTimestamp) ->
                                ((EventScore) record.value()).getTimestamp())
                );

        KStream<Bet, Long> bet_count =
                bets.map(
                        (key, value) -> KeyValue.pair(value, value.getAmount())
                );

        /// Bets of bettors
        KStream<String, Long> bets_of_bettors_sum = new BetsOfBettorsTransformer().transformStream(streamsBuilder, bet_count);
        bets_of_bettors_sum.to(BETTOR_AMOUNTS, Produced.with(Serdes.String(), Serdes.Long()));

        // Bets for teams
        KStream<String, Long> bets_for_teams_sum = new BetsForTeamsTransformer().transformStream(streamsBuilder, bet_count);
        bets_for_teams_sum.to(TEAM_AMOUNTS, Produced.with(Serdes.String(), Serdes.Long()));

        /// Possible fraud ///
        KStream<String, Bet> bets_wins = new BetsWinsTransformer().transformStream(streamsBuilder, scores);
        KStream<String, Fraud> possible_fraud = bets.join(bets_wins,
                (bet, winningBet) -> Fraud.builder()
                                .bettor(bet.getBettor())
                                .outcome(bet.getOutcome())
                                .amount(bet.getAmount())
                                .match(bet.getMatch())
                                .odds(bet.getOdds())
                                .lag(winningBet.getTimestamp() - bet.getTimestamp())
                                .build(),
                JoinWindows.of(Duration.ofSeconds(1)).before(Duration.ZERO),
                StreamJoined.with(Serdes.String(), new JsonSerde<>(Bet.class), new JsonSerde<>(Bet.class))
        ).selectKey((k, fraud) -> fraud.getBettor());
        possible_fraud.to(POSSIBLE_FRAUDS, Produced.with(Serdes.String(), new JsonSerde<>(Fraud.class)));

        Topology topology = streamsBuilder.build();
        System.out.println("==============================");
        System.out.println(topology.describe());
        System.out.println("==============================");
        return topology;
    }
}
