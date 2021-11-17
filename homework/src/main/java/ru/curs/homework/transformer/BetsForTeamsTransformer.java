package ru.curs.homework.transformer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.*;
import ru.curs.homework.util.StatefulTransformer;

import java.util.Optional;

public class BetsForTeamsTransformer implements StatefulTransformer<String, Long, Bet, Long, String, Long> {
    @Override
    public String storeName() {
        return "bets-for-teams-store";
    }

    @Override
    public Serde<String> storeKeySerde() {
        return Serdes.String();
    }

    @Override
    public Serde<Long> storeValueSerde() {
        return new JsonSerde<>(Long.class);
    }

    @Override
    public KeyValue<String, Long> transform(Bet key, Long value, KeyValueStore<String, Long> stateStore) {
        String[] names = key.getMatch().split("-");
        if (key.getOutcome() == Outcome.D) {
            return null;
        }
        String name = key.getOutcome() == Outcome.H ? names[0] : names[1];
        long bet_sum = Optional.ofNullable(stateStore.get(name)).orElse(0L) + value;
        stateStore.put(name, bet_sum);
        return KeyValue.pair(name, bet_sum);
    }
}