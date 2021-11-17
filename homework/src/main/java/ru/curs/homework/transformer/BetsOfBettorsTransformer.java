package ru.curs.homework.transformer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.homework.util.StatefulTransformer;

import java.util.Optional;

public class BetsOfBettorsTransformer implements StatefulTransformer<String, Long, Bet, Long, String, Long> {
    @Override
    public String storeName() {
        return "bets-of-bettors-store";
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
        long bet_sum = Optional.ofNullable(stateStore.get(key.getBettor())).orElse(0L) + value;
        stateStore.put(key.getBettor(), bet_sum);
        return KeyValue.pair(key.getBettor(), bet_sum);
    }
}
