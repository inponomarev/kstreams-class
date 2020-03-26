package ru.curs.windows.transformer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Outcome;
import ru.curs.counting.model.Score;
import ru.curs.windows.util.StatefulTransformer;

import java.util.Optional;

public class ScoreTransformer implements StatefulTransformer<String, Score,
        String, EventScore, String, Bet> {
    @Override
    public String storeName() {
        return "score-cache";
    }

    @Override
    public Serde<String> storeKeySerde() {
        return Serdes.String();
    }

    @Override
    public Serde<Score> storeValueSerde() {
        return new JsonSerde<>(Score.class);
    }

    @Override
    public KeyValue<String, Bet> transform(String key, EventScore value, KeyValueStore<String, Score> stateStore) {
        //TODO: implement!
        return KeyValue.pair(null, null);
    }
}
