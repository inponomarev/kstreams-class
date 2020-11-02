package ru.curs.hw;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Assertions;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.*;
import ru.curs.hw.configuration.KafkaConfiguration;
import ru.curs.hw.configuration.TopologyConfiguration;

import static ru.curs.counting.model.TopicNames.*;

public class TestTopology {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Bet> inputBetTopic;
    private TestInputTopic<String, EventScore> inputEventScoreTopic;
    private TestOutputTopic<String, Long> outputGainTopic;
    private TestOutputTopic<String, Long> outputCommandBetTopic;
    private TestOutputTopic<String, Fraud> outputFraudTopic;

    @BeforeEach
    public void setUp() {
        KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
        StreamsBuilder sb = new StreamsBuilder();
        Topology topology = new TopologyConfiguration().createTopology(sb);

        topologyTestDriver = new TopologyTestDriver(topology, config.asProperties());
        inputBetTopic = topologyTestDriver.createInputTopic(BET_TOPIC, Serdes.String().serializer(), new JsonSerde<>(Bet.class).serializer());
        inputEventScoreTopic = topologyTestDriver.createInputTopic(EVENT_SCORE_TOPIC, Serdes.String().serializer(), new JsonSerde<>(EventScore.class).serializer());
        outputGainTopic = topologyTestDriver.createOutputTopic(GAIN_TOPIC, Serdes.String().deserializer(), Serdes.Long().deserializer());
        outputCommandBetTopic = topologyTestDriver.createOutputTopic(COMMAND_BET_TOPIC, Serdes.String().deserializer(), Serdes.Long().deserializer());
        outputFraudTopic = topologyTestDriver.createOutputTopic(FRAUD_TOPIC, Serdes.String().deserializer(), new JsonSerde<>(Fraud.class).deserializer());
    }

    @AfterEach
    public void setDown() {
        topologyTestDriver.close();
    }

    void placeBet(Bet value) {
        inputBetTopic.pipeInput(value.key(), value);
    }
    void placeEvent(EventScore event) {
        inputEventScoreTopic.pipeInput(event.getEvent(), event);
    }

    @Test
    void testTopologyBettorGainSimple() {
        placeBet(new Bet("John", "A-B", Outcome.A, 10, 1, 0));
        TestRecord<String, Long> record1 = outputGainTopic.readRecord();
        placeBet(new Bet("John", "A-B", Outcome.H, 100, 1.5, 0));
        TestRecord<String, Long> record2 = outputGainTopic.readRecord();
        placeBet(new Bet("John", "A-B", Outcome.A, 0, 1, 0));
        TestRecord<String, Long> record3 = outputGainTopic.readRecord();

        Assertions.assertTrue(outputGainTopic.isEmpty());

        Assertions.assertEquals("John", record1.key());
        Assertions.assertEquals(10L, record1.value());
        Assertions.assertEquals(160L, record2.value());
        Assertions.assertEquals(160L, record3.value());
    }

    @Test
    void testTopologyBettorGainTwoNames() throws Exception {
        placeBet(new Bet("John", "A-B", Outcome.A, 10, 1, 0));
        placeBet(new Bet("Sara", "B-T", Outcome.H, 20, 0.5, 0));
        placeBet(new Bet("John", "A-B", Outcome.H, 100, 1.5, 0));
        placeBet(new Bet("Sara", "B-T", Outcome.H, 50, 2, 0));
        placeBet(new Bet("John", "A-B", Outcome.A, 0, 1, 0));

        Long[] JohnSum = new Long[]{10L, 160L, 160L};
        Long[] SaraSum = new Long[]{10L, 110L};

        for (int i = 0, j = 0, s = 0; i < 5; ++i) {
            TestRecord<String, Long> record = outputGainTopic.readRecord();
            if (record.key().equals("John")) {
                Assertions.assertEquals(JohnSum[j], record.value());
                ++j;
            } else if (record.key().equals("Sara")) {
                Assertions.assertEquals(SaraSum[s], record.value());
                ++s;
            } else {
                throw new Exception("Problem with two name in GAIN_TOPIC");
            }
        }
        Assertions.assertTrue(outputGainTopic.isEmpty());
    }

    @Test
    void testTopologyCommandBetSimple() {
        placeBet(new Bet("John", "A-B", Outcome.H, 10, 1, 0));
        TestRecord<String, Long> record1 = outputCommandBetTopic.readRecord();
        placeBet(new Bet("John", "A-B", Outcome.H, 100, 1.5, 0));
        TestRecord<String, Long> record2 = outputCommandBetTopic.readRecord();

        Assertions.assertTrue(outputCommandBetTopic.isEmpty());

        Assertions.assertEquals("A", record1.key());
        Assertions.assertEquals(10L, record1.value());
        Assertions.assertEquals("A", record2.key());
        Assertions.assertEquals(160L, record2.value());
    }

    @Test
    void testTopologyCommandBetMany() throws Exception {
        placeBet(new Bet("John", "A-B", Outcome.A, 10, 1, 0));
        placeBet(new Bet("Sara", "B-T", Outcome.H, 20, 0.5, 0));
        placeBet(new Bet("John", "A-B", Outcome.H, 100, 1.5, 0));
        placeBet(new Bet("Sara", "B-T", Outcome.H, 50, 2, 0));
        placeBet(new Bet("John", "A-B", Outcome.H, 0, 1, 0));

        Long[] aSum = new Long[]{150L, 150L};
        Long[] bSum = new Long[]{10L, 20L, 120L};

        for (int i = 0, a = 0, b = 0; i < 5; ++i) {
            TestRecord<String, Long> record = outputCommandBetTopic.readRecord();
            if (record.key().equals("A")) {
                Assertions.assertEquals(aSum[a], record.value());
                ++a;
            } else if (record.key().equals("B")) {
                Assertions.assertEquals(bSum[b], record.value());
                ++b;
            } else {
                throw new Exception("Problem with many commands in COMMAND_BET_TOPIC");
            }
        }
        Assertions.assertTrue(outputCommandBetTopic.isEmpty());
    }

    @Test
    void testTopologyCommandBetFilterOutcomes() {
        placeBet(new Bet("John", "A-B", Outcome.H, 10, 1, 0));
        TestRecord<String, Long> record1 = outputCommandBetTopic.readRecord();

        placeBet(new Bet("John", "A-B", Outcome.D, 100, 1.5, 0));
        Assertions.assertTrue(outputCommandBetTopic.isEmpty());

        placeBet(new Bet("John", "A-B", Outcome.A, 15, 2, 0));
        TestRecord<String, Long> record2 = outputCommandBetTopic.readRecord();

        Assertions.assertTrue(outputCommandBetTopic.isEmpty());

        Assertions.assertEquals("A", record1.key());
        Assertions.assertEquals(10L, record1.value());
        Assertions.assertEquals("B", record2.key());
        Assertions.assertEquals(30L, record2.value());
    }

    @Test
    void testTopologyFraud() {
        long currentTimestamp = System.currentTimeMillis();

        placeEvent(new EventScore("A-B", new Score().goalHome(), currentTimestamp));
        placeBet(new Bet("John", "A-B", Outcome.H, 1, 1, currentTimestamp - 2000));
        placeBet(new Bet("Sara", "A-B", Outcome.H, 1, 1, currentTimestamp - 10));
        placeBet(new Bet("Sara", "A-B", Outcome.A, 1, 1, currentTimestamp - 20));

        Fraud expected = Fraud.builder()
                .bettor("Sara").match("A-B").outcome(Outcome.H).amount(1).odds(1)
                .lag(10)
                .build();

        TestRecord<String, Fraud> record = outputFraudTopic.readRecord();

        Assertions.assertTrue(outputFraudTopic.isEmpty());
        Assertions.assertEquals("Sara", record.key());
        Assertions.assertEquals(expected, record.value());
    }

    @Test
    void testTopologyFraudManyEvent() {
        long currentTimestamp = System.currentTimeMillis();

        Score score = new Score().goalHome();
        placeEvent(new EventScore("A-B", score, currentTimestamp));

        score = score.goalHome();
        placeEvent(new EventScore("A-B", score, currentTimestamp + 100 * 1000));

        score = score.goalAway();
        placeEvent(new EventScore("A-B", score, currentTimestamp + 200 * 1000));

        placeBet(new Bet("John", "A-B", Outcome.H, 1, 1, currentTimestamp - 2000));
        placeBet(new Bet("Sara", "A-B", Outcome.H, 1, 1, currentTimestamp + 100 * 1000 - 10));
        placeBet(new Bet("Sara", "A-B", Outcome.A, 1, 1, currentTimestamp + 200 * 1000 - 20));

        Fraud expected1 = Fraud.builder()
                .bettor("Sara").match("A-B").outcome(Outcome.H).amount(1).odds(1)
                .lag(10)
                .build();
        Fraud expected2 = Fraud.builder()
                .bettor("Sara").match("A-B").outcome(Outcome.A).amount(1).odds(1)
                .lag(20)
                .build();

        TestRecord<String, Fraud> record1 = outputFraudTopic.readRecord();
        TestRecord<String, Fraud> record2 = outputFraudTopic.readRecord();

        Assertions.assertTrue(outputFraudTopic.isEmpty());
        Assertions.assertEquals("Sara", record1.key());
        Assertions.assertEquals(expected1, record1.value());
        Assertions.assertEquals("Sara", record2.key());
        Assertions.assertEquals(expected2, record2.value());
    }
}
