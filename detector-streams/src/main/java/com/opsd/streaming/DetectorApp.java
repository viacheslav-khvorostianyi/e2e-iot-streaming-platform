package com.opsd.streaming;

import com.opsd.streaming.config.DetectorConfig;
import com.opsd.streaming.model.HouseholdReading;
import com.opsd.streaming.model.PeakEvent;
import com.opsd.streaming.processor.IqrDetectorProcessor;
import com.opsd.streaming.serde.DoubleListSerde;
import com.opsd.streaming.serde.HouseholdReadingSerde;
import com.opsd.streaming.serde.PeakEventSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class DetectorApp {
    private static final Logger log = LoggerFactory.getLogger(DetectorApp.class);

    public static void main(String[] args) {
        DetectorConfig config = new DetectorConfig();
        Topology topology = buildTopology(config);
        Properties props  = buildProperties(config);

        KafkaStreams streams = new KafkaStreams(topology, props);
        log.info("detector_starting input={} output={} sigma={} window={}/{}",
                config.inputTopic(), config.outputTopic(), config.sigma(),
                config.minWindow(), config.windowSize());
        streams.setStateListener((newState, oldState) ->
            log.info("state_transition old={} new={}", oldState, newState)
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting_down");
            streams.close(Duration.ofSeconds(30));
        }));

        streams.start();
    }

    static Topology buildTopology(DetectorConfig config) {
        Topology topology = new Topology();

        topology.addSource(
            "readings-source",
            new StringDeserializer(),
            new HouseholdReadingSerde().deserializer(),
            config.inputTopic()
        );

        topology.addProcessor(
            "iqr-detector",
            () -> new IqrDetectorProcessor(config),
            "readings-source"
        );

        StoreBuilder<KeyValueStore<String, List<Double>>> storeBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(IqrDetectorProcessor.STORE_NAME),
                Serdes.String(),
                new DoubleListSerde()
            );
        topology.addStateStore(storeBuilder, "iqr-detector");

        topology.addSink(
            "peaks-sink",
            config.outputTopic(),
            new StringSerializer(),
            new PeakEventSerde().serializer(),
            "iqr-detector"
        );

        return topology;
    }

    static Properties buildProperties(DetectorConfig config) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,           config.applicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,        config.bootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,       1000);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,       2);
        props.put(StreamsConfig.STATE_DIR_CONFIG,                "/var/lib/kafka-streams");
        props.put("auto.offset.reset",                           "earliest");
        return props;
    }
}
