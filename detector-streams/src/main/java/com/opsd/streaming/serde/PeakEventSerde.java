package com.opsd.streaming.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opsd.streaming.model.PeakEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class PeakEventSerde implements Serde<PeakEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Serializer<PeakEvent> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try { return MAPPER.writeValueAsBytes(data); }
            catch (Exception e) { throw new RuntimeException(e); }
        };
    }

    @Override
    public Deserializer<PeakEvent> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try { return MAPPER.readValue(data, PeakEvent.class); }
            catch (Exception e) { throw new RuntimeException(e); }
        };
    }
}
