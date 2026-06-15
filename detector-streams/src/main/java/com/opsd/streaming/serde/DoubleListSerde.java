package com.opsd.streaming.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class DoubleListSerde implements Serde<List<Double>> {

    @Override
    public Serializer<List<Double>> serializer() {
        return (topic, data) -> {
            if (data == null || data.isEmpty()) return new byte[0];
            ByteBuffer buf = ByteBuffer.allocate(data.size() * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            for (double d : data) buf.putDouble(d);
            return buf.array();
        };
    }

    @Override
    public Deserializer<List<Double>> deserializer() {
        return (topic, data) -> {
            if (data == null || data.length == 0) return new ArrayList<>();
            ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            List<Double> result = new ArrayList<>(data.length / Double.BYTES);
            while (buf.hasRemaining()) result.add(buf.getDouble());
            return result;
        };
    }
}
