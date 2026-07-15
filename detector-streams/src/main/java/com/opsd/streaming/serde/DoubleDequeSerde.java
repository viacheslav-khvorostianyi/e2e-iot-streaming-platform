package com.opsd.streaming.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Serde for the rolling window kept as a Deque (oldest first).
 * Wire format is raw little-endian doubles — identical to the former
 * DoubleListSerde, so existing state stores and changelog topics stay readable.
 */
public class DoubleDequeSerde implements Serde<Deque<Double>> {

    @Override
    public Serializer<Deque<Double>> serializer() {
        return (topic, data) -> {
            if (data == null || data.isEmpty()) return new byte[0];
            ByteBuffer buf = ByteBuffer.allocate(data.size() * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            for (double d : data) buf.putDouble(d);
            return buf.array();
        };
    }

    @Override
    public Deserializer<Deque<Double>> deserializer() {
        return (topic, data) -> {
            if (data == null || data.length == 0) return new ArrayDeque<>();
            ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            Deque<Double> result = new ArrayDeque<>(data.length / Double.BYTES + 1);
            while (buf.hasRemaining()) result.addLast(buf.getDouble());
            return result;
        };
    }
}