package com.opsd.streaming.processor;

import com.opsd.streaming.config.DetectorConfig;
import com.opsd.streaming.model.HouseholdReading;
import com.opsd.streaming.model.PeakEvent;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;

public class IqrDetectorProcessor implements Processor<String, HouseholdReading, String, PeakEvent> {

    private static final Logger log = LoggerFactory.getLogger(IqrDetectorProcessor.class);
    public static final String STORE_NAME = "iqr-window-store";
    public static final String COOLDOWN_STORE_NAME = "cooldown-store";

    private final int windowSize;
    private final int minWindow;
    private final double sigma;
    private final String detectionFeed;
    private final long cooldownMs;

    private KeyValueStore<String, Deque<Double>> store;
    private KeyValueStore<String, Long> cooldownStore;
    private ProcessorContext<String, PeakEvent> context;
    private final Percentile percentile = new Percentile();
    private long processedCount;
    private long peaksCount;

    public IqrDetectorProcessor(DetectorConfig config) {
        this.windowSize    = config.windowSize();
        this.minWindow     = config.minWindow();
        this.sigma         = config.sigma();
        this.detectionFeed = config.detectionFeed();
        this.cooldownMs    = config.cooldownMs();
    }

    @Override
    public void init(ProcessorContext<String, PeakEvent> context) {
        this.context       = context;
        this.store         = context.getStateStore(STORE_NAME);
        this.cooldownStore = context.getStateStore(COOLDOWN_STORE_NAME);
    }

    @Override
    public void process(Record<String, HouseholdReading> record) {
        HouseholdReading reading = record.value();
        if (reading == null || !detectionFeed.equals(reading.getFeed())) return;

        String key      = record.key();
        double valueKwh = reading.getValueKwh();

        // Deque: O(1) eviction at the head (ArrayList.remove(0) was an O(n) shift
        // on every record). A fixed-size primitive double[] ring buffer would
        // additionally avoid boxing and per-record allocations — future work.
        Deque<Double> window = store.get(key);
        if (window == null) window = new ArrayDeque<>(windowSize + 1);

        boolean isPeak = false;

        // evaluate-before-append: outlier cannot inflate its own threshold
        if (window.size() >= minWindow) {
            double[] arr = window.stream().mapToDouble(Double::doubleValue).toArray();
            percentile.setData(arr);
            double q3         = percentile.evaluate(75.0);
            double iqr        = q3 - percentile.evaluate(25.0);
            double upperFence = q3 + sigma * iqr;

            if (valueKwh > upperFence) {
                isPeak = true;
                long now = record.timestamp();
                Long last = cooldownStore.get(key);
                if (last == null || now - last >= cooldownMs) {
                    double level = valueKwh - upperFence;
                    context.forward(new Record<>(
                        key,
                        new PeakEvent(reading.getHousehold(), reading.getRoom(), reading.getUtcTimestamp(), level),
                        now
                    ));
                    cooldownStore.put(key, now);
                    log.debug("peak_detected key={} level={} fence={}", key, level, upperFence);
                    peaksCount++;
                }
            }
        }

        // peaks are kept out of the baseline: otherwise each anomaly inflates
        // Q3/IQR and progressively raises the fence, masking later anomalies
        if (!isPeak) {
            window.addLast(valueKwh);
            if (window.size() > windowSize) window.removeFirst();
            store.put(key, window);
        }
        processedCount++;
    }

    @Override
    public void close() {
        log.info("detector_closing processed={} peaks_emitted={}", processedCount, peaksCount);
    }
}
