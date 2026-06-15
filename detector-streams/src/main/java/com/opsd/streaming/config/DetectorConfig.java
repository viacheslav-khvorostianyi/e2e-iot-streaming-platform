package com.opsd.streaming.config;

public class DetectorConfig {
    private final String bootstrapServers;
    private final String inputTopic;
    private final String outputTopic;
    private final String applicationId;
    private final int windowSize;
    private final int minWindow;
    private final double sigma;
    private final String detectionFeed;

    public DetectorConfig() {
        bootstrapServers = env("BOOTSTRAP_SERVERS", "broker:9092");
        inputTopic       = env("INPUT_TOPIC",       "household.power.readings");
        outputTopic      = env("OUTPUT_TOPIC",       "household.power.peaks");
        applicationId    = env("APPLICATION_ID",     "peak-detector-streams");
        windowSize       = Integer.parseInt(env("WINDOW_SIZE", "200"));
        minWindow        = Integer.parseInt(env("MIN_WINDOW",  "30"));
        sigma            = Double.parseDouble(env("SIGMA",     "1.5"));
        detectionFeed    = env("DETECTION_FEED",    "grid_import");
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : fallback;
    }

    public String bootstrapServers() { return bootstrapServers; }
    public String inputTopic()       { return inputTopic; }
    public String outputTopic()      { return outputTopic; }
    public String applicationId()    { return applicationId; }
    public int    windowSize()       { return windowSize; }
    public int    minWindow()        { return minWindow; }
    public double sigma()            { return sigma; }
    public String detectionFeed()    { return detectionFeed; }
}
