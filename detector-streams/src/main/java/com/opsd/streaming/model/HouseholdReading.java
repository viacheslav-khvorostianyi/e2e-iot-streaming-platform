package com.opsd.streaming.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HouseholdReading {
    @JsonProperty("household")     private String household;
    @JsonProperty("room")          private String room;
    @JsonProperty("feed")          private String feed;
    @JsonProperty("utc_timestamp") private String utcTimestamp;
    @JsonProperty("value_kwh")     private double valueKwh;

    public HouseholdReading() {}

    public String getHousehold()    { return household; }
    public String getRoom()         { return room; }
    public String getFeed()         { return feed; }
    public String getUtcTimestamp() { return utcTimestamp; }
    public double getValueKwh()     { return valueKwh; }
}
