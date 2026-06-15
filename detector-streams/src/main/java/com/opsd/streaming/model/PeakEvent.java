package com.opsd.streaming.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PeakEvent {
    @JsonProperty("household") private final String household;
    @JsonProperty("room")      private final String room;
    @JsonProperty("datetime")  private final String datetime;
    @JsonProperty("level")     private final double level;

    public PeakEvent(String household, String room, String datetime, double level) {
        this.household = household;
        this.room      = room;
        this.datetime  = datetime;
        this.level     = level;
    }

    public String getHousehold() { return household; }
    public String getRoom()      { return room; }
    public String getDatetime()  { return datetime; }
    public double getLevel()     { return level; }
}
