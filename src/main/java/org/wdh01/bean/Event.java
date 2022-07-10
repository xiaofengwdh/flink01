package org.wdh01.bean;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.sql.Timestamp;

/**
 * 实体类
 */
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    public Event() {
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp)  +
                '}';
    }

}
