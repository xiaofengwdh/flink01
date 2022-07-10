package org.wdh01.bean;

import java.sql.Timestamp;

public class UrlViewCount {
    public String url;
    public Long cnt;
    public Long start;
    public Long end;

    public UrlViewCount() {
    }

    public UrlViewCount(String url, Long cnt, Long start, Long end) {
        this.url = url;
        this.cnt = cnt;
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", cnt=" + cnt +
                ", TimestampStart=" + new Timestamp(start) +
                ", TimestampEnd=" + new Timestamp(end) +
                '}';
    }
}
