package org.wdh01.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;

/**
 * 自定义source
 */
public class SourceCustmomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> eventSource = environment.addSource(new ClickSource());

        eventSource.print();
        environment.execute();
    }
}
