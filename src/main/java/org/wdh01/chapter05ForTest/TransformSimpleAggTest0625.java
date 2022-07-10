package org.wdh01.chapter05ForTest;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;

public class TransformSimpleAggTest0625 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L),
                new Event("依琳", "/info?id=2", 8000L),
                new Event("任盈盈", "/home", 8000L),
                new Event("依琳", "/error", 100000L)
        );
        /**
         * maxBy ：最新的一条记录
         * max ：最新的一条记录的时间错+第一次出现的记录
         */
        eventDStream.keyBy(data -> data.user).maxBy("timestamp").print();
        eventDStream.keyBy(data -> data.user).max("timestamp").print();

        env.execute();
    }
}
