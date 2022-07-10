package org.wdh01.chapter05ForTest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;

public class TransformFlatMaprTest0614 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L),
                new Event("依琳", "/info?id=2", 8000L),
                new Event("任盈盈", "/home", 8000L),
                new Event("依琳", "/error", 100000L)
        );

 /*       eventDStream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                if (value.user.equals("依琳")) {
                out.collect(value.toString());
            }{
                out.collect(value.user+" " + value.url);
            }
            }
        }).print();*/
        eventDStream.flatMap((Event value, Collector<String> out) -> {
            if (value.user.equals("依琳")) {
                out.collect(value.toString());
            }
            {
                out.collect(value.user + " " + value.url);
            }
        }).returns(new TypeHint<String>() {
        }).print();


        env.execute();
    }
}
