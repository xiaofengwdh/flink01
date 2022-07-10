package org.wdh01.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;

import java.time.Duration;

public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> stream = env.fromElements(
                Tuple2.of("依琳", 5000L),
                Tuple2.of("令狐冲", 5000L),
                Tuple2.of("依琳", 20000L),
                Tuple2.of("令狐冲", 20000L),
                Tuple2.of("依琳", 51000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }));
        SingleOutputStreamOperator<Event> stream1 = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 36000L),
                new Event("依琳", "/info?id=2", 30000L),
                new Event("任盈盈", "/home", 23000L),
                new Event("依琳", "/error", 33000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long recordTimestamp) {
                                return event.timestamp;
                            }
                        }));

        stream.keyBy(data -> data.f0)
                .intervalJoin(stream1.keyBy(data -> data.user))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> left, Event right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(right + " -> " + left);
                    }
                }).print();

        env.execute();
    }
}
