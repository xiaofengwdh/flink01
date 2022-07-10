package org.wdh01.chapter06Test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class WindowProcessFunction0628 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                ).keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Event, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        long end = context.window().getEnd();
                        long staerr = context.window().getStart();
                        HashSet<String> strings = new HashSet<>();
                        for (Event element : elements) {
                            strings.add(element.user);
                        }

                        Long aLong = Long.valueOf(strings.size());
                        out.collect("窗口  " + new Timestamp(staerr)+ " ~ " + new Timestamp(end) + "  --> " +aLong);

                    }
                }).print();

        env.execute();
    }
}
