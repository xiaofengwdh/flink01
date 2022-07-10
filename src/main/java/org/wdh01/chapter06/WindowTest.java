package org.wdh01.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.time.Duration;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度
        env.setParallelism(1);
        //设置水位线生成间隔
        env.getConfig().setAutoWatermarkInterval(100);
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        //无序水位线：延迟2s
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            //指定水位线的字段：这里从 timestamp 读取时间信息
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));


        /**
         * 滚动事件时间窗口
         * 窗口大小 10 min ,偏移 0
         */
        eventStream.map(data -> Tuple2.of(data.user, 1L))
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }) ;
        /**
         * 滚动事件时间窗口
         * 窗口大小 10 min ,偏移 2 min
         */
        WindowedStream<Event, String, TimeWindow> window2 =
                eventStream.keyBy(data -> data.user)
                        .window(TumblingEventTimeWindows.of(Time.minutes(10), Time.minutes(2)));

        /**
         * 滑动事件时间窗口
         * 窗口大小 30 min,步长 5 min 无偏移量
         * window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(10)))
         */
        WindowedStream<Event, String, TimeWindow> window1 = eventStream.keyBy(data -> data.user)
                .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(10)));
         /**
         * 滑动事件时间窗口
         * 窗口大小 30 min,步长 5 min 偏移量 -8h
         * SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(5), Time.hours(8)
         */
        WindowedStream<Event, String, TimeWindow> window11 = eventStream.keyBy(data -> data.user)
                .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(5), Time.hours(-8)));
        //事件时间会话窗口：窗口大小 5s
        WindowedStream<Event, String, TimeWindow> window3 =
                eventStream.keyBy(data -> data.user)
                        .window(EventTimeSessionWindows.withGap(Time.seconds(5)));
        //滑动计数窗口 窗口大小 10  ：滑动步长 2
        WindowedStream<Event, String, GlobalWindow> eventStringGlobalWindowWindowedStream =
                eventStream.keyBy(data -> data.user)
                        .countWindow(10, 2);
        env.execute();
    }
}
