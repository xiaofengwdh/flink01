package org.wdh01.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;

import java.time.Duration;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度
        env.setParallelism(1);
        //设置水位线生成间隔
        env.getConfig().setAutoWatermarkInterval(100);
     /*   DataStreamSource<Event> eventDataStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L),
                new Event("依琳", "/info?id=2", 8000L),
                new Event("任盈盈", "/home", 8000L),
                new Event("依琳", "/error", 100000L));*/

        env.socketTextStream("hadoop103", 9999)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                    }
                }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        ).keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String user, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long l = context.currentWatermark();
                        long exactSizeIfKnown = elements.spliterator().getExactSizeIfKnown();
                        out.collect("窗口 " + start + " ~ " + end + " 共有 " + exactSizeIfKnown + " 个元素，窗口关闭时，水位线处于 " + l);
                    }
                }).print();


    /*    //有序流 Watermark 生成策略  forMonotonousTimestamps()
        DataStream<Event> eventDStream = eventDataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forMonotonousTimestamps() //有序流 Watermark 生成策略
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override//指定时间戳列
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;//recordTimestamp 是毫秒，如果 Event timestamp 是秒 需要*1000
                                    }
                                }));

        //无序流 Watermark 生成,forBoundedOutOfOrderness(Duration.ofSeconds(2) 等2s
        DataStream<Event> eventDStream1 = eventDataStream.//无序流 Watermark 生成
                assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)) //无序流 Watermark 生成 2s延迟
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                //指定时间戳列
                                return element.timestamp;
                            }
                        }));*/

        env.execute();
    }
}
