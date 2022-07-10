package org.wdh01.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 时间时间定时器
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> eventDS = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }));


        eventDS.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        //获取处理时间
                        long currTs = ctx.timerService().currentWatermark();
                        out.collect(ctx.getCurrentKey() + " 数据到时间戳 -> " + new Timestamp(currTs) + " watermaker " + ctx.timerService().currentWatermark());
                        //注册 10s 的定时器
                        ctx.timerService().registerEventTimeTimer(currTs + 10 * 1000);

                    }

                    //触发
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 定时器触发时间 -> " + new Timestamp(timestamp));
                    }
                }).print();



        env.execute();
    }
}
