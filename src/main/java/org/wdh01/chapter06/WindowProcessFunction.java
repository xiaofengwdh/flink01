package org.wdh01.chapter06;


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

/**
 * 每10s 统计一次 uv
 */
public class WindowProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置全局并行度
        env.setParallelism(1);
        //设置水位线生成间隔
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));


        //ProcessWindowFunction 计算 UV
        eventStream.keyBy(data -> "true")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new MyProcessWindowFunction()).print();


        env.execute();

    }

    //实现自定义 ProcessWindowFunction =》 窗口 Uv
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Event, String, String, TimeWindow> {

       /* @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {

        }*/

        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            // hashset 做剔重
            HashSet<String> userSet = new HashSet<>();
            //从elements 获取数据
            for (Event element : elements) {
                userSet.add(element.user);
            }
            Long uv = Long.valueOf(userSet.size());
            //结合窗口信息
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect("窗口 " + new Timestamp(start) + "~" + new Timestamp(end) + " --> " + uv);
        }
    }

}

