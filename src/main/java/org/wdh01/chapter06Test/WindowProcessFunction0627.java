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

/**
 * 统计每10s 的 UV
 */
public class WindowProcessFunction0627 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
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
                );

        //方案1 使用全量窗口函数实现：10s 输出一次Uv
        eventStream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new MyProcessWindowFun0627()).print();

        //execut
        env.execute();
    }

    public static class MyProcessWindowFun0627 extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {


        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            //声明用户数据的结合
            HashSet<String> uSet = new HashSet<>();
            //循环保存
            for (Event element : elements) {
                uSet.add(element.user);
            }
            //获取 Uv
            Long aLong = Long.valueOf(uSet.size());
//获取窗口开始结束时间
            long end = context.window().getEnd();
            long start = context.window().getStart();
            out.collect("窗口  " + new Timestamp(start)+ " ~ " + new Timestamp(end) + "  --> " +aLong);
        }
    }
}
