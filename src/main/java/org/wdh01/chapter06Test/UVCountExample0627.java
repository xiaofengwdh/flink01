package org.wdh01.chapter06Test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.bean.UrlViewCount;
import org.wdh01.chapter05.ClickSource;

import java.time.Duration;

/**
 * 使用增量聚合函数计算 PV
 * 使用全窗口函数包装数据
 */
public class UVCountExample0627 {
    public static void main(String[] args) throws Exception {
        //获取执行环境&设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据&提取时间戳
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );
        //按 url key
        eventSingleOutputStreamOperator.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                //AggregateFunction 进行增量聚聚合
                .aggregate(new AggregateFunction<Event, Long, Long>() {
                               //增量聚合逻辑
                               @Override
                               public Long createAccumulator() {
                                   //初始值
                                   return 0L;
                               }

                               @Override
                               public Long add(Event value, Long accumulator) {
                                   //累加规则
                                   return accumulator + 1L;
                               }

                               @Override
                               public Long getResult(Long accumulator) {
                                   //获取结果
                                   return accumulator;
                               }

                               @Override
                               public Long merge(Long a, Long b) {
                                   //合并
                                   return a + b;
                               }
                           }, //使用全窗口函数包装数据
                        new ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>() {
                            /**
                             * new ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>
                             *     Long 输入数据类型
                             *     UrlViewCount 输出黄数据类型
                             *     String key 类型：data -> data.url ；url String
                             *     TimeWindow 需要一个这样的变量
                             */

                            @Override
                            public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
                                /**
                                 * String url key
                                 * Context context 上下文
                                 *  Iterable<Long> elements 数据
                                 *  Collector<UrlViewCount> out 搜集器
                                 */
                                long end = context.window().getEnd();
                                long start = context.window().getStart();
                                Long next = elements.iterator().next();
                                out.collect(new UrlViewCount(url, next, start, end));
                            }
                        }).print();

        //执行
        env.execute();
    }
}


