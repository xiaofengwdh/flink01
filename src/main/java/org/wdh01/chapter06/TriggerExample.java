package org.wdh01.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.bean.UrlViewCount;
import org.wdh01.chapter05.ClickSource;

import java.time.Duration;

public class TriggerExample {
    public static void main(String[] args) throws Exception {
        //获取执行环境，设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //湖区数据，提取时间戳
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                ).keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new Trigger<Event, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        ValueState<Boolean> isFirst = ctx.getPartitionedState(
                                new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN()));
                        if (isFirst.value() == null) {
                            for (Long l = window.getStart(); l < window.getEnd(); l = l + 1000L) {
                                ctx.registerEventTimeTimer(l);
                            }
                            isFirst.update(true);
                        }
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        ValueState<Boolean> isFirst = ctx.getPartitionedState(
                                new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN()));
                        isFirst.clear();
                    }
                })
                .process(new ProcessWindowFunction<Event, UrlViewCount, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Event> elements, Collector<UrlViewCount> out) throws Exception {

                        out.collect(new UrlViewCount(s, elements.spliterator().getExactSizeIfKnown(),
                                context.window().getStart(), context.window().getEnd()));
                    }
                }).print();


        env.execute();
    }
}
