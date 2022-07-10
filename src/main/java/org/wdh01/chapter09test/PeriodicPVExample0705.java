package org.wdh01.chapter09test;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.time.Duration;

/**
 * 使用值状态计算PV，定时器周期输出结果
 */
public class PeriodicPVExample0705 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );
        System.out.println(" eventStream -> " + eventStream);
        //按用户统计‘
        eventStream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {

                    //定义状态，保存当前PV
                    ValueState<Long> cntState;
                    ValueState<Long> timeKeyState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //获取状态句柄
                        cntState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("cnt-state", Long.class));
                        timeKeyState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-key-state", Long.class));
                    }

                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        //每来一条数据，更新状态
                        Long cnt = cntState.value();
                        cntState.update(cnt == null ? 1L : cnt + 1L);
                        //注册定时器，没有注册定时器，就去注册，否则不注册
                        if (timeKeyState.value() == null) {
                            //注册基于当前 value 10s 后的定时器
                            ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                            timeKeyState.update(value.timestamp + 10 * 1000L);
                        }

                    }

                    //触发输出
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " pv-> " + cntState.value());
                        //清空
                        timeKeyState.clear();
                        //注册基于当前 value 10s 后的定时器
                        ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
                        timeKeyState.update(timestamp + 10 * 1000L);
                    }
                })
                .print();


        env.execute();

    }
}
