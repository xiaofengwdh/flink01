package org.wdh01.chapter08test;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class BillCheckTest0704 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //支付数据
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appDS = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-3", "app", 3500L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                })
                );
        //平台数据
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> ptDS = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                                        return element.f3;
                                    }
                                })
                );
        //检查同一订单ID 是否完全匹配，不匹配报警
        appDS.connect(ptDS).
                keyBy(data -> data.f0, data -> data.f0)
                .process(new OrderMatchFun())
                .print();


        env.execute();

    }

    //自定义实现 CoProcessFunction
    public static class OrderMatchFun extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        //定义状态变量，用来保存到达的事件
        private ValueState<Tuple3<String, String, Long>> appEvent;
        private ValueState<Tuple4<String, String, String, Long>> ptEvent;

        //获取状态实例
        @Override
        public void open(Configuration parameters) throws Exception {
            appEvent = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );

            ptEvent = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>("pt-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
            );

        }


        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
//app-event,看 另一条数据是否来过
            if (ptEvent.value() != null) {
                out.collect("对账成功 " + value + " " + ptEvent.value());
                //清空状态
                ptEvent.clear();
            } else {
                //更新状态
                appEvent.update(value);
                //定义注册定时器，开始等待另一条数据  等待5s
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            if (appEvent.value() != null) {
                out.collect("对账成功 " + appEvent.value() + " " + value);
                //清空状态
                appEvent.clear();
            } else {
                //更新状态
                ptEvent.update(value);
                //定义注册定时器，开始等待另一条数据  等待5s
                ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，判断某个状态不为空，说明另一条流中的事件没来
            if (appEvent.value() != null) {
                out.collect("对账失败 " + appEvent.value() + " 第三方数据缺失");
            } else if (ptEvent.value() != null) {
                out.collect("对账失败 " + ptEvent.value() + " app 数据缺失");
            }
            //清空所有状态
            appEvent.clear();
            ptEvent.clear();
        }
    }
}

