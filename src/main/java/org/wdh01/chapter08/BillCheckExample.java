package org.wdh01.chapter08;

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

public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //定义app支付数据
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("1000", "app", 1000L),
                Tuple3.of("1001", "app", 2000L),
                Tuple3.of("1003", "app", 3500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.
                <Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));
        //定义 第三方支付数据
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> stream3 = env.fromElements(
                Tuple4.of("1000", "third-party", "success", 100L),
                Tuple4.of("1001", "third-party", "success", 200L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                                return element.f3;
                            }
                        }));
        //appStream.keyBy(data -> data.f0).connect(stream3.keyBy(data -> data.f0));
        appStream.connect(stream3).keyBy(data -> data.f0, data -> data.f0)
                .process(new OrderMatchResult()).print();

        //检查同一订单：2个流是否匹配，5s内找不到提示

        env.execute();

    }

    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        //定义状态：用于保存已经到达的事件
        ValueState<Tuple3<String, String, Long>> appEventState;
        ValueState<Tuple4<String, String, String, Long>> stream3State;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().
                    getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("app.event",
                            Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            stream3State = getRuntimeContext().
                    getState(new ValueStateDescriptor<Tuple4<String, String, String, Long>>("third.event",
                            Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            //来的 appStream 判断另外一条流中通是否来过
            if (stream3State.value() != null) {
                out.collect("对账成功：" + value + " " + stream3State.value());
                //对账成功：清空状态
                stream3State.clear();
            } else {
                //更新
                appEventState.update(value);
                //注册定时器：等待5s 另一流的数据
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            //来的 appStream 判断另外一条流中通是否来过
            if (appEventState.value() != null) {
                out.collect("对账成功：" + appEventState.value() + " " + value);
                //对账成功：清空状态
                appEventState.clear();
            } else {
                //更新
                stream3State.update(value);
                //注册定时器：等待2s 另一流的数据
                ctx.timerService().registerEventTimeTimer(value.f3);
            }
        }

        //定时器出发2
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发：判断状态是否为空
            if (appEventState.value() != null) {
                out.collect("对账失败 " + appEventState.value() + " " + "第三方支付信息未获取");
            }
            if (stream3State.value() != null) {
                out.collect("对账失败 " + stream3State.value() + " " + "app支付信息未获取");
            }

            appEventState.clear();
            stream3State.clear();
        }
    }
}
