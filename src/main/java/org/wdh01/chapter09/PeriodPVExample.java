package org.wdh01.chapter09;

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
 * 周期性状态编程实现 PV
 */
public class PeriodPVExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> strteam = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        strteam.print(" input-stream ");

        strteam.keyBy(data -> data.user)
                .process(new PeriodicPVResult())
                .print();

        env.execute();
    }

    public static class PeriodicPVResult extends KeyedProcessFunction<String, Event, String> {


        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            //每来一条数据更新 cnt
            Long cnt = cntState.value();
            //如果 cnt 不为空就+1
            cntState.update(cnt == null ? 1L : cnt + 1L);
            //注册定时器：如果未注册，则注册
            if (timerTsState.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                timerTsState.update(value.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时输出
            out.collect(ctx.getCurrentKey() + " -> " + cntState.value());
            //一次输出后：定时器清空状态
            timerTsState.clear();

            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timerTsState.update(timestamp + 10 * 1000L);

        }

        //定义状态，保存当前PV统计值
        ValueState<Long> cntState;
        //定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            cntState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("cnt", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }
        //定时器：10s 输出一次

    }
}
