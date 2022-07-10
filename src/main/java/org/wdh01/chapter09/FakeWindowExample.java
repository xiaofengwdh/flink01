package org.wdh01.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.sql.Timestamp;
import java.time.Duration;

public class FakeWindowExample {
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

        strteam.keyBy(data -> data.url)
                .process(new FakeWindowResult(10000L)).print();

        env.execute();

    }

    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {
        private Long windowSize;


        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        //定义状态：保存每个窗口的状态
        MapState<Long, Long> windowUrlCntState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowUrlCntState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window cnt", Long.class, Long.class));


        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            //判断每一条数据属于哪一个窗口（窗口分配）
            long start = value.timestamp / windowSize * windowSize;
            long end = start + windowSize;
            //注册 end-1 定时器
            ctx.timerService().registerEventTimeTimer(end - 1);
            //更新状态：增量聚合
            if (windowUrlCntState.contains(start)) {
                Long cnt = windowUrlCntState.get(start);
                windowUrlCntState.put(start, cnt + 1L);
            } else {
                windowUrlCntState.put(start, 1L);
            }

        }
        //定时器触发


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long end = timestamp + 1L;
            long start = end - windowSize;
            long cnt = windowUrlCntState.get(start);

            out.collect("窗口 " + new Timestamp(start) + " - " + new Timestamp(end)
                    + "url " + ctx.getCurrentKey()
            +" cnt ~ " + cnt);

            //模拟窗口关闭：
            windowUrlCntState.remove(start);
        }
    }
}
