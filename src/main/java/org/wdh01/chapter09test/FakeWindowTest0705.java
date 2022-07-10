package org.wdh01.chapter09test;

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

public class FakeWindowTest0705 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        eventStream.keyBy(data -> data.url)
                .process(new FakeProcessFun(10000L))
                .print();


        env.execute();
    }

    public static class FakeProcessFun extends KeyedProcessFunction<String, Event, String> {

        private Long windowSize;

        public FakeProcessFun(Long windowSize) {
            this.windowSize = windowSize;
        }

        //map状态
        MapState<Long, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-cnt", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
//每来一条数据，看看分配到哪个窗口
            Long windowStart = value.timestamp / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;

            //注册定时器
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);
            //更新状态，增量聚合
            if (mapState.contains(windowStart)) {
                long cnt = mapState.get(windowStart);
                mapState.put(windowStart, cnt + 1L);
            } else {
                mapState.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long windowEnd = timestamp + 1L;
            long windowStart = windowEnd - windowSize;
            long cnt = mapState.get(windowStart);

            out.collect("窗口 " + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd) +
                    " url -> " + ctx.getCurrentKey() +
                    " cnt -> " + cnt);
//模拟窗口关闭
            mapState.remove(windowStart);
        }
    }
}

