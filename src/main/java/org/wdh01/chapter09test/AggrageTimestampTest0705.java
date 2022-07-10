package org.wdh01.chapter09test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.time.Duration;

public class AggrageTimestampTest0705 {
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
                .flatMap(new MyFlatMap0705(5L)).print();

        env.execute();
    }

    public static class MyFlatMap0705 extends RichFlatMapFunction<Event, String> {

        private long cnt;

        public MyFlatMap0705(long cnt) {
            this.cnt = cnt;
        }

        //定义状态
        AggregatingState<Event, Long> aggregatingState;
        //保存用户访问次数
        ValueState<Long> cntState;

        @Override
        public void open(Configuration parameters) throws Exception {
            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>("" +
                    "agg-state",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1L);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)));
            cntState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("cnt-state", Long.class));

        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            //每来一条数据  +1
            Long currCnt = cntState.value();
            if(currCnt == null){
                currCnt=1l;
            }else{
                currCnt++;
            }
 //更新状态
            cntState.update(currCnt);
            aggregatingState.add(value);

            //达到上线 触发
            if(currCnt.equals(cnt)){
                out.collect(value.user+ " 过去 " + cnt+ " 访问平均时间戳 " +
                        aggregatingState.get());
                //清理
                cntState.clear();
                aggregatingState.clear();

            }
        }
    }
}
