package org.wdh01.chapter09;

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

public class AverageTimeStampExample {
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
        //自定义实现平均时间戳
        strteam.keyBy(data -> data.user)
                .flatMap(new AvgTsResult(5L))
                .print();

        env.execute();

    }

    public static class AvgTsResult extends RichFlatMapFunction<Event, String> {

        private Long cnt;

        public AvgTsResult(Long cnt) {
            this.cnt = cnt;
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
//每来一条数据 current+1
            Long currCnt = cntState.value();
            if(currCnt==null){
                currCnt=1L;
            }else{
                currCnt++;
            }
            //更新状态
            cntState.update(currCnt);
            avgTsState.add(value);
            //如达到次数 输出
            if(currCnt.equals(cnt)){
                out.collect(value.user+" 过去 " + cnt + "次平均访问时间 " + avgTsState.get());
                //清理状态
                cntState.clear();
                avgTsState.clear();

            }


        }

        //定义聚合状态：计算平均时间
        AggregatingState<Event, Long> avgTsState;
        //定义值状态：保存用户访问次数
        ValueState<Long> cntState;

        @Override
        public void open(Configuration parameters) throws Exception {
            avgTsState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "agvts",
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
                    Types.TUPLE(Types.LONG, Types.LONG)
            ));
            cntState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("cnt State", Long.class));

        }
    }
}
