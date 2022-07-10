package org.wdh01.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TowStreamJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> strteam = env.fromElements(
                Tuple3.of("a", "stream1", 1000L),
                Tuple3.of("b", "stream2", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
        );


        SingleOutputStreamOperator<Tuple3<String, String, Long>> strteam1 = env.fromElements(
                Tuple3.of("a", "stream1", 3000L),
                Tuple3.of("b", "stream2", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
        );

        //自定义列表全外连接

        strteam.connect(strteam1)
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    //定义列表抓状态，保存两个 列表的所有数据
                    ListState<Tuple2<String, Long>> stat1;
                    ListState<Tuple2<String, Long>> stat2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stat1 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("state1", Types.TUPLE(Types.STRING, Types.LONG)));
                        stat2 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("state2", Types.TUPLE(Types.STRING, Types.LONG)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, Context ctx, Collector<String> out) throws Exception {
                        //获取另一条流数据 进行匹配
                        for (Tuple2<String, Long> right : stat2.get()) {
                            out.collect(left.f0 + " " + left.f2 + " -> " + right);
                        }
                        stat1.add(Tuple2.of(left.f0, left.f2));
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                        //获取另一条流数据 进行匹配
                        for (Tuple2<String, Long> left : stat1.get()) {
                            out.collect(left + " ->" + right.f0+" " + right.f2);
                        }
                        stat2.add(Tuple2.of(right.f0, right.f2));
                    }
                }).print();

        env.execute();
    }
}
