package org.wdh01.chapter05ForTest;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;

public class TransformReduceTest0614 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L),
                new Event("依琳", "/info?id=2", 8000L),
                new Event("任盈盈", "/home", 8000L),
                new Event("依琳", "/error", 100000L)
        );
        //取出访问页最多的那个用户
        SingleOutputStreamOperator<Tuple2<String, Integer>> returns = eventDStream.map(d -> Tuple2.of(d.user, 1)).returns(new TypeHint<Tuple2<String, Integer>>() {
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> returns1 = returns.keyBy(d -> d.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value2.f1 + value1.f1);
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
        });

        returns1.keyBy(da -> "k").reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return value1.f1 > value2.f1 ? value1 : value2;
            }
        }).print();


        env.execute();


    }
}
