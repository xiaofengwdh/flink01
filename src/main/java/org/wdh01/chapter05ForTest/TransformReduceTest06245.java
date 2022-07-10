package org.wdh01.chapter05ForTest;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;


public class TransformReduceTest06245 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //获取输入
        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L),
                new Event("依琳", "/info?id=2", 8000L),
                new Event("任盈盈", "/home", 8000L),
                new Event("依琳", "/error", 100000L));
        //转换数据并且进行词频统计
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = eventDStream.map(date -> Tuple2.of(date.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1);

        //聚合函数实现 词频统计
        SingleOutputStreamOperator<Tuple2<String, Long>> reduce =
                eventDStream.map(data -> Tuple2.of(data.user, 1L)).
                        returns(new TypeHint<Tuple2<String, Long>>() {
                        }).keyBy(data -> data.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });
        //返回词频对大的那个数据
        sum.keyBy(data -> data.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return value1.f1 > value2.f1 ? value1 : value2;
            }
        }).print();

        //执行
        env.execute();
    }
}
