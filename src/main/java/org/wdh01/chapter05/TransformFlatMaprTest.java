package org.wdh01.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;

/**
 * FlatMap :扁平化：
 * 可以灵活的实现 map  在out 逻辑实现 封装即可
 * 或者filter  out 逻辑实现 过滤即可:
 */
public class TransformFlatMaprTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从元素中读取数据
        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L)
        );
        //实现FlatMapFunction
        SingleOutputStreamOperator<String> flatmapRes = eventDStream.flatMap(new MyFlatMapFun());
        flatmapRes.print();
        //使用匿名类
        eventDStream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        }).print();

        //使用lambda表达式
        eventDStream.flatMap((Event value, Collector<String> out) -> {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
        }).returns(new TypeHint<String>() {
        }).print();

        env.execute();
    }

    public static class MyFlatMapFun implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
        }
    }
}
