package org.wdh01.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;

/**
 * 转换算子
 */
public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从元素中读取数据
        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L)
        );
        //转换：提取 user
        //使用自定义类实现 ，MapFunction
        SingleOutputStreamOperator<String> users = eventDStream.map(new MyMapFun());
        //也可以使用匿名类实现
        SingleOutputStreamOperator<String> user1 = eventDStream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        });
        //对于只有一个方法而言：可以使用lambda 表达式
        SingleOutputStreamOperator<String> user2 = eventDStream.map(data -> data.user);
        users.print();
        user1.print();
        user2.print();

        env.execute();
    }

    //自定义 MapFunction
    public static class MyMapFun implements MapFunction<Event, String> {

        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}
