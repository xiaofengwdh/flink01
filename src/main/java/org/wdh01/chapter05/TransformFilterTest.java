package org.wdh01.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;

public class                                                                                                                                                                                                                                                                                  TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从元素中读取数据
        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L)
        );
        //传入实现了 filterFunc
        SingleOutputStreamOperator<Event> filter = eventDStream.filter(new MyFilterFun());
        //传入匿名类
        SingleOutputStreamOperator<Event> filter1 = eventDStream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("依琳");
            }
        });
        //使用lambda表达式
        eventDStream.filter(data -> data.user.equals("依琳")).print();
        filter.print();
        filter1.print();
        env.execute();
    }

    public static class MyFilterFun implements FilterFunction<Event> {

        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("依琳");
        }
    }
}
