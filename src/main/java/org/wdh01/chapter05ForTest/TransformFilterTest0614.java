package org.wdh01.chapter05ForTest;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;

public class TransformFilterTest0614 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从元素中读取数据
        //从元素中读取数据
        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L),
                new Event("依琳", "/info?id=2", 8000L),
                new Event("任盈盈", "/home", 8000L),
                new Event("依琳", "/error", 100000L)
        );
        //匿名类方式
    /*    eventDStream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("依琳");
            }
        }).print();*/
        //实现类实现
        //eventDStream.filter(new MyFilterFunTest()).print();
        //lambda 实现
        eventDStream.filter(data -> data.user.equals("依琳")).print();

        env.execute();
    }

    public static class MyFilterFunTest implements FilterFunction<Event> {

        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("依琳");
        }
    }
}
