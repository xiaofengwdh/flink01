package org.wdh01.chapter05ForTest;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;

public class TransformTestMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从元素中读取数据
        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L),
                new Event("依琳", "/info?id=2", 8000L),
                new Event("任盈盈", "/home", 8000L),
                new Event("依琳", "/error", 100000L)
        );
        //调用map 返回用户名称
        /*********************map*************************/
        //方法1 使用实现方法
        //eventDStream.map(new MyMapTest0614()).print();
        //匿名函数实现
/*        eventDStream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user.toString();
            }
        }).print();*/
        //方式3 lambda 表达式实现
        // eventDStream.map(data->data.user.toString()).print();
        /**********************************************/
        //简单聚合 max/maxBy

        /**
         * maxBy 返回最新的一条记录
         * max 返回第一次出现的记录：当前用户最后一次出现的时间
         */
    /*    eventDStream.keyBy(data->data.user).max("timestamp").print("---max---");
        eventDStream.keyBy(data->data.user).maxBy("timestamp").print("---maxBy---");*/
        env.execute();

    }

    public static class MyMapTest0614 implements MapFunction<Event, String> {

        @Override
        public String map(Event value) throws Exception {
            return value.user.toString();
        }
    }
}
