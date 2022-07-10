package org.wdh01.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;

public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L),
                new Event("依琳", "/info?id=2", 8000L),
                new Event("任盈盈", "/home", 8000L),
                new Event("依琳", "/error", 100000L)
        );
        //提取每个用户：最大时间戳记录


        eventDStream.keyBy(data->data.user).max("timestamp").print("max");

       // eventDStream.keyBy(data -> data.user).maxBy("timestamp").print(" maxBy ");

        /**
         * maxBy 返回最新的一条数据
         * max返回的是最新的分组键+第一次数据（除了分组键，其余字段均是第一次出现的数据组成成的一条数据）
         *  max > Event{user='令狐冲', url='/home', timestamp=1970-01-01 08:00:01.0}
         *  maxBy > Event{user='令狐冲', url='/home', timestamp=1970-01-01 08:00:01.0}
         *  max > Event{user='依琳', url='/cat', timestamp=1970-01-01 08:00:09.0}
         *  maxBy > Event{user='依琳', url='/cat', timestamp=1970-01-01 08:00:09.0}
         *  max > Event{user='任盈盈', url='/pay', timestamp=1970-01-01 08:00:08.0}
         *  maxBy > Event{user='任盈盈', url='/pay', timestamp=1970-01-01 08:00:08.0}
         *  maxBy > Event{user='依琳', url='/cat', timestamp=1970-01-01 08:00:09.0}
         *  max > Event{user='依琳', url='/cat', timestamp=1970-01-01 08:00:09.0}
         *  maxBy > Event{user='任盈盈', url='/pay', timestamp=1970-01-01 08:00:08.0}
         *  max > Event{user='任盈盈', url='/pay', timestamp=1970-01-01 08:00:08.0}
         *  maxBy > Event{user='依琳', url='/error', timestamp=1970-01-01 08:01:40.0}
         *  max > Event{user='依琳', url='/cat', timestamp=1970-01-01 08:01:40.0}
         */
        env.execute();
    }
}
