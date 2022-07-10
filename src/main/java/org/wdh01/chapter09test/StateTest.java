package org.wdh01.chapter09test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.time.Duration;

public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        eventStream.keyBy(data -> data.user)
                .flatMap(new MyrichFlatMapFun())
                .print();


        env.execute();

    }

    public static class MyrichFlatMapFun extends RichFlatMapFunction<Event, String> {
        //定义值 状态
        ValueState<Event> mystate;
        //list 状态
        ListState<Event> listState;
        //map 状态
        MapState<String, Long> mapState;
        //聚合状态
        ReducingState<Event> reducingState;
        AggregatingState<Event, String> aggregatingState;

        //定义一个变量
        Long cnt = 0L;

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取状态
            mystate = getRuntimeContext().getState(new ValueStateDescriptor<Event>("my-state", Event.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("list-state", Event.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map-state", String.class, Long.class));
            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("redu state", new ReduceFunction<Event>() {
                @Override
                public Event reduce(Event value1, Event value2) throws Exception {
                    return new Event(value1.user, value1.url, value2.timestamp);
                }
            }, Event.class));
            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("agg-state", new AggregateFunction<Event, Long, String>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(Event value, Long accumulator) {
                    return accumulator + 1L;
                }

                @Override
                public String getResult(Long accumulator) {
                    return " -> " + accumulator;
                }

                @Override
                public Long merge(Long a, Long b) {
                    return a + b;
                }
            }, Long.class));

        }


        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            //使用状态
            //相同key 第一次出现输出 null,第二次有结果
 /*           System.out.println(mystate.value());
            mystate.update(value);
            System.out.println(" -- " + mystate.value());


            listState.add(value);*/

            mapState.put(value.user, mapState.get(value.user) == null ? 1L : mapState.get(value.user) + 1L);
            System.out.println(value.user + " map  --  " + mapState.get(value.user));

            aggregatingState.add(value);
            System.out.println(" agg " + aggregatingState.get());

            cnt++;
            System.out.println( " cnt " + cnt);

          /*  null
                     -- Event{user='莫大', url='./cat', timestamp=2022-07-05 05:37:55.602}
            null
                    -- Event{user='依琳', url='./home', timestamp=2022-07-05 05:37:56.622}
            Event{user='莫大', url='./cat', timestamp=2022-07-05 05:37:55.602}
            -- Event{user='莫大', url='./pay', timestamp=2022-07-05 05:37:57.625}
            Event{user='莫大', url='./pay', timestamp=2022-07-05 05:37:57.625}
            -- Event{user='莫大', url='./home', timestamp=2022-07-05 05:37:58.627}
            null
                    -- Event{user='风清扬', url='./cat', timestamp=2022-07-05 05:37:59.633}
            Event{user='莫大', url='./home', timestamp=2022-07-05 05:37:58.627}
            -- Event{user='莫大', url='./info', timestamp=2022-07-05 05:38:00.648}
            Event{user='依琳', url='./home', timestamp=2022-07-05 05:37:56.622}
            -- Event{user='依琳', url='./info', timestamp=2022-07-05 05:38:01.652}
            Event{user='莫大', url='./info', timestamp=2022-07-05 05:38:00.648}
            -- Event{user='莫大', url='./pay', timestamp=2022-07-05 05:38:02.667}*/

        }
    }
}
