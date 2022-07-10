package org.wdh01.chapter09;

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
        SingleOutputStreamOperator<Event> strteam = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );


        strteam.keyBy(data -> data.user)
                .flatMap(new MyFlatMap())
                .print();
        env.execute();
    }

    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            //访问更新状态
          /*  System.out.println(myValueState.value());
            myValueState.update(value);
            System.out.println("myValueState -> " + myValueState.value());
*/
            myListState.add(value);

            myMapState.put(value.user, myMapState.get(value.user) == null ? 0 : myMapState.get(value.user) + 1L);

            System.out.println("myMapState " + value.user + " -- " + myMapState.get(value.user));

            myAggregatingState.add(value);
            System.out.println("myAggregatingState  -> " + myAggregatingState.get());


            myReducingState.add(value);
            System.out.println("myReducingState  -> " + myReducingState.get());

        }

        //定义状态
        ValueState<Event> myValueState;
        ListState<Event> myListState;
        MapState<String, Long> myMapState;
        ReducingState<Event> myReducingState;
        AggregatingState<Event, String> myAggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            myValueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Event>("my-state", Event.class));
            myListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<Event>("my-list-state", Event.class));
            myMapState = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<String, Long>("my-map-state", String.class, Long.class));
            myReducingState = getRuntimeContext()
                    .getReducingState(new ReducingStateDescriptor<Event>("my-reduc-state",
                            new ReduceFunction<Event>() {
                                @Override
                                public Event reduce(Event value1, Event value2) throws Exception {
                                    return new Event(value1.user, value1.url, value2.timestamp);
                                }
                            },
                            Event.class));
            myAggregatingState = getRuntimeContext()
                    .getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("my-agg-state",
                            new AggregateFunction<Event, Long, String>() {
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
                                    return "cnt -> " + accumulator;
                                }

                                @Override
                                public Long merge(Long a, Long b) {
                                    return a + b;
                                }
                            },
                            Long.class));


        }
    }
}
