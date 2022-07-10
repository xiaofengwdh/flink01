package org.wdh01.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;

import java.time.Duration;

public class UnionStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventStream = env.socketTextStream("hadoop103", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp * 10000;
                            }
                        }));

        eventStream.print("eventStream   ");

        SingleOutputStreamOperator<Event> eventStream1 = env.socketTextStream("hadoop103", 8888)
                .map(data -> {
                    String[] split = data.split(",");
                    return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp * 10000;
                            }
                        }));

        eventStream1.print("eventStream1   ");
        /**
         * union 何以合并多个流，数据类型需要一样
         */
        eventStream.union(eventStream1).process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                out.collect("水位线 " + ctx.timerService().currentWatermark());
            }
        }).print();

        env.execute();

    }
}
