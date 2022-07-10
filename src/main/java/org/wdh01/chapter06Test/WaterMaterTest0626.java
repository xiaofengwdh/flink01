package org.wdh01.chapter06Test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;

import java.time.Duration;

/**
 * 水位线生成练习
 */
public class WaterMaterTest0626 {
    public static void main(String[] args) throws Exception {
        //获取执行环境&设置并行度 1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        DataStreamSource<Event> eventDataStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L),
                new Event("依琳", "/info?id=2", 8000L),
                new Event("任盈盈", "/home", 8000L),
                new Event("依琳", "/error", 100000L));
        //有序流中水位线

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = eventDataStream.assignTimestampsAndWatermarks(
                //指定水位线生成策略：并声明数据泛型
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                //返回时间信息
                                return element.timestamp * 1000L;
                            }
                        }));
        //无序流中会水位线
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator1 = eventDataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        //指定无序流的水位线延迟 2s
                        <Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp * 1000L;
                            }
                        }));

        env.execute();
    }
}
