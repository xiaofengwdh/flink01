package org.wdh01.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.wdh01.bean.Event;
import org.wdh01.bean.UrlViewCount;

import java.time.Duration;

public class LaterDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置全局并行度
        env.setParallelism(1);
        //设置水位线生成间隔
        env.getConfig().setAutoWatermarkInterval(100);
        SingleOutputStreamOperator<Event> eventStream = env.socketTextStream("hadoop103", 9999).map(
                new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                    }
                }
        ).returns(new TypeHint<Event>() {
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        eventStream.print("  input  ");

        //定义输出标签
        OutputTag<Event> later = new OutputTag<Event>("later") {
        };

        //统计 url 访问量


        SingleOutputStreamOperator<UrlViewCount> result = eventStream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))

                .allowedLateness(Time.minutes(1)) //1 min 延迟
                //迟到数据输出到册数出列
                .sideOutputLateData(later)
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());


        result.print("  result  ");
        //侧输出流
        result.getSideOutput(later).print("later datas");

        env.execute();
    }
}
