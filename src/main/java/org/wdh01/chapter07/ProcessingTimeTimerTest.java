package org.wdh01.chapter07;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.sql.Timestamp;

/**
 * 处理时间定时器
 */
public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> eventDS = env.addSource(new ClickSource());

        eventDS.keyBy(data -> data.user)
                //KeyedProcessFunction<String, Event, String>( K I O )
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        //获取处理时间
                        long currTs = ctx.timerService().currentProcessingTime(); // 处理时间
                        out.collect(ctx.getCurrentKey() + " 数据到达时间 -> " + new Timestamp(currTs));
                        //注册 10s 的定时器：处理时间定时器
                        ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000);

                    }
                     @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 定时器触发时间 -> " + new Timestamp(timestamp));
                    }
                }).print();
        env.execute();
    }
}
