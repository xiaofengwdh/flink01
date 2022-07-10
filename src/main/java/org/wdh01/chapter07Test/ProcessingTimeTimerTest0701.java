package org.wdh01.chapter07Test;

import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.sql.Timestamp;

public class ProcessingTimeTimerTest0701 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //处理时间语义，无需分配水位线
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource());
        eventStream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        TimerService timerService = ctx.timerService();
                        long processingTime = timerService.currentProcessingTime();
                        System.out.println("数据到达时间 " + new Timestamp(processingTime));
                        //注册一个 10s 后的定时器
                        timerService.registerProcessingTimeTimer(processingTime + 10 * 1000L);
                    }
                   //定时器触发操作
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("定时器触发时间 ---> " + new Timestamp(timestamp));
                    }
                }).print();


        env.execute();
    }
}
