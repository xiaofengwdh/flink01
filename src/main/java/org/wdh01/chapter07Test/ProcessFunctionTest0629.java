package org.wdh01.chapter07Test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.time.Duration;

public class ProcessFunctionTest0629 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .process(new ProcessFunction<Event, String>() {
                             @Override
                             public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                                 //可以根据需要通过 value 进行判断数据，out 输出数据
                                 if (value.user.equals("依琳")) {
                                     out.collect(value.user + " 其实喜欢令狐冲");
                                 } else if (value.user.equals("令狐冲")) {
                                     out.collect(value.user + " 喜欢很多人");
                                     out.collect(value.user + " 他也喜欢东方菇凉   ");
                                     System.out.println(ctx.timerService().currentWatermark());
                                 }
                             }
                         }

                ).print();


        env.execute();
    }
}
