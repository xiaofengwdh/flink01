package org.wdh01.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.time.Duration;

/**
 * 侧输出流实现分流
 */
public class SplitSDtreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp * 10000;
                            }
                        }));

        //定义侧输出流标签
        OutputTag<Tuple3<String, String, Long>> yilin = new OutputTag<Tuple3<String, String, Long>>("依琳"){};
        OutputTag<Tuple3<String, String, Long>> linghc = new OutputTag<Tuple3<String, String, Long>>("令狐冲"){};

        SingleOutputStreamOperator<Event> processStream = eventStream.process(new ProcessFunction<Event, Event>() {

            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("依琳")) {
                    ctx.output(yilin, Tuple3.of(value.user, value.url, value.timestamp));
                } else if (value.user.equals("令狐冲")) {
                    ctx.output(linghc, Tuple3.of(value.user, value.url, value.timestamp));
                } else {
                    out.collect(value);
                }
            }
        });
        //获取主流
        processStream.print("  else  ");
        //获取侧输出流
        processStream.getSideOutput(yilin).print();
        processStream.getSideOutput(linghc).print();
        env.execute();
    }
}
