package org.wdh01.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BufferingSinkExample {
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

        strteam.print(" input-stream ");
        //批量缓存输出 10条数据输出一次
        strteam.addSink(new BufferingSink(10));

        env.execute();
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        //定义类属性:数量
        private final int threshold;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElement = new ArrayList<>();
        }

        private List<Event> bufferedElement;
        //定义一个算子状态
        private ListState<Event> checkpointState;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            //每来一条数据，进行缓存
            bufferedElement.add(value);
            //达到阈值10，输出
            if (bufferedElement.size() == threshold) {
                //批量输出模拟写入
                for (Event event : bufferedElement) {
                    System.out.println(event);
                }
                System.out.println("----------------输出完毕----------------");
                bufferedElement.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //清空
            checkpointState.clear();
            //持久化状态，复制缓存的列表到列表状态
            for (Event event : bufferedElement) {
                checkpointState.add(event);
            }

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
//初始化算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffed-list", Event.class);
            checkpointState = context.getOperatorStateStore().getListState(descriptor);
            //如果故障恢复，需要从ListState复制数据到bufferedElement
            if (context.isRestored()) {
                for (Event event : checkpointState.get()) {
                    bufferedElement.add(event);
                }
            }


        }
    }
}
