package org.wdh01.chapter09test;

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

public class BufferingSinkExample0705 {
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
        strteam.addSink(new BufferingSink00705(10));

        env.execute();
    }

    public static class BufferingSink00705 implements SinkFunction<Event>, CheckpointedFunction {
        public BufferingSink00705(int thresghold) {
            this.thresghold = thresghold;
            this.buffList = new ArrayList<>();
        }

        //阈值
        private int thresghold;

        private List<Event> buffList;
        //定义算子状态
        private ListState<Event> checkpointState;


        @Override
        public void invoke(Event value, Context context) throws Exception {
            //每条数据操作
            buffList.add(value);//缓存数据
            //达到阈值，批量写入
            if (buffList.size() == thresghold) {
                for (Event event : buffList) {
                    System.out.println(event);
                }

                System.out.println("输出完毕");
                //清空
                buffList.clear();
            }

        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //清空
            checkpointState.clear();
            //持久化状态
            for (Event event : buffList) {
                checkpointState.add(event);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //初始化
            ListStateDescriptor<Event> buff = new ListStateDescriptor<>("buff", Event.class);
            checkpointState = context.getOperatorStateStore().getListState(buff);
            //故障恢复
            if (context.isRestored()) {//故障恢复
                for (Event event : checkpointState.get()) {
                    buffList.add(event);
                }

            }

        }
    }
}
