package org.wdh01.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.wdh01.bean.Event;

public class TransfromPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L),
                new Event("依琳", "/info?id=2", 8000L),
                new Event("任盈盈", "/home", 8000L),
                new Event("依琳", "/error", 100000L)
        );
        //随机分区
        // eventDStream.shuffle().print().setParallelism(4);
        //轮询分区
        //eventDStream.rebalance().print().setParallelism(4);
        //重缩放 ： 分组内 轮询
/*
        env.addSource(new RichParallelSourceFunction<Integer>() {

            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 1; i <= 8; i++) {
                    //奇偶数分别发送到 0 1 并行分区
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).rescale().print().setParallelism(4);
*/
        // 广播:每条数据 每个分区处理一次
        //eventDStream.broadcast().print().setParallelism(4);
        //全局分区:强制将所有数据分配到第一个分区内
        //eventDStream.global().print().setParallelism(4);
        //自定义分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).print().setParallelism(4);
        //eventDStream.rescale().print();
        env.execute();
    }
}
