package org.wdh01.chapter08;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> integerStream = env.fromElements(1, 2, 3);
        DataStreamSource<Long> LongStream = env.fromElements(4L, 5L, 6L, 7L);
        LongStream.connect(integerStream).map(new CoMapFunction<Long, Integer, String>() {
            @Override
            public String map1(Long value) throws Exception {
                return " long " + value.toString();
            }

            @Override
            public String map2(Integer value) throws Exception {
                return " Integer " + value.toString();
            }
        }).print();

        env.execute();
    }
}
