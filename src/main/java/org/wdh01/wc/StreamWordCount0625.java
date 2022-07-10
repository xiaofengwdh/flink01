package org.wdh01.wc;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount0625 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop103", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> returns = socketTextStream.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] s = line.split(" ");
            for (String s1 : s) {
                out.collect(Tuple2.of(s1, 1));
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
            @Override
            public boolean equals(Object obj) {
                return super.equals(obj);
            }
        });

        returns.keyBy(data -> data.f0).sum(1).print();
        env.execute();

    }
}
