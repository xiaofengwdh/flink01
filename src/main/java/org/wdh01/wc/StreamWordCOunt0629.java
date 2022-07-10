package org.wdh01.wc;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCOunt0629 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("hadoop103", 9999)
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] s = line.split(" ");
                    for (String s1 : s) {
                        out.collect(Tuple2.of(s1, 1L));
                    }
                }).returns(new TypeHint<Tuple2<String, Long>>() {
        }).keyBy(data -> data.f0)
                .sum(1).print();

        env.execute();
    }
}
