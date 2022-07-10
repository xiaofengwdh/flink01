package org.wdh01.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * 有界流 wordcount
 */
public class BoundeStreamWordCount {
    public static void main(String[] args) throws Exception {
    //流处理环节
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOne = lineDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> dataGroup = wordOne.keyBy(data -> data.f0);
        dataGroup.sum(1).print();
        //开启任务
        env.execute();
    }
}
