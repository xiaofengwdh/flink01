package org.wdh01.wc;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount0624 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //获取数据
        // DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop103", 9999);
        //设置批处理模式读取一个文件
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> socketTextStream = env.readTextFile("input/word.txt");
        //flatMap 转换数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatRes = socketTextStream.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] wores = line.split(" ");
            for (String wore : wores) {
                out.collect(Tuple2.of(wore, 1));
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
        });
        flatRes.keyBy(data -> data.f0).sum(1).print();


        env.execute();
    }
}
