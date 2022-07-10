package org.wdh01.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 无界流 org.wdh01.wc.StreamWordCountNoArgs
 * --host hadoop103 --port 9999
 */
public class StreamWordCountNoArgs {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment remoteEnv = StreamExecutionEnvironment.createRemoteEnvironment("host", // JobManager主机名
                1234, // JobManager进程端口号
                "path/to/jarFile.jar"// 提交给JobManager的JAR包
        );


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop103", 9999);
        socketTextStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] s = line.split(" ");
            for (String s1 : s) {
                out.collect(Tuple2.of(s1, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
