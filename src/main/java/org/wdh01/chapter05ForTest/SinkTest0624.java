package org.wdh01.chapter05ForTest;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkTest0624 {
    public static void main(String[] args) throws Exception {
        //获取执行护环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop103:9092");
        //从kafka 读取数据
        DataStreamSource<String> lhc = env.addSource(new FlinkKafkaConsumer<String>("lhc", new SimpleStringSchema(), properties));

        lhc.addSink(new FlinkKafkaProducer<String>("hadoop103:9092","tbg",new SimpleStringSchema()));

        env.execute();
    }
}
