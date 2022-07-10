package org.wdh01.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.wdh01.bean.Event;

import java.util.Properties;

public class SinkToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop103:9092");
        properties.setProperty("group.id", "consumer-group");
    /*    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");*/

        //5、从kafka 读取数据

        DataStreamSource<String> clicks = env.addSource(new FlinkKafkaConsumer<String>("lhc", new SimpleStringSchema(), properties));

        //
        SingleOutputStreamOperator<String> map = clicks.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] split = value.split(",");
                return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim())).toString();
            }
        });

        //写入kafka
        map.addSink(new FlinkKafkaProducer<String>("hadoop103:9092", "tbg", new SimpleStringSchema()));

        map.print();
        env.execute();

    }
}
