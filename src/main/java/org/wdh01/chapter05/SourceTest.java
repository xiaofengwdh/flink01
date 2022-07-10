package org.wdh01.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.wdh01.bean.Event;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //全局并行度
        env.setParallelism(1);
        //1、从文件读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/clicks.txt");
        //2、从集合读取数据
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(3);
        integers.add(0);
        DataStreamSource<Integer> integerDataStreamSource = env.fromCollection(integers);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("令狐冲", "/home", 1000L));
        events.add(new Event("依琳", "/cat", 9000L));
        DataStreamSource<Event> eventDataStreamSource = env.fromCollection(events);
        //3、从元素中读取数据
        DataStreamSource<Event> eventDataStreamSource1 = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L)
        );
        //4、从socket读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop103", 9999);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop103:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        //5、从kafka 读取数据

        DataStreamSource<String> clicks = env.addSource(new FlinkKafkaConsumer<String>("lhc", new SimpleStringSchema(), properties));

        //打印
       /* stringDataStreamSource.print("---1---  ");
        integerDataStreamSource.print("---2---  ");
        eventDataStreamSource.print("---3---  ");
        eventDataStreamSource1.print("---4---  ");
        socketTextStream.print("---5---  ");*/


        clicks.print("---5---  ");
        //执行
        env.execute();

    }
}
