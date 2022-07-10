package org.wdh01.kks;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class CustomeConsumerByHandSync {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //连接
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop103:9092,hadoop104:9092,hadoop105:9092");
        //反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //配置消费者组ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test3");
        //设置手动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //创建消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        //订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("lhc");
        kafkaConsumer.subscribe(topics);
        //消费数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
            //手动提交偏移量
            //kafkaConsumer.commitSync();
            //异步提交
            kafkaConsumer.commitAsync();
        }
    }
}
