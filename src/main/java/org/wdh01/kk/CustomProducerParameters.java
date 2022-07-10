package org.wdh01.kk;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerParameters {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //链接
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop103:9092");
        //KV 序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);//32M
        //批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);//16k
        //linger.ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);//1ms
        //压缩
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        //创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //发送数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "hello wdh01"));
        }
        //关闭资源
        kafkaProducer.close();
    }
}
