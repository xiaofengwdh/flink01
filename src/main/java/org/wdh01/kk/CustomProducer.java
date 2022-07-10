package org.wdh01.kk;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //连接ZK
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop103:9092,hadoop104:9092");
        //设置KV序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //指定 kv 的序列化类型
        //1、创建 生产者
        KafkaProducer<String, String> KafkaProducer = new KafkaProducer<String, String>(properties);
        //2、发送数据 put异步发送
        for (int i = 0; i < 5; i++) {
            KafkaProducer.send(new ProducerRecord<>("first", i + "  hello wdh01"));
        }
        //3、关闭资源
        KafkaProducer.close();

    }
}

  /*      [hui@hadoop103 kafka]$ bin/kafka-console-consumer.sh --bootstrap-server hadoop103:9092 --topic first
        0  hello wdh01
        1  hello wdh01
        2  hello wdh01
        3  hello wdh01
        4  hello wdh01*/