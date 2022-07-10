package org.wdh01.kk;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerTranaction {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //指定事务ID
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "myshiwu");
        //指定 kv 的序列化类型
        //1、创建 生产者
        KafkaProducer<String, String> KafkaProducer = new KafkaProducer<String, String>(properties);
        //初始化事务
        KafkaProducer.initTransactions();
        //开启事务
        KafkaProducer.beginTransaction();

        //2、发送数据 put异步发送
        try {
            for (int i = 0; i < 5; i++) {
                KafkaProducer.send(new ProducerRecord<>("first", i + "  hello wdh01"));
            }
            //提交事务
            KafkaProducer.commitTransaction();
        } catch (Exception e) {
            //终止事务
            KafkaProducer.abortTransaction();
        } finally {
            //3、关闭资源
            KafkaProducer.close();
        }
    }
}
