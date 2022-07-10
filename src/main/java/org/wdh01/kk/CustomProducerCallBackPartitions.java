package org.wdh01.kk;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallBackPartitions {
    public static void main(String[] args) throws InterruptedException {
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
            KafkaProducer.send(new ProducerRecord<>("lhc", 0, "", i + "  hello wdh01 a "), new Callback() {
                // new Callback( 回调函数
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(" a 主题  " + metadata.topic() + " 分区 " + metadata.partition());
                    }
                }
            });
        }
        //3、关闭资源
        KafkaProducer.close();
    }
}
