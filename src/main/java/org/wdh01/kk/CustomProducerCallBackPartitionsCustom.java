package org.wdh01.kk;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallBackPartitionsCustom {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop103:9092,hadoop104:9092,hadoop105:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //关联自定义分区器
        // properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.wdh01.kk.MyPartitioner");
        //指定 kv 的序列化类型
        //1、创建 生产者
        KafkaProducer<String, String> KafkaProducer = new KafkaProducer<String, String>(properties);
        //2、发送数据 put异步发送
        for (int i = 0; i < 5; i++) {
            KafkaProducer.send(new ProducerRecord<>("rwx", 0, "", i + "  hello wdh01"), new Callback() {
                // new Callback( 回调函数
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("主题 " + metadata.topic() + " 分区 " + metadata.partition());
                    }
                }
            });
        }
        //3、关闭资源
        KafkaProducer.close();
    }
}
