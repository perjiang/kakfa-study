package com.jx.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerTest {
    public static void main(String[] args) {
        // 创建生产者对象
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        // 指定一个消息
        String topicName = "perjson";
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            String value = "value" + i;
            ProducerRecord<String,String> record = new ProducerRecord<>(topicName,key,value);
            // 使用生产者吧消息发送给kafka服务器
            producer.send(record);
        }

        // 关闭生产者对象
        producer.close();
    }
}
