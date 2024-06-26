package com.jx.comsumer;

import com.sun.corba.se.impl.orbutil.ObjectStreamClassUtil_1_3;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import sun.applet.Main;

import java.util.Collections;
import java.util.Properties;

public class ComsumerTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        // key 和 value 的犯序列化方式
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 组id 一个消费者组有多个消费者，对同一个主题的消息，只能被其中一个消费者组中的一个消费者消费到
        // 不同的消费者，消费者主id不同的话，那么这些消费者都能收到这个主题的消息
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"group one");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        // 订阅一个组题
        consumer.subscribe(Collections.singletonList("perjson"));
        // 消费者消费者去拉取数据（主动获取）
        while (true){
            ConsumerRecords<String, String> datas = consumer.poll(100);
            datas.forEach(System.out::println);
        }
    }
}
