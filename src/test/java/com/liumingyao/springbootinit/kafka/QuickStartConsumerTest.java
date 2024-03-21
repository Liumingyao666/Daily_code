package com.liumingyao.springbootinit.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class QuickStartConsumerTest {

    private static final String HOST = "192.168.122.128:9092";

    public static void main(String[] args) {

        // 1. 构建Consumer
        Properties props = new Properties();
        // bootstrap.servers是Kafka集群的IP地址。多个时,使用逗号隔开
        props.put("bootstrap.servers", HOST);
        // 消费者群组
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer
                <String, String>(props);

        // 2. 设置主题
        consumer.subscribe(Collections.singletonList("Hello"));

        // 3.接受消息
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(500);
                System.out.println("customer Message---");
                for (ConsumerRecord<String, String> record : records)

                    // print the offset,key and value for the consumer records.
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
            }
        } finally {
            // 4.关闭消息
            consumer.close();
        }
    }


}
