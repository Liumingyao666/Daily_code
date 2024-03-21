package com.liumingyao.springbootinit.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class QuickStartProducerTest {

    private static final String HOST = "192.168.122.128:9092";

    public static void main(String[] args) {
        // 1.指定生产者配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        // 2.使用配置初始化kafka生产者
        Producer<String, String> producer = new KafkaProducer<>(properties);

//        ProducerRecord<String, String> record =
//                new ProducerRecord<>("CustomerCountry", "Precision Products", "France");

        try {
            // 3.使用send发送消息
            for (int i = 0; i < 100; i++) {
                String message = "message" + i;
                producer.send(new ProducerRecord<>("Hello", message));
                System.out.println("Sent:" + message);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            // 4.关闭生产者
            producer.close();
        }
    }


}
