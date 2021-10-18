package com.at.loggercreate.util;

import com.at.loggercreate.config.AppConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author zero
 * @create 2021-06-26 17:36
 */
public class KafkaUtil {
    public static KafkaProducer<String, String> kafkaProducer = null;

    public KafkaUtil() {
    }

    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", AppConfig.kafka_server);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = null;

        try {
            producer = new KafkaProducer(properties);
        } catch (Exception var3) {
            var3.printStackTrace();
        }

        return producer;
    }

    public static void send(String topic, String msg) {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer();
        }

        kafkaProducer.send(new ProducerRecord(topic, msg));
    }
}
