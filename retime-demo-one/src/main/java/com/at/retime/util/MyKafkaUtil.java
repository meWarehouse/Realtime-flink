package com.at.retime.util;

import com.at.retime.bean.FlinkKafkaConsumerRecord;
import com.at.retime.common.FlinkKafkaDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.UUID;

/**
 * @create 2021-10-12
 */
public class MyKafkaUtil {

    private static final FlinkKafkaDeserializationSchema kafkaDeserializationSchema = new FlinkKafkaDeserializationSchema();

    public static FlinkKafkaConsumer<FlinkKafkaConsumerRecord> getKafkaConsumer(String topic,String groupId) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        return new FlinkKafkaConsumer<FlinkKafkaConsumerRecord>(topic, kafkaDeserializationSchema, properties);

    }

}
