package com.at.retime.common;


import com.at.retime.bean.FlinkKafkaConsumerRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Optional;

/**
 * @create 2021-10-13
 */
public class FlinkKafkaDeserializationSchema implements KafkaDeserializationSchema<FlinkKafkaConsumerRecord> {
    @Override
    public boolean isEndOfStream(FlinkKafkaConsumerRecord flinkKafkaConsumerRecord) {
        return false;
    }

    @Override
    public FlinkKafkaConsumerRecord deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {

        byte[] key = Optional.ofNullable(consumerRecord.key()).orElseGet(() -> new byte[0]);
        byte[] msg = Optional.ofNullable(consumerRecord.value()).orElseGet(() -> new byte[0]);
        String topic = consumerRecord.topic();
        int partition = consumerRecord.partition();
        long offset = consumerRecord.offset();
        long timestamp = consumerRecord.timestamp();

        return FlinkKafkaConsumerRecord.builder().key(key).msg(msg).topic(topic).partition(partition).offset(offset).timestamp(timestamp).build();
    }

    @Override
    public TypeInformation<FlinkKafkaConsumerRecord> getProducedType() {
        return TypeInformation.of(FlinkKafkaConsumerRecord.class);
    }
}
