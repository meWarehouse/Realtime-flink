package com.at.app.dwd;


import com.at.common.AppConstant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author zero
 * @create 2021-06-26 21:34
 */
public class OdsToDwdLogApp {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // parallel
        env.setParallelism(4);

        // rocksdb checkpoint 开启增量检查点
        RocksDBStateBackend rocksDBStateBackend =
                new RocksDBStateBackend(
                        AppConstant.STETE_PREFIX+"odstodwdlogApp",
                        true);
        env.setStateBackend(rocksDBStateBackend);

        // checkpoint interval 3 minute
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(3L), CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        //checkpoint min interval
        checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(5));
        //checkpoint timeout
        checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(10));
        // save last checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //read kafka

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,AppConstant.KAFKA_BOOTSERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,AppConstant.ODS_LOG_GROUP);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(AppConstant.ODS_LOG_TOPIC, new SimpleStringSchema(), props);

        env.addSource(kafkaConsumer)
                .print();


        env.execute();


    }

}
