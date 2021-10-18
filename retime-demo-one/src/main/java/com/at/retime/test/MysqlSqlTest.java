package com.at.retime.test;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.Set;

/**
 * @author zero
 * @create 2021-06-12 10:09
 */
public class MysqlSqlTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-test-group");
//        env.addSource(new FlinkKafkaConsumer<String>("user_behaviors",new SimpleStringSchema(),props)).print();

        tEnv.executeSql("CREATE TABLE flink_temp (\n" +
                "  `userId` BIGINT,\n" +
                "  `itemId` BIGINT,\n" +
                "  `categoryId` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `rowDate` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behaviors',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
                "  'properties.group.id' = 'test-flink-group',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'format' = 'json'\n" +
                ")\n");


        tEnv.executeSql("select * from flink_temp").print();


        env.execute();


    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static
    class Test {
        String key;
        String val;
    }

    //    static class MySchema implements KafkaRecordDeserializer<Test> {
    static class MySchema implements KafkaDeserializationSchema<Test> {
        @Override
        public boolean isEndOfStream(Test test) {
            return false;
        }

        @Override
        public Test deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
            String key = new String(consumerRecord.key(), "UTF-8");
            String val = new String(consumerRecord.value(), "UTF-8");

//            collector.collect(new Test(key, val));
            return new Test(key, val);
        }

        @Override
        public TypeInformation<Test> getProducedType() {
            return TypeInformation.of(Test.class);
        }

//    @Override
//        public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<Test> collector) throws Exception {
//            String key = new String(consumerRecord.key(), "UTF-8");
//            String val = new String(consumerRecord.value(), "UTF-8");
//
//            collector.collect(new Test(key,val));
//        }
//
//        @Override
//        public TypeInformation<Test> getProducedType() {
//            return TypeInformation.of(new TypeHint<Test>() {});
//        }
    }

    class MyDeserializationFormat implements DeserializationFormatFactory {
        @Override
        public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig) {
            return null;
        }

        @Override
        public String factoryIdentifier() {
            return null;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return null;
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return null;
        }
    }

}
