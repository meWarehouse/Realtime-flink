package com.at.sql;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zero
 * @create 2021-06-26 16:39
 */
public class UserBehaviorSql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings setting = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, setting);

        tEnv.executeSql("CREATE TABLE USER_BEHAVIOR (\n" +
                "  `userId` BIGINT,\n" +
                "  `itemId` BIGINT,\n" +
                "  `categoryId` BIGINT,\n" +
                "  `behaviour` STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'flink-scala-test-topic',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop:104:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.timestamp-millis' = '1624690800000',\n" +
                "  'format' = 'json'\n" +
                ")");

        tEnv.executeSql("select * from USER_BEHAVIOR").print();


        env.execute();

    }


}
