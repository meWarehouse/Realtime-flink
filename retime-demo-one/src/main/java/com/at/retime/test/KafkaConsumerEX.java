package com.at.retime.test;

import com.at.retime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zero
 * @create 2021-06-03 20:09
 */
public class KafkaConsumerEX {

    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment hadoop102 = StreamExecutionEnvironment.createRemoteEnvironment("hadoop102", 8081, "D:\\workspace\\workspace2021\\bigdata\\实时-flink\\Realtime-flink\\realtime-cal-eng\\target\\realtime-cal-eng-1.0-SNAPSHOT.jar");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        env.addSource(MyKafkaUtil.getKafkaSource("user_behavior", "test_group")).print();
//
        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "    userId BIGINT,\n" +
                "    itemId BIGINT,\n" +
                "    categoryId BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts BIGINT,\n" +
                "    rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "    WATERMARK FOR rowtime as rowtime - INTERVAL '2' SECOND  \n" +
                ") WITH (" + MyKafkaUtil.getKafkaDDL("user_behavior", "test_group") + ")");


//        tEnv.executeSql("CREATE TABLE cumulative_uv (\n" +
//                "    date_str STRING,\n" +
//                "    time_str STRING,\n" +
//                "    uv BIGINT,\n" +
//                "    PRIMARY KEY (date_str, time_str) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                "    'connector' = 'elasticsearch-6',\n" +
//                "    'hosts' = 'http://hadoop102:9200',\n" +
//                "    'index' = 'cumulative_uv'\n" +
//                ")\n");

        tEnv.executeSql("CREATE TABLE user_behavior_cnt (\n" +
                "  behavior STRING,\n" +
                "  behavior_cnt STRING,\n" +
                "  win_end STRING\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall_report',\n" +
                "   'table-name' = 'user_behavior_cnt',\n" +
                "   'username'='root',\n" +
                "   'password'='root'\n" +
                ")\n");




        tEnv.executeSql("INSERT INTO user_behavior_cnt" +
                " SELECT\n" +
                "  behavior,\n" +
                "  COUNT(behavior) AS behavior_cnt,\n" +
                "  HOP_END(rowtime,INTERVAL '10' MINUTE,INTERVAL '1' HOUR) AS win_end\n" +
                "FROM user_behavior \n" +
                "group by behavior,HOP(rowtime,INTERVAL '10' MINUTE,INTERVAL '1' HOUR)").print();


//        tEnv.executeSql(
//                " select\n" +
//                "  date_str,\n" +
//                "  max(time_str) time_str,\n" +
//                "  count(distinct userId) as uv\n" +
//                " from\n" +
//                " (\n" +
//                "  select\n" +
//                "    date_format(rowtime,'yyyy-MM-dd') as date_str,\n" +
//                "    substr(date_format(rowtime,'HH:mm'),1,4) || '0' as time_str,\n" +
//                "    userId\n" +
//                "  from user_behavior\n" +
//                " )\n" +
//                " group by date_str").print();


//        tableResult.print();


        env.execute();


    }


}
