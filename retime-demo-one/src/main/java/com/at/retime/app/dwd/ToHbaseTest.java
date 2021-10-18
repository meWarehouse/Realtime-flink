package com.at.retime.app.dwd;

import com.at.retime.utils.ParseArgsUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2021-10-14
 */
public class ToHbaseTest {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParseArgsUtil.init(args, false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        //需要设置 checkpoint 失败容忍次数
//        CheckpointUtil.enableCheckpoint(env,parameterTool);

//        tableEnv.executeSql("" +
//                "" +
//                "create table source_table_process(\n" +
//                "\tsource_table STRING ,\n" +
//                "\toperate_type STRING ,\n" +
//                "\tsink_type STRING ,\n" +
//                "\tsink_table STRING ,\n" +
//                "\tsink_columns STRING ,\n" +
//                "\tsink_pk STRING ,\n" +
//                "\tsink_extend STRING \n" +
//                ")WITH(\n" +
//                "\t'connector' = 'mysql-cdc',\n" +
//                "\t'hostname' = 'hadoop102',\n" +
//                "\t'port' = '3306',\n" +
//                "\t'username' = 'root',\n" +
//                "\t'password' = 'root',\n" +
//                "\t'database-name' = 'gmall_report',\n" +
//                "\t'table-name' = 'table_process'\n" +
//                ")" +
//                "");
//
//        tableEnv.executeSql("select * from source_table_process").print();


//
//        tableEnv.executeSql("" +
//                "CREATE TABLE source_test_flinkcdc(  \n" +
//                "  id BIGINT,\n" +
//                "  name STRING,\n" +
//                "  age STRING\n" +
//                ")WITH(\n" +
//                "  'connector' = 'mysql-cdc',\n" +
//                "  'hostname' = 'hadoop102',\n" +
//                "  'port' = '3306',\n" +
//                "  'username' = 'root',\n" +
//                "  'password' = 'root',\n" +
//                "  'database-name' = 'gmall_report',\n" +
//                "  'table-name' = 'test_flinkcdc'\n" +
//                ")" +
//                "");
//
//
//
//
//
        tableEnv.executeSql("" +
                "CREATE TABLE hTable (\n" +
                " rowkey BIGINT,\n" +
                " info ROW<name STRING, age STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'MYTABLEQ',\n" +
                " 'zookeeper.quorum' = 'hadoop102:2181,hadoop103:2181,hadoop104:2181'\n" +
                ")" +
                "");

//        tableEnv.executeSql("" +
//                "" +
//                "INSERT INTO hTable\n" +
//                "SELECT\n" +
//                " id,\n" +
//                " ROW(name,age) \n" +
//                "FROM source_test_flinkcdc" +
//                "");



//        tableEnv.executeSql("select * from source_test_flinkcdc ").print();

//
//        tableEnv.executeSql("" +
//                "CREATE TABLE tt (\n" +
//                " rowkey STRING,\n" +
//                " info ROW<r BIGINT>,\n" +
//                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                " 'connector' = 'hbase-2.2',\n" +
//                " 'table-name' = 'student',\n" +
//                " 'zookeeper.quorum' = 'hadoop102:2181,hadoop103:2181,hadoop104:2181'\n" +
//                ")" +
//                "");
//
        tableEnv.executeSql("select rowkey,info from hTable").print();


//        env.execute();






    }

}
