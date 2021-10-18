package com.at.app.dwd;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2021-10-14
 */
public class ToHbaseTest {

    public static void main(String[] args) throws Exception {

//        ParameterTool parameterTool = ParseArgsUtil.init(args, false);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


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
//                "create TABLE source_user_switch_msg(\n" +
//                "\tid BIGINT,\n" +
//                "\tuser_id BIGINT,\n" +
//                "\ttype_code STRING,\n" +
//                "\tstatus INT,\n" +
//                "\tcreate_time BIGINT,\n" +
//                "\tupdate_time BIGINT,\n" +
//                "\tis_delete INT\n" +
//                ")WITH(\n" +
//                "\t'connector' = 'mysql-cdc',\n" +
//                "\t'hostname' = 'hadoop102',\n" +
//                "\t'port' = '3306',\n" +
//                "\t'username' = 'root',\n" +
//                "\t'password' = 'root',\n" +
//                "\t'database-name' = 'gmall_report',\n" +
//                "\t'table-name' = 'user_switch_msg'\n" +
//                ")" +
//                "");
//
//        tableEnv.executeSql("select * from source_user_switch_msg").print();


        tableEnv.executeSql("" +
                "CREATE TABLE source_test_flinkcdc(  \n" +
                "  id BIGINT,\n" +
                "  name BIGINT,\n" +
                "  age BIGINT\n" +
                ")WITH(\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'hadoop102',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'root',\n" +
                "  'database-name' = 'gmall_report',\n" +
                "  'table-name' = 'test_flinkcdc'\n" +
                ")" +
                "");


        tableEnv.executeSql("select * from source_test_flinkcdc ").print();



        env.execute();






    }

}
