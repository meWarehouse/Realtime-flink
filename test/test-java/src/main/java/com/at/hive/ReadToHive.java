package com.at.hive;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author zero
 * @create 2021-06-27 13:51
 */
public class ReadToHive {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "student";
        String defaultDatabase = "default";
        String hiveConfDir     = "D:\\workspace\\workspace2021\\bigdata\\实时-flink\\Realtime-flink\\test\\test-java\\src\\main\\resources\\hive-site.xml";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir,version);

        tableEnv.registerCatalog(name,hive);

        try {
            tableEnv.useCatalog(name);
            tableEnv.useDatabase(defaultDatabase);

//            Table studentTable = tableEnv.sqlQuery("select * from student");
//            TupleTypeInfo<Tuple2<Integer,String>> tupleTypeInfo = new TupleTypeInfo<>(Types.INT,Types.STRING);

            tableEnv.executeSql("select * from student").print();




        }catch (Exception e){
            e.printStackTrace();
        }






        env.execute();

    }
}
