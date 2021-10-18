package com.at.hive;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;

/**
 * @author zero
 * @create 2021-06-27 20:39
 */
public class Test {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings setting  = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(setting);

//        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        String catalogName     = "student";
        String defaultDatabase = "default";
        String hiveConfDir     = "classpath:hive-site.xml";
        String version         = "3.2.1"; // or 1.2.1

        HiveCatalog hive = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog(catalogName, hive);


        tableEnv.loadModule(catalogName,new HiveModule(version));

        tableEnv.executeSql("select * from student").print();




        env.execute();




    }

}
