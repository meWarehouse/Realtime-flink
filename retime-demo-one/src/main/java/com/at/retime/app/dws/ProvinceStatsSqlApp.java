package com.at.retime.app.dws;


import com.at.retime.bean.ProvinceStats;
import com.at.retime.utils.ClickHouseUtil;
import com.at.retime.utils.MyKafkaUtil;
import jdk.nashorn.internal.ir.EmptyNode;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zero
 * @create 2021-05-23 15:29
 */
public class ProvinceStatsSqlApp {

    /**
     *
     * 统计地区主题
     *
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/provincestatssqlApp"));

        EnvironmentSettings setting = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (" +
                "province_id BIGINT, " +
                "province_name STRING," +
                "province_area_code STRING" +
                ",province_iso_code STRING," +
                "province_3166_2_code STRING," +
                "order_id STRING, " +
                "split_total_amount DOUBLE," +
                "create_time STRING," +
                "rowtime AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR  rowtime  AS rowtime)" +
                " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        //        tableEnv.executeSql("select * from ORDER_WIDE").print();

        Table provinceStateTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                "province_id," +
                "province_name," +
                "province_area_code area_code," +
                "province_iso_code iso_code ," +
                "province_3166_2_code iso_3166_2 ," +
                "COUNT( DISTINCT  order_id) order_count, " +
                "sum(split_total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts "+
                "from  ORDER_WIDE " +
                "group by  TUMBLE(rowtime, INTERVAL '10' SECOND ),province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");


        DataStream<ProvinceStats> res = tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);
//        res.print(">>>>>>>>>>>");

        res.addSink(ClickHouseUtil.getJdbcSink("insert into  province_stats  values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute();


    }

}
