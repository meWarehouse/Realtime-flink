package com.at.retime.common;

/**
 * @author zero
 * @create 2021-05-14 18:37
 */
public class MallConfig {



    //Hbase的命名空间
    public static final String HABSE_SCHEMA = "REALTIME_FLINK";

    //Phonenix连接的服务器地址
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    //ClickHouse的URL连接地址
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop202:8123/default";


}
