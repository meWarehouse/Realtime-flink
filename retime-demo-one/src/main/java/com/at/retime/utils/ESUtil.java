package com.at.retime.utils;

/**
 * @author zero
 * @create 2021-06-03 22:49
 */
public class ESUtil {

    public static String HOST_ADDRESS="http://hadoop102:9200";

    public static String getESDDL(String index){

        String ddl = " 'connector' = 'elasticsearch-6',\n" +
                "  'hosts' = '"+HOST_ADDRESS+"',\n" +
                "  'index' = '"+index+"',\n" +
                "  'document-type' = '_doc'";

        return ddl;
    }


}
