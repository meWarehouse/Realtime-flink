package com.at.retime.app.func;

import com.alibaba.fastjson.JSONObject;

import com.at.retime.common.MallConfig;
import com.at.retime.utils.DimUtil;
import com.at.retime.utils.MyPhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.util.Collection;
import java.util.Set;

/**
 * @author zero
 * @create 2021-05-15 14:14
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    /**
     * 将hbase侧输出流中的数据写入habse中
     */

    //hbase>>>> {"database":"rt_mall_flink","xid":37230,"data":{"tm_name":"14","id":14},"commit":true,"sink_table":"dim_base_trademark","type":"insert","table":"base_trademark","ts":1621058838}
    //kafka>>>>> {"database":"rt_mall_flink","xid":37317,"data":{"tm_name":"15","logo_url":"15","id":15},"commit":true,"sink_table":"dim_base_trademark","type":"insert","table":"base_trademark","ts":1621058872}

    private Connection hbaseConnection;

    @Override
    public void open(Configuration parameters) throws Exception {
//        hbaseConnection = MyPhoenixUtil.getConnection(hbaseConnection);
    }


//    @Override
//    public void invoke(JSONObject value, org.apache.flink.streaming.api.functions.sink.SinkFunction.Context context) throws Exception {
//
////        upsert into REALTIME_MALL.DIM_TEST(id,tm_name) values('1','1')
//
//        if (value != null && value.size() > 0) {
//
//            String sql = createUpsertSql(value.getString("sink_table"), value.getJSONObject("data"));
//            System.out.println("向Phoenix插入数据的SQL：" + sql);
//
////            MyPhoenixUtil.executeSql(hbaseConnection, sql);
//            MyPhoenixUtil.executeSql(sql);
//
//        }
//
//        //如果当前做的是更新操作，需要将Redis中缓存的数据清除掉
//        if(value.getString("type").equals("update")){
//            DimUtil.deleteCached(value.getString("sink_table"),value.getString("id"));
//        }
//
//
//    }

    @Override
    public void invoke(JSONObject value) throws Exception {

        if (value != null && value.size() > 0) {

            String sql = createUpsertSql(value.getString("sink_table"), value.getJSONObject("data"));
            System.out.println("向Phoenix插入数据的SQL：" + sql);

//            MyPhoenixUtil.executeSql(hbaseConnection, sql);
            MyPhoenixUtil.executeSql(sql);

        }

        //如果当前做的是更新操作，需要将Redis中缓存的数据清除掉
        if(value.getString("type").equals("update")){
            DimUtil.deleteCached(value.getString("sink_table"),value.getString("id"));
        }
    }

    private String createUpsertSql(String tableName, JSONObject data) {

        //upsert into REALTIME_MALL.DIM_TEST(id,tm_name) values('1','1')
        Set<String> keys = data.keySet();
        Collection<Object> values = data.values();

        String sql = "upsert into " + MallConfig.HABSE_SCHEMA + "." + tableName.toUpperCase() + "(" + StringUtils.join(keys, ",") + ")";
        String valSql = " values('" + StringUtils.join(values, "','") + "')";


        return sql + valSql;
    }


}
