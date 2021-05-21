package com.at.utils;


import com.alibaba.fastjson.JSONObject;
import com.at.common.MallConfig;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/**
 * @author zero
 * @create 2021-05-14 18:32
 */
@Slf4j
public class MyPhoenixUtil {

    /**
     * 两个任务
     * 1.查询
     * 2.建表
     */

    private static Connection pConn;


    /**
     * 获取连接
     *
     * @return
     */
    public static Connection getConnection() {

        if (pConn == null) {
            synchronized (MyPhoenixUtil.class) {
                if (pConn == null) {
                    try {
                        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                        pConn = DriverManager.getConnection(MallConfig.PHOENIX_SERVER);
                        pConn.setSchema(MallConfig.HABSE_SCHEMA);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("Phoenix 连接异常:" + e.getMessage());

                    }
                }
            }
        }
        return pConn;
    }


    /**
     * @param sql
     * @param clz
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(String sql, Class<T> clz) {

        if (pConn == null) {
            pConn = getConnection();
        }

        PreparedStatement stat = null;
        ResultSet resultSet = null;

        List<T> resultList = new ArrayList<>();

        try {
            stat = pConn.prepareStatement(sql);
            resultSet = stat.executeQuery();

            resultList = SqlUtil.parseDate(resultSet, clz, false);

        } catch (Exception e) {
            e.printStackTrace();
            log.error("hbase 查询异常:" + e.getMessage());
        } finally {
            SqlUtil.close(null, stat, resultSet);
        }


        return resultList;
    }


    /**
     * 执行sql
     * 不需要返回值得都可以
     *
     * @param sql
     */
    public static void executeSql(String sql) {

        if (pConn == null) {
            pConn = getConnection();
        }

        try {

            SqlUtil.executeSql(sql, pConn);
        } catch (Exception throwables) {
            throwables.printStackTrace();
            System.out.println("执行sql <" + sql + "> 失败:" + throwables.getMessage());
        }


    }


    public static void main(String[] args) {

//        System.out.println(queryList(getConnection(),"SELECT * FROM REALTIME_MALL.test_table", JSONObject.class));
//        executeSql(getConnection(),"upsert into REALTIME_MALL.DIM_TEST(id,tm_name) values('2','1')");
        //需要手动提交


//        Connection connection = null;
//        executeSql("drop table REALTIME_FLINK.DIM_BASE_REGION");
//        executeSql(getConnection(connection),"upsert into REALTIME_MALL.DIM_USER_INFO(birthday,login_name,gender,create_time,name,user_level,id,operate_time) values('1993-05-15','f3y8v4p42','F','2021-05-15 16:47:46','韩菲','1','9','')");


        List<JSONObject> jsonObjects = queryList("select * from REALTIME_FLINK.DIM_USER_INFO where ID='7'", JSONObject.class);
        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject.toString());
        }

        System.out.println("操作成功");

    }


}
