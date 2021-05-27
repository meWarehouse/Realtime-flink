package com.at.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.at.common.MallConfig;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.net.SocketTimeoutException;
import java.util.List;

/**
 * @author zero
 * @create 2021-05-21 15:35
 */
public class DimUtil {

    /**
     * 通过指定的条件查询hbsea中的维度表数据
     */


    public static JSONObject getDimInfo(String tableName, String val) {

        return getDimInfo(tableName, Tuple2.of("ID", val));

    }

    /**
     * 不使用缓存查询数据
     *
     * @param tableName
     * @param cloNameAndValue
     * @return
     */
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... cloNameAndValue) {

        //select * from REALTIME_FLINK.DIM_USER_INFO where ID='7'

        String whereSql = " where 0=0";

        if (cloNameAndValue != null && cloNameAndValue.length > 0) {
            for (int i = 0; i < cloNameAndValue.length; i++) {

                Tuple2<String, String> tuple2 = cloNameAndValue[i];
                String fileName = tuple2.f0;
                String fileVal = tuple2.f1;

                whereSql += " and " + fileName + "='" + fileVal + "'";
            }
        }

        String sql = "select * from " + tableName + whereSql;

        System.out.println("sql:" + sql);

        List<JSONObject> dimObj = MyPhoenixUtil.queryList(sql, JSONObject.class);
        JSONObject res = null;
        if (ObjectUtils.isNotEmpty(dimObj)) {
            res = dimObj.get(0);
        } else {
            System.out.println("维度数据没有找到:" + sql);
            System.out.println();
        }

        return res;

    }

    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... cloNameAndValue) {

        //select * from REALTIME_FLINK.DIM_USER_INFO where 0=0 and ID='7'
        //key = dim:REALTIME_FLINK.DIM_USER_INFO:ID

        String whereSql = " where 0=0 ";
        String key = "dim:" + tableName.toLowerCase() + ":";

        if (cloNameAndValue != null && cloNameAndValue.length > 0) {

            for (int i = 0; i < cloNameAndValue.length; i++) {
                Tuple2<String, String> tuple2 = cloNameAndValue[i];
                String k = tuple2.f0;
                String v = tuple2.f1;

                if (i > 0) {
                    key += "_";
                }

                whereSql += " and " + k + "='" + v + "'";
                key += v;
            }

        }

        String sql = "select * from " + tableName + whereSql;

//        System.out.println("key >>>> " + key);
//        System.out.println("sql >>>> " + sql);


        return find(sql, key);


    }

    private static JSONObject find(String sql, String key) {

        //查缓存
        Jedis jedis = null;
        JSONObject resObj = null;

        try {

            jedis = MyRedisUtil.getJedis();

            String res = jedis.get(key);
            if (StringUtils.isNotEmpty(res)) {
                resObj = JSON.parseObject(res);
            } else {

                List<JSONObject> jsonObjects = MyPhoenixUtil.queryList(sql, JSONObject.class);
                if (ObjectUtils.isNotEmpty(jsonObjects)) {
                    resObj = jsonObjects.get(0);
                    if (jedis != null) {
                        jedis.setex(key, 24 * 3600, resObj.toString());
                    }
                }

            }

        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

        return resObj;

    }


    //根据key让Redis中的缓存失效
    public static void deleteCached(String tableName, String id) {
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = MyRedisUtil.getJedis();
            // 通过key清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {

        //select * from REALTIME_FLINK.DIM_USER_INFO where ID='7'

//        JSONObject object = getDimInfoNoCache(MallConfig.HABSE_SCHEMA + ".DIM_USER_INFO", Tuple2.of("ID", "7"));
        JSONObject object = getDimInfo(MallConfig.HABSE_SCHEMA + ".DIM_USER_INFO", Tuple2.of("ID", "7"));

        System.out.println(object);


    }


}
