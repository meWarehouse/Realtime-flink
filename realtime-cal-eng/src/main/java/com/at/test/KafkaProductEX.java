package com.at.test;

import com.alibaba.fastjson.JSONObject;
import com.at.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author zero
 * @create 2021-06-03 20:03
 */
public class KafkaProductEX {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("D:\\workspace\\workspace2021\\bigdata\\实时-flink\\Realtime-flink\\realtime-cal-eng\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        String[] split = value.split(",");
                        UserBehaviour userBehaviour = new UserBehaviour(
                                Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                Long.parseLong(split[4]) * 1000
                        );
                        return JSONObject.toJSONString(userBehaviour);
                    }
                })
                .addSink(MyKafkaUtil.getKafkaSink("user_behavior"));


        env.execute();

    }
}
