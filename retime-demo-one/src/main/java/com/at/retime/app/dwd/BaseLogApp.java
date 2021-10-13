package com.at.retime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.at.retime.bean.FlinkKafkaConsumerRecord;
import com.at.retime.common.Constant;
import com.at.retime.util.CheckpointUtil;
import com.at.retime.util.MyKafkaUtil;
import com.at.retime.util.ParseArgsUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @create 2021-10-12
 */
public class BaseLogApp {

    /**
     *
     *
     * 数据格式 files 文件夹中查看
     *
     *  曝光日志
     *  {
     *      "common":{},
     *      "page":{},
     *      "display":[],
     *      "actions":[],
     *      "ts":1231234
     *  }
     *  --------------------------
     *  页面日志
     *  {
     *      "common":{},
     *      "page":{},
     *      "ts":1231234
     *  }
     *  ------------------------
     *  启动日志
     *  {
     *       "common":{},
     *       "start":{},
     *       "ts":1231234
     *  }
     *  -----------------------------
     *  错误日志
     *  {
     *      "common":{},
     *      "err":{},
     *      "page":{},
     *      "ts":1245346
     *  }
     *  {
     *      "actions":[],
     *      "common":{},
     *      "display":[],
     *      "err":{},
     *      "page":{},
     *      "ts":1324566
     *  }
     *
     *  将启动日志单独分流出
     *  将曝光日志中的display取出并添加当前曝光页的信息
     *
     *
     * TODO
     *  1 获取kafka中的数据
     *  2 识别新老访客
     *  3 分流
     *      将日志数据分为3类, 页面日志、启动日志和曝光日志
     *  4 分流后发往指定的主题
     *
     *
     */

    private static Logger logger =  LoggerFactory.getLogger(BaseLogApp.class);

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParseArgsUtil.init(args, false);

        System.out.println("envType:" + parameterTool.get(Constant.RUN_TYPE));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if(Constant.ENV_TYPE.equals(parameterTool.get(Constant.RUN_TYPE)) || Constant.ENV_LOCAL_TYPE.equals(parameterTool.get(Constant.RUN_TYPE))){
            env.setParallelism(1);
        }else {
            env.setParallelism(4);
        }

        //set checkpoint
        CheckpointUtil.enableCheckpoint(env,parameterTool);

        String groupId = "ods_dwd_base_log_app";
        String topic = "ods_base_log";
        FlinkKafkaConsumer<FlinkKafkaConsumerRecord> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        kafkaConsumer.setStartFromEarliest();

        System.out.println("kafka");

        env.addSource(kafkaConsumer)
                .flatMap(new FlatMapFunction<FlinkKafkaConsumerRecord, JSONObject>() {
                    @Override
                    public void flatMap(FlinkKafkaConsumerRecord record, Collector<JSONObject> out) throws Exception {
                        if(record.getMsg() != null){
                            JSONObject jsonObject = JSON.parseObject(new String(record.getMsg(), "utf-8"));
                            out.collect(jsonObject);
                        }
                    }
                }).print();




        env.execute("BaseLogApp Job");

    }

}
