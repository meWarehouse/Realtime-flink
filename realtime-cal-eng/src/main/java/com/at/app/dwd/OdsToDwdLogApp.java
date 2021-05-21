package com.at.app.dwd;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.at.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zero
 * @create 2021-05-12 20:01
 */
public class OdsToDwdLogApp {

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


    private static final int PARALLELISM = 4;
    private static final String TOPIC_START="dwd_start_log";
    private static final String TOPIC_DISPLAY="dwd_display_log";
    private static final String TOPIC_PAGE="dwd_page_log";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/mall/checkpoint/odstodwdlogApp"));



        OutputTag<String> startTag = new OutputTag<String>("start-tag"){};
        OutputTag<String> displayTag = new OutputTag<String>("display-tag"){};


        SingleOutputStreamOperator<String> res = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_log_app_group"))
                //转化为 json
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String data) throws Exception {
                        return JSONObject.parseObject(data);
                    }
                })
                //按 设备id(mid) 分流
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObj) throws Exception {
                        return jsonObj.getJSONObject("common").getString("mid");
                    }
                })
                //识别新老访客
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    //定义一个状态变量保存设备第一次登录的时间  如果有10亿用户怎么办？？？ 100亿呢？？？？？
                    private ValueState<String> midFirstLoginTime;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        midFirstLoginTime = getRuntimeContext().getState(new ValueStateDescriptor<String>("mid-first-login-time", String.class));
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        //在common中已有 "is_new" 可以判断是否为新用户 但是肯不准确 所以需要再次判断
                        // is_new=0 不是新用户(无需判断) is_new=1 新用户
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) {
                            String ts = midFirstLoginTime.value();
                            if (StringUtils.isNotEmpty(ts)) {
                                String date = sdf.format(new Date(jsonObject.getLongValue("ts")));
                                if (!ts.equals(date)) {
                                    //不是新用户 已存在状态并且保存的日期状态不是今天 则说明该设备不是今天的新增设备 则修改改正其状态
                                    jsonObject.getJSONObject("common").put("is_new", "0");
                                }
                            }
                        }
                        return jsonObject;
                    }
                })
                //分流
                .process(new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                        String start = value.getString("start");
                        String streamStr = value.toString();

                        if (StringUtils.isNotEmpty(start)) {
                            //启动日志侧输出流
                            ctx.output(startTag, streamStr);
                        } else {
                            //主输出流 曝光日志也可以看做是页面日志的一种
                            out.collect(streamStr);

                            //分解出曝光日志
                            JSONArray jsonArray = value.getJSONArray("displays");
                            if (jsonArray != null && jsonArray.size() > 0) {
                                for (int i = 0; i < jsonArray.size(); i++) {
                                    JSONObject object = jsonArray.getJSONObject(i);
                                    object.put("page_id",value.getJSONObject("page").getString("page_id"));
                                    //曝光日志测输出流
                                    ctx.output(displayTag,object.toString());

                                }

                            }
                        }
                    }
                });

//        res.getSideOutput(startTag).print("start>>>");
//        res.print("page>>>>");
//        res.getSideOutput(displayTag).print("display>>>");

        //将数据流发往kafka
        res.addSink(MyKafkaUtil.getKafkaSink(TOPIC_PAGE));
        res.getSideOutput(startTag).addSink(MyKafkaUtil.getKafkaSink(TOPIC_START));
        res.getSideOutput(displayTag).addSink(MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY));


        env.execute();

    }

}
