package com.at.retime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.at.retime.bean.FlinkKafkaConsumerRecord;
import com.at.retime.common.Constant;
import com.at.retime.util.CheckpointUtil;
import com.at.retime.util.MyKafkaUtil;
import com.at.retime.util.ParseArgsUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PublicKey;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @create 2021-10-12
 */
public class BaseLogApp {

    /**
     * 数据格式 files 文件夹中查看
     * <p>
     * 曝光日志
     * {
     * "common":{},
     * "page":{},
     * "display":[],
     * "actions":[],
     * "ts":1231234
     * }
     * --------------------------
     * 页面日志
     * {
     * "common":{},
     * "page":{},
     * "ts":1231234
     * }
     * ------------------------
     * 启动日志
     * {
     * "common":{},
     * "start":{},
     * "ts":1231234
     * }
     * -----------------------------
     * 错误日志
     * {
     * "common":{},
     * "err":{},
     * "page":{},
     * "ts":1245346
     * }
     * {
     * "actions":[],
     * "common":{},
     * "display":[],
     * "err":{},
     * "page":{},
     * "ts":1324566
     * }
     * <p>
     * 将启动日志单独分流出
     * 将曝光日志中的display取出并添加当前曝光页的信息
     * <p>
     * <p>
     * TODO
     * 1 获取kafka中的数据
     * 2 识别新老访客
     * 3 分流
     * 将日志数据分为3类, 页面日志、启动日志和曝光日志
     * 4 分流后发往指定的主题
     */

    private static Logger logger = LoggerFactory.getLogger(BaseLogApp.class);

    private static final String TOPIC_START="dwd_start_log";
    private static final String TOPIC_DISPLAY="dwd_display_log";
    private static final String TOPIC_PAGE="dwd_page_log";

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParseArgsUtil.init(args, false);

        System.out.println("envType:" + parameterTool.get(Constant.RUN_TYPE));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (Constant.ENV_TYPE.equals(parameterTool.get(Constant.RUN_TYPE)) || Constant.ENV_LOCAL_TYPE.equals(parameterTool.get(Constant.RUN_TYPE))) {
            env.setParallelism(1);
        } else {
            env.setParallelism(4);
        }

        //set checkpoint
        CheckpointUtil.enableCheckpoint(env, parameterTool);

        OutputTag<String> startTag = new OutputTag<String>("start-tag"){};
        OutputTag<String> displayTag = new OutputTag<String>("display-tag"){};

        String groupId = "ods_dwd_base_log_app";
        String topic = "ods_base_log";
        FlinkKafkaConsumer<FlinkKafkaConsumerRecord> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        kafkaConsumer.setStartFromEarliest();

        System.out.println("kafka");

        SingleOutputStreamOperator<String> streamOperator = env.addSource(kafkaConsumer)
                //map
                .flatMap(new FlatMapFunction<FlinkKafkaConsumerRecord, JSONObject>() {
                    @Override
                    public void flatMap(FlinkKafkaConsumerRecord record, Collector<JSONObject> out) throws Exception {
                        if (record.getMsg() != null) {
                            JSONObject jsonObject = JSON.parseObject(new String(record.getMsg(), "utf-8"));
                            out.collect(jsonObject);
                        }
                    }
                })
                //mid keyBy
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("mid");
                    }
                })
                //识别新老用户
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    //声明第一次访问日期的状态
                    private ValueState<String> firstVisitDataState;
                    //                    private DateTimeFormatter dateTimeFormatter;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDataState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first-visited-state", String.class));
//                        dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.CANADA);
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        //在common中已有 "is_new" 可以判断是否为新用户 但是肯不准确 所以需要再次判断
                        // is_new=0 不是新用户(无需判断) is_new=1 新用户
                        String isNew = value.getJSONObject("common").getString("is_new");

                        if ("1".equals(isNew)) {
                            //可能需要修复
                            if (StringUtils.isNotEmpty(firstVisitDataState.value())) {
                                if (!firstVisitDataState.value().equals(sdf.format(new Date(value.getLongValue("ts"))))) {
                                    //不是新用户 已存在状态并且保存的日期状态不是今天 则说明该设备不是今天的新增设备 则修改改正其状态
                                    value.getJSONObject("common").put("is_new", 0);
                                }
                            } else {
                                //确实是首次访问
                                firstVisitDataState.update(sdf.format(new Date(value.getLongValue("ts"))));
                            }
                        }

                        return value;
                    }
                })
                //分流
                .process(new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                        String startLog = value.getString("start");
                        String streamStr = value.toJSONString();

                        if (StringUtils.isNotEmpty(startLog)) {
                            //启动日志侧输出流
                            ctx.output(startTag, streamStr);
                        } else {
                            //主输出流 曝光日志也可以看做是页面日志的一种
                            out.collect(streamStr);

                            //分解出曝光日志
                            JSONArray displaysArr = value.getJSONArray("displays");
                            if (displaysArr != null && displaysArr.size() > 0) {
                                for (int i = 0; i < displaysArr.size(); i++) {
                                    JSONObject displayLog = displaysArr.getJSONObject(i);
                                    displayLog.put("page_id", value.getJSONObject("page").getString("page_id"));
                                    //曝光日志测输出流
                                    ctx.output(displayTag, displayLog.toJSONString());
                                }
                            }
                        }
                    }
                });


        streamOperator.getSideOutput(startTag).addSink(MyKafkaUtil.getKafkaSink(TOPIC_START));
        streamOperator.getSideOutput(displayTag).addSink(MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY));
        streamOperator.addSink(MyKafkaUtil.getKafkaSink(TOPIC_PAGE));


        env.execute("BaseLogApp Job");

    }

}
