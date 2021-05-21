package com.at.app.dwd;

import com.alibaba.fastjson.JSON;
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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zero
 * @create 2021-05-12 15:28
 */
public class OdsToDwdLogAppTest {




    public static void main(String[] args) throws Exception {

        /*
            TODO
                1.准备环境
                2.从kafka中读取数据
                3.分流
                    3.1 判断新老用户
                    3.2 根据日志内容将日志分为 页面日志、启动日志和曝光日志
                4.将不同流的数据写回到kafka的不同topic中
         */

        //创建flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度 并行度=分区数
        env.setParallelism(1);

        //设置checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/mall/checkpoint/odstodwdlogApp"));


        //从kafka中读取数据
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";

        DataStreamSource<String> kfDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kfDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String val) throws Exception {
                return JSON.parseObject(val);
            }
        });

        //识别新老用户
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject data) throws Exception {
                return data.getJSONObject("common").getString("mid");
            }
        });

        /*
            前端也会对新老状态进行记录，有可能会不准，咱们这里是再次做一个确认
            保存mid某天方法情况（将首次访问日期作为状态保存起来），等后面该设备在有日志过来的时候，从状态中获取日期
            和日志产生日志进行对比。如果状态不为空，并且状态日期和当前日期不相等，说明是老访客，如果is_new标记是1，那么对其状态进行修复

         */
        SingleOutputStreamOperator<JSONObject> jsonDSWithFlag = midKeyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {

            //定义一个保存mid的状态变量
            private ValueState<String> firstVisitDateState;
            //定义一个日期格式化对象
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("newMidDateState", String.class));
                sdf = new SimpleDateFormat("yyyyMMdd");
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

                //获取当前日志标记状态
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");

                if ("1".equals(isNew)) {
                    //获取当前的mid对象的状态
                    String stateDate = firstVisitDateState.value();
                    String currDate = sdf.format(new Date(jsonObject.getLong("ts")));

                    if (StringUtils.isNotEmpty(stateDate)) {
                        //判断是否为统一天数据
                        if (stateDate.equals(currDate)) {
                            jsonObject.getJSONObject("common").put("is_new", "0");
                        }
                    } else {
                        firstVisitDateState.update(currDate);
                    }

                }

                return jsonObject;
            }
        });

        /*

            根据日志数据内容,将日志数据分为3类, 页面日志、启动日志和曝光日志。
            页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流
            侧输出流：1)接收迟到数据    2)分流

         */
        OutputTag<String> startTag = new OutputTag<String>("start-tag"){};
        OutputTag<String> displayTag = new OutputTag<String>("display-tag"){};


        SingleOutputStreamOperator<String> pageDs = jsonDSWithFlag.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                        JSONObject start = value.getJSONObject("start");

                        String dataStr = value.toString();

                        if (start != null && start.size() > 0) {
                            //启动日志
                            ctx.output(startTag, dataStr);
                        } else {
                            //页面日志 输出到主流
                            out.collect(dataStr);

                            //获取曝光日志
                            JSONArray displays = value.getJSONArray("displays");
                            if (displays != null && displays.size() > 0) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject displaysJsonObj = displays.getJSONObject(i);
                                    displaysJsonObj.put("page_id", value.getJSONObject("page").getString("page_id"));
                                    ctx.output(displayTag, displaysJsonObj.toString());
                                }
                            }

                        }


                    }
                }
        );

        pageDs.getSideOutput(startTag).print("start>>>");
        pageDs.getSideOutput(displayTag).print("displays>>");

        pageDs.print("page>>>");




        env.execute();


    }

}
