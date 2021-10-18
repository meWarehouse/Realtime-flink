package com.at.retime.app.dwm;

import com.alibaba.fastjson.JSONObject;

import com.at.retime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author zero
 * @create 2021-05-19 22:33
 */
public class UserJumpDetailApp {

    /**
     * 用户跳出统计准备
     * <p>
     * 跳出就是用户成功访问了网站的一个页面后就退出，不在继续访问网站的其它页面。而跳出率就是用跳出次数除以访问次数
     * 1 该页面是用户近期访问的第一个页面
     * 2 首次访问之后很长一段时间（自己设定），用户没继续再有其他页面的访问
     * </p>
     */

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/userjumpdetailApp"));

//        DataStream<String> dataStream = env
//            .fromElements(
//                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                    "\"home\"},\"ts\":150000} ",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                    "\"detail\"},\"ts\":300000} "
//            );

        KeyedStream<JSONObject, String> keyedStream = env.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", "user_jump_detail_group"))
        .map(JSONObject::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                }))
                .keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));


        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return StringUtils.isEmpty(lastPageId);
                    }
                })
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        return StringUtils.isNotEmpty(pageId);
                    }
                })
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                        return false;
                    }
                })
                .within(Time.milliseconds(10000L));

        NFACompiler.NFAFactory<JSONObject> nfaFactory = NFACompiler.compileFactory(pattern, false);


        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};
//
//        CEP.pattern(keyedStream,pattern)
//                .select(
//                        timeoutTag,
//                        new PatternTimeoutFunction<JSONObject, String>() {
//                            @Override
//                            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
//                                return "";
//                            }
//                        },
//                        new PatternSelectFunction<JSONObject, Object>() {
//                            @Override
//                            public Object select(Map<String, List<JSONObject>> map) throws Exception {
//                                return null;
//                            }
//                        }
//                )

        SingleOutputStreamOperator<Object> filterDS = CEP.pattern(keyedStream, pattern)
                .flatSelect(
                        timeoutTag,
                        new PatternFlatTimeoutFunction<JSONObject, String>() {
                            @Override
                            public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                                List<JSONObject> firstEvents = map.get("first");
                                for (JSONObject event : firstEvents) {
                                    collector.collect(event.toString());
                                }
                            }
                        },
                        new PatternFlatSelectFunction<JSONObject, Object>() {
                            @Override
                            public void flatSelect(Map<String, List<JSONObject>> map, Collector<Object> collector) throws Exception {

                            }
                        }
                );

        filterDS.getSideOutput(timeoutTag).addSink(MyKafkaUtil.getKafkaSink("dwm_user_jump_detail"));


        env.execute();


    }

}
