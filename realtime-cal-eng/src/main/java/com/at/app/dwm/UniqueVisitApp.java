package com.at.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.at.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zero
 * @create 2021-05-18 20:37
 */
public class UniqueVisitApp {

    /**
     * TODO
     * UV 数据流过滤
     * 1 从kafka 的 dwd_page_log 中读取数据
     * 2 根据设备id 分流
     * 3 直接筛掉 last_page_id 不为空的字段，因为只要有上一页，说明这条不是这个用户进入的首个页面
     * 4 写回kafka
     */


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/uniquevisitApp"));

        SingleOutputStreamOperator<String> resDS = env.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", "unique_visit_app"))
                .map(JSONObject::parseObject)
                .keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .filter(new RichFilterFunction<JSONObject>() {

                    private ValueState<String> uvListState;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("uv-list-state", String.class);
                        descriptor.enableTimeToLive(ttlConfig);

                        uvListState = getRuntimeContext().getState(descriptor);

                        sdf = new SimpleDateFormat("yyyyMMdd");

                    }

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {

                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                        if (StringUtils.isNotEmpty(lastPageId)) return false;

                        String ts = uvListState.value();
                        String curr = sdf.format(new Date(jsonObject.getLong("ts")));

                        if (StringUtils.isNotEmpty(ts) && ts.equals(curr)) return false;

                        uvListState.update(curr);

                        return true;

                    }
                })
                .map(jsonObject -> jsonObject.toString());


//        resDS.print(">>>>>");


        //写回kafka

        resDS.addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));

        env.execute();

    }
}
