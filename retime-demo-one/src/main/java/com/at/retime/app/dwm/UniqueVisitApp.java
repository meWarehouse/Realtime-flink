package com.at.retime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.at.retime.bean.FlinkKafkaConsumerRecord;
import com.at.retime.common.Constant;
import com.at.retime.helper.EnvironmentHelper;
import com.at.retime.helper.ParallelismHelper;
import com.at.retime.helper.SlotHelper;
import com.at.retime.utils.CheckpointUtil;
import com.at.retime.utils.MyKafkaUtil;
import com.at.retime.utils.ParseArgsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zero
 * @create 2021-05-18 20:37
 */
@Slf4j
public class UniqueVisitApp {

    //  ./bin/flink run -t yarn-per-job -c com.at.retime.app.dwm.UniqueVisitApp /opt/module/flink-1.13.2/retime-demo-one-1.0-SNAPSHOT.jar

    //  ./bin/flink run -c com.at.retime.app.dwm.UniqueVisitApp /opt/module/flink-1.13.2/retime-demo-one-1.0-SNAPSHOT.jar --envType=production
    /**
     * TODO
     * UV 数据流过滤
     * 1 从kafka 的 dwd_page_log 中读取数据
     * 2 根据设备id 分流
     * 3 直接筛掉 last_page_id 不为空的字段，因为只要有上一页，说明这条不是这个用户进入的首个页面
     * 4 写回kafka
     *
     *
     * 根据设备id分流，这样每条流中都为同一设备的流数据
     * 根据last_page_id判断当前流是否为第一次访问(last_page_id 不为空 着说明当前页面是由上一个一面跳转过来的)
     * 如果 last_page_id 为空则将该条数据的时间保存到状态变量中
     *
     */


    public static void main(String[] args) throws Exception {


        ParameterTool parameterTool = ParseArgsUtil.init(args, false);

        log.info("envType:{}",parameterTool.get(Constant.RUN_TYPE));
        System.out.println("envType:"+parameterTool.get(Constant.RUN_TYPE));


//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);

        StreamExecutionEnvironment env = EnvironmentHelper.getStreamExecutionEnvironment(parameterTool);

        env.setParallelism(10);

        CheckpointUtil.enableCheckpoint(env,parameterTool);


//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/uniquevisitApp"));

//        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaSource("dwd_page_log", "unique_visit_app");
//        kafkaConsumer.setStartFromEarliest();

        FlinkKafkaConsumer<FlinkKafkaConsumerRecord> kafkaConsumer = MyKafkaUtil.getKafkaConsumer("dwd_page_log", "unique_visit_app");
        kafkaConsumer.setStartFromEarliest();



        DataStreamSource<FlinkKafkaConsumerRecord> kafkaConsumerRecordDataStreamSource = env.addSource(kafkaConsumer);

        //DataStreamSource  -> SingleOutputStreamOperator
//        kafkaConsumerRecordDataStreamSource.setParallelism(1);
//        kafkaConsumerRecordDataStreamSource.slotSharingGroup("");
//        kafkaConsumerRecordDataStreamSource.name("");
        ParallelismHelper.setParallelism(kafkaConsumerRecordDataStreamSource,parameterTool,1);
        SlotHelper.setSlot(kafkaConsumerRecordDataStreamSource,parameterTool,"test1","group1");


        SingleOutputStreamOperator<JSONObject> flatMapStream = kafkaConsumerRecordDataStreamSource
                .flatMap(new FlatMapFunction<FlinkKafkaConsumerRecord, JSONObject>() {
                    @Override
                    public void flatMap(FlinkKafkaConsumerRecord record, Collector<JSONObject> out) throws Exception {

                        if (record.getMsg() != null && record.getMsg().length > 0) {

                            JSONObject jsonObject = JSON.parseObject(new String(record.getMsg(), "utf-8"));
                            out.collect(jsonObject);
                        }

                    }
                });

        //SingleOutputStreamOperator
//        flatMapStream.slotSharingGroup("");
//        flatMapStream.name("");
//        flatMapStream.setParallelism(1);

        ParallelismHelper.setParallelism(flatMapStream,parameterTool,1);
        SlotHelper.setSlot(flatMapStream,parameterTool,"test2","group2");

        KeyedStream<JSONObject, String> keyedStream = flatMapStream
//                .map(JSONObject::parseObject)
                .keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        // KeyedStream 没有


        SingleOutputStreamOperator<JSONObject> filterStream = keyedStream
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
                });

        //SingleOutputStreamOperator
//        filterStream.setParallelism(1);
//        filterStream.slotSharingGroup("");
//        filterStream.name("");

        ParallelismHelper.setParallelism(filterStream,parameterTool,2);
        SlotHelper.setSlot(filterStream,parameterTool,"test3","group3");

        SingleOutputStreamOperator<String> resDS = filterStream
                .map(jsonObject -> jsonObject.toString());


        //SingleOutputStreamOperator
//        resDS.slotSharingGroup("");
//        resDS.name("");
//        resDS.setParallelism(1);
        ParallelismHelper.setParallelism(resDS,parameterTool,1);
        SlotHelper.setSlot(resDS,parameterTool,"test4","group4");

//        resDS.print(">>>>>");


        //写回kafka

        DataStreamSink<String> sinkStream = resDS.addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));

        //DataStreamSink
        sinkStream.slotSharingGroup("");
        sinkStream.name("");
        sinkStream.setParallelism(1);

        ParallelismHelper.setParallelism(sinkStream,parameterTool,1);
        SlotHelper.setSlot(sinkStream,parameterTool,"test5","group5");

        env.execute();

    }
}
