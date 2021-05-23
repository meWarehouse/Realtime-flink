package com.at.app.dwm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.at.app.func.DimAsyncFunction;
import com.at.bean.OrderDetail;
import com.at.bean.OrderInfo;
import com.at.bean.OrderWide;
import com.at.common.MallConfig;
import com.at.utils.MyKafkaUtil;
import org.apache.avro.Schema;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author zero
 * @create 2021-05-20 12:31
 */
public class OrderWideApp {


    /**
     * 获取订单信息数据流 dwd_order_info
     * 获取订单详情数据流 dwd_order_detail
     * <p>
     * 将两条流的格式转换为json
     * 设置事件时间
     * 指定关联的key
     * 进行数据明细关联
     * 关联维度表
     * 写回kafka
     */

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/orderwideApp"));

        env.setRestartStrategy(RestartStrategies.noRestart());

        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String groupId = "order_wide_group";

        String orderWideSinkTopic = "dwm_order_wide";


        //dwd_order_info
        KeyedStream<OrderInfo, Long> orderInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId))
                .map(new RichMapFunction<String, OrderInfo>() {

                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String s) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(s, OrderInfo.class);
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long l) {
                                return orderInfo.getCreate_ts();
                            }
                        }
                ))
                .keyBy(new KeySelector<OrderInfo, Long>() {
                    @Override
                    public Long getKey(OrderInfo orderInfo) throws Exception {
                        return orderInfo.getId();
                    }
                });

        //dwd_order_detail
        KeyedStream<OrderDetail, Long> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId))
                .map(new RichMapFunction<String, OrderDetail>() {
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String s) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(s, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(
                        ((orderDetail, l) -> orderDetail.getCreate_ts())
                ))
                .keyBy(OrderDetail::getOrder_id);


        OutputTag<String> outputTag = new OutputTag<String>("timeout") {
        };

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoDS
                .intervalJoin(orderDetailDS)
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });


        //省市关联
        SingleOutputStreamOperator<OrderWide> orderWideProvinceDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                new DimAsyncFunction<OrderWide>(MallConfig.HABSE_SCHEMA + ".DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );



        //用户维度关联
        SingleOutputStreamOperator<OrderWide> orderWideUserInfoDS = AsyncDataStream.unorderedWait(
                orderWideProvinceDS,
                new DimAsyncFunction<OrderWide>(MallConfig.HABSE_SCHEMA + ".DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                        //获取用户生日
                        String birthday = dimInfo.getString("BIRTHDAY");
                        //定义日期转换工具类
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        //将生日字符串转换为日期对象
                        Date birthdayDate = sdf.parse(birthday);
                        //获取生日日期的毫秒数
                        Long birthdayTs = birthdayDate.getTime();

                        //获取当前时间的毫秒数
                        Long curTs = System.currentTimeMillis();

                        //年龄毫秒数
                        Long ageTs = curTs - birthdayTs;
                        //转换为年龄
                        Long ageLong = ageTs / 1000L / 60L / 60L / 24L / 365L;
                        Integer age = ageLong.intValue();

                        //将维度中的年龄赋值给订单宽表中的属性
                        orderWide.setUser_age(age);

                        //将维度中的性别赋值给订单宽表中的属性
                        orderWide.setUser_gender(dimInfo.getString("GENDER"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );


        //SKU关联
        SingleOutputStreamOperator<OrderWide> orderWideSkuDS = AsyncDataStream.unorderedWait(
                orderWideUserInfoDS,
                new DimAsyncFunction<OrderWide>(MallConfig.HABSE_SCHEMA + ".DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        orderWide.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        orderWide.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //spu关联
        SingleOutputStreamOperator<OrderWide> orderWideSpuDS = AsyncDataStream.unorderedWait(
                orderWideSkuDS,
                new DimAsyncFunction<OrderWide>(MallConfig.HABSE_SCHEMA + ".DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //品类关联
        SingleOutputStreamOperator<OrderWide> orderWideCat3DS = AsyncDataStream.unorderedWait(
                orderWideSpuDS,
                new DimAsyncFunction<OrderWide>(MallConfig.HABSE_SCHEMA + ".DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getCategory3_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setCategory3_name(dimInfoJsonObj.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //品牌维度
        SingleOutputStreamOperator<OrderWide> res = AsyncDataStream.unorderedWait(
                orderWideCat3DS,
                new DimAsyncFunction<OrderWide>(MallConfig.HABSE_SCHEMA + ".DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        res.print(">>>>>");

        res.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));


        env.execute();


    }
}
