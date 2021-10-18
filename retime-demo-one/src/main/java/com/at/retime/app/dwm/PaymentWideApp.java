package com.at.retime.app.dwm;

import com.alibaba.fastjson.JSON;

import com.at.retime.bean.OrderWide;
import com.at.retime.bean.PaymentInfo;
import com.at.retime.bean.PaymentWide;
import com.at.retime.utils.DateTimeUtil;
import com.at.retime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author zero
 * @create 2021-05-21 20:21
 */
public class PaymentWideApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/paymentwideApp"));


        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String groupId = "paymentwide_app_group";

        String paymentWideSinkTopic = "dwm_payment_wide";

        //dwd_payment_info
        KeyedStream<PaymentInfo, Long> paymentDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId))
                .map(json -> JSON.parseObject(json, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(
                        (paymentInfo, l) -> DateTimeUtil.toTs(paymentInfo.getCallback_time())
                ))
                .keyBy(PaymentInfo::getOrder_id);

        //dwm_order_wide
        KeyedStream<OrderWide, Long> orderWideDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId))
                .map(json -> JSON.parseObject(json, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(
                        ((orderWide, l) -> DateTimeUtil.toTs(orderWide.getCreate_time()))
                ))
                .keyBy(OrderWide::getOrder_id);


//        paymentDS.print("paymentDS>>>>");
//        orderWideDS.print("orderWideDS>>>>");

        SingleOutputStreamOperator<PaymentWide> res = paymentDS.intervalJoin(orderWideDS)
                .between(Time.seconds(-1800L), Time.seconds(0L))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

//        res.print(">>>>>>>>>>>>>");

        res.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));



        env.execute();

    }

}
