package com.at.retime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;

import com.at.retime.app.func.DimAsyncFunction;
import com.at.retime.bean.OrderWide;
import com.at.retime.bean.PaymentWide;
import com.at.retime.bean.ProductStats;
import com.at.retime.common.MallConfig;
import com.at.retime.common.MallConstant;
import com.at.retime.utils.ClickHouseUtil;
import com.at.retime.utils.DateTimeUtil;
import com.at.retime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author zero
 * @create 2021-05-22 22:23
 */
public class ProductStatsApp {

    /**
     *
     */


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/productstatsApp"));


        String groupId = "product_stats_app";


        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";


        //dwd_page_log 对点击和曝光数据
        SingleOutputStreamOperator<ProductStats> clickAndDisplayDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId))
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String s, Context context, Collector<ProductStats> collector) throws Exception {

                        /**
                         * 格式化转换 点击 及 曝光
                         *
                         * 点击：
                         *      当前页面的页面信息(page_id)为 good_detail
                         *      统计 什么时候 那件商品 点击次数
                         *
                         *  曝光：
                         *      displays 字段不为空的流数据
                         *      统计 什么时候 那件商品 曝光次数
                         *
                         */
                        JSONObject object = JSON.parseObject(s);
                        JSONObject page = object.getJSONObject("page");

                        Long ts = object.getLong("ts");

                        if ("good_detail".equals(page.getString("page_id"))) {
                            ProductStats productStats = ProductStats.builder().sku_id(page.getLong("item")).ts(ts).click_ct(1L).build();
                            collector.collect(productStats);
                        }

                        JSONArray displays = object.getJSONArray("displays");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                //判断是否曝光的某一个商品
                                if ("sku_id".equals(display.getString("item_type"))) {
                                    //获取商品id
                                    //封装曝光商品对象
                                    ProductStats productStats = ProductStats.builder().sku_id(display.getLong("item")).display_ct(1L).ts(ts).build();
                                    //向下游输出
                                    collector.collect(productStats);
                                }
                            }
                        }

                    }
                });


        //dwm_order_wide  对订单宽表进行转换
        SingleOutputStreamOperator<ProductStats> orderDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId))
                .map(s -> {
                    /**
                     * 统计
                     *      该条订单是那个商品的 下单的件数 支付的金额 支付的时间 订单数
                     */
                    //将json字符串转换为对应的订单宽表对象
                    OrderWide orderWide = JSON.parseObject(s, OrderWide.class);
                    //将字符串日期转换为毫秒数
                    Long ts = DateTimeUtil.toTs(orderWide.getCreate_time());
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(orderWide.getSku_id())
                            .order_sku_num(orderWide.getSku_num())
                            .order_amount(orderWide.getSplit_total_amount())
                            .ts(ts)
                            .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                            .build();
                    return productStats;
                });


        //dwm_payment_wide 支付流数据
        SingleOutputStreamOperator<ProductStats> paymentDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId))
                .map(s -> {
                    /**
                     *
                     * 那件商品 什么时候 支付金额 支付订单数
                     *
                     */
                    PaymentWide paymentWide = JSON.parseObject(s, PaymentWide.class);
                    Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                    return ProductStats.builder()
                            .sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                            .ts(ts)
                            .build();
                });

        //dwd_cart_info 购物车流数据
        SingleOutputStreamOperator<ProductStats> cartDS = env.addSource(MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId))
                .map(s -> {
                    /**
                     *
                     * 统计 什么时候 那件商品 加入购物车的次数
                     *
                     */
                    JSONObject jsonObj = JSON.parseObject(s);
                    //将字符串日期转换为毫秒数
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));

                    return ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .cart_ct(1L)
                            .ts(ts)
                            .build();
                });


        //dwd_favor_info  收藏流数据
        SingleOutputStreamOperator<ProductStats> favorDS = env.addSource(MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId))
                .map(s -> {
                    JSONObject jsonObj = JSON.parseObject(s);
                    //将字符串日期转换为毫秒数
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .favor_ct(1L)
                            .ts(ts)
                            .build();
                    return productStats;
                });

        //dwd_order_refund_info 退款流数据
        SingleOutputStreamOperator<ProductStats> refundDS = env.addSource(MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId))
                .map(s -> {
                    /**
                     * 什么商品 什么时候 退款金额 该商品被退款次数
                     */
                    JSONObject refundJsonObj = JSON.parseObject(s);
                    Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(
                                    new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts)
                            .build();
                    return productStats;
                });

        //dwd_comment_info 评价流数据
        SingleOutputStreamOperator<ProductStats> commentDS = env.addSource(MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId))
                .map(s -> {
                    /**
                     * 什么时候 那件商品 评价次数 评价内容
                     */
                    JSONObject commonJsonObj = JSON.parseObject(s);
                    Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                    Long goodCt = MallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(commonJsonObj.getLong("sku_id"))
                            .comment_ct(1L)
                            .good_comment_ct(goodCt)
                            .ts(ts)
                            .build();
                    return productStats;
                });


        SingleOutputStreamOperator<ProductStats> reduceDS = clickAndDisplayDS.union(orderDS, paymentDS, cartDS, favorDS, refundDS, commentDS)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(
                        (p, l) -> p.getTs()
                ))
                .keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                .reduce(
                        new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                                stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                                stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                                stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                                stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                                stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                                stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                                stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                                stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                                stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                                stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                                stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                                stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                                stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                                stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                                stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                                stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                                return stats1;
                            }
                        },
                        new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void process(Long aLong, Context context, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {
                                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                for (ProductStats productStats : iterable) {
                                    productStats.setStt(simpleDateFormat.format(new Date(context.window().getStart())));
                                    productStats.setEdt(simpleDateFormat.format(new Date(context.window().getEnd())));
                                    productStats.setTs(new Date().getTime());
                                    collector.collect(productStats);
                                }
                            }
                        }
                );


        //关联维度信息

        // sku 维度
        SingleOutputStreamOperator<ProductStats> withSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>(MallConfig.HABSE_SCHEMA + ".DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats p) {
                        return p.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfoJsonObj.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        // spu
        SingleOutputStreamOperator<ProductStats> withSpuDS = AsyncDataStream.unorderedWait(
                withSkuDS,
                new DimAsyncFunction<ProductStats>(MallConfig.HABSE_SCHEMA + ".DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats obj, JSONObject dimInfoJsonObj) throws Exception {
                        obj.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        //品牌
        SingleOutputStreamOperator<ProductStats> withTMDS = AsyncDataStream.unorderedWait(
                withSpuDS,
                new DimAsyncFunction<ProductStats>(MallConfig.HABSE_SCHEMA + ".DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats obj, JSONObject dimInfoJsonObj) throws Exception {
                        obj.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        //cat3
        SingleOutputStreamOperator<ProductStats> withCat3DS = AsyncDataStream.unorderedWait(
                withTMDS,
                new DimAsyncFunction<ProductStats>(MallConfig.HABSE_SCHEMA + ".DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats obj, JSONObject dimInfoJsonObj) throws Exception {
                        obj.setCategory3_name(dimInfoJsonObj.getString("NAME"));
                    }
                },
                60, TimeUnit.SECONDS
        );


        withCat3DS.print(">>>>");

        withCat3DS.addSink(
                ClickHouseUtil.<ProductStats>getJdbcSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        withCat3DS
                .map(productStat->JSON.toJSONString(productStat,new SerializeConfig(true)))
                .addSink(MyKafkaUtil.getKafkaSink("dws_product_stats"));

        env.execute();


    }


}
