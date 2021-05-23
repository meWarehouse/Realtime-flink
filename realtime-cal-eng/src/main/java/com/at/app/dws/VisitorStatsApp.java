package com.at.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.at.bean.VisitorStats;
import com.at.utils.ClickHouseUtil;
import com.at.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.glassfish.jersey.internal.util.collection.StringIgnoreCaseKeyComparator;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @author zero
 * @create 2021-05-22 9:28
 */
public class VisitorStatsApp {

    /**
     *
     *  读取 dwm dwd 层的 pv(dwd_page_log) uv(dwm_unique_visit) 跳出(dwm_user_jump_detail) 明细数据
     *  将各条流转换为 VisitorStats 格式 再进行 union
     *  设置水位线
     *  根据维度进行分流
     *  再聚合窗口内的数据
     *  最后将数据写入 clickHouse
     *
     *
     */



    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/visitorstatsApp"));


        String pageTopic = "dwd_page_log";
        String uvTopic = "dwm_unique_visit";
        String jumpTopic = "dwm_user_jump_detail";

        String groupId = "visitor_stats_app";

        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getKafkaSource(pageTopic, groupId));

        //pv(dwd_page_log)
        SingleOutputStreamOperator<VisitorStats> pvDS = pageDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                return new VisitorStats(
                        "", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0L,
                        1L,
                        0L,
                        0L,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts")
                );
            }
        });

        //sv
        SingleOutputStreamOperator<VisitorStats> svDS = pageDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                return new VisitorStats(
                        "", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0L,
                        0L,
                        1L,
                        0L,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts")
                );
            }
        });

        // uv(dwm_unique_visit)
        SingleOutputStreamOperator<VisitorStats> uvDS = env.addSource(MyKafkaUtil.getKafkaSource(uvTopic, groupId))
                .map(new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String s) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        return new VisitorStats(
                                "", "",
                                jsonObject.getJSONObject("common").getString("vc"),
                                jsonObject.getJSONObject("common").getString("ch"),
                                jsonObject.getJSONObject("common").getString("ar"),
                                jsonObject.getJSONObject("common").getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                jsonObject.getJSONObject("page").getLong("during_time"),
                                jsonObject.getLong("ts")
                        );
                    }
                });

        // 跳出(dwm_user_jump_detail)
        SingleOutputStreamOperator<VisitorStats> juDS = env.addSource(MyKafkaUtil.getKafkaSource(jumpTopic, groupId))
                .map(new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String s) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        return new VisitorStats(
                                "", "",
                                jsonObject.getJSONObject("common").getString("vc"),
                                jsonObject.getJSONObject("common").getString("ch"),
                                jsonObject.getJSONObject("common").getString("ar"),
                                jsonObject.getJSONObject("common").getString("is_new"),
                                0L,
                                0L,
                                0L,
                                1L,
                                jsonObject.getJSONObject("page").getLong("during_time"),
                                jsonObject.getLong("ts")
                        );
                    }
                });


//        pvDS.print("pv>>>>>");
//        svDS.print("sv>>>>>");  //https://github.com/zhisheng17/flink-learning
//        uvDS.print("uv>>>>>");
//        juDS.print("ju>>>>>");


        SingleOutputStreamOperator<VisitorStats> res = pvDS.union(svDS, uvDS, juDS)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(
                        new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats visitorStats, long l) {
                                return visitorStats.getTs();
                            }
                        }
                ))
                .keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                        return Tuple4.of(
                                visitorStats.getAr(),
                                visitorStats.getCh(),
                                visitorStats.getVc(),
                                visitorStats.getIs_new()
                        );
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(20L)))
                .reduce(
                        new ReduceFunction<VisitorStats>() {
                            @Override
                            public VisitorStats reduce(VisitorStats visitorStats, VisitorStats t1) throws Exception {
                                visitorStats.setPv_ct(visitorStats.getPv_ct() + t1.getPv_ct());
                                visitorStats.setUv_ct(visitorStats.getUv_ct() + t1.getUv_ct());
                                visitorStats.setSv_ct(visitorStats.getSv_ct() + t1.getSv_ct());
                                visitorStats.setUj_ct(visitorStats.getUj_ct() + t1.getUj_ct());
                                return visitorStats;
                            }
                        },
                        new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                            @Override
                            public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, Context context, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                for (VisitorStats visitorStats : iterable) {
                                    //获取窗口的开始时间
                                    String startDate = sdf.format(new Date(context.window().getStart()));
                                    //获取窗口的结束时间
                                    String endDate = sdf.format(new Date(context.window().getEnd()));
                                    visitorStats.setStt(startDate);
                                    visitorStats.setEdt(endDate);
                                    visitorStats.setTs(new Date().getTime());
                                    collector.collect(visitorStats);
                                }
                            }
                        }
                );


//        res.addSink(JdbcSink.sink(
//                "sql",
//                new JdbcStatementBuilder<VisitorStats>() {
//                    @Override
//                    public void accept(PreparedStatement statement, VisitorStats visitorStats) throws SQLException {
//
//                    }
//                },
//                new JdbcExecutionOptions.Builder(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().build()
//        ))

//        res.print(">>>>");
        res.addSink(ClickHouseUtil.getJdbcSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();

    }





}
