package com.at.app.dws;

import com.at.app.func.KeywordUDTF;
import com.at.bean.KeywordStats;
import com.at.common.MallConstant;
import com.at.utils.ClickHouseUtil;
import com.at.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zero
 * @create 2021-05-23 19:28
 */
public class KeywordStatsApp {

    /**
     *
     * dWS层 搜索关键字
     *
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/keywordstatsApp"));


        EnvironmentSettings setting = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, setting);

        String pageViewSourceTopic = "dwd_page_log";
        String groupId = "keywordstats_app_group";

        tEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        tEnv.executeSql(
                "CREATE TABLE page_view (" +
                        " common MAP<STRING, STRING>," +
                        " page MAP<STRING, STRING>," +
                        " ts BIGINT," +
                        " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                        " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                        " WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")"
        );

//        tEnv.executeSql("select * from page_view");

        Table wordsTable = tEnv.sqlQuery(
                "select" +
                        " page['item'] as words," +
                        " rowtime" +
                        " from page_view" +
                        " where page['page_id']='good_list' and page['item'] is not null"
        );

        Table keywordTable = tEnv.sqlQuery(
                "SELECT " +
                        " keyword, " +
                        " rowtime" +
                        " FROM " + wordsTable + "," +
                        " LATERAL TABLE(ik_analyze(words)) as t(keyword)"
        );

        Table res = tEnv.sqlQuery(
                "SELECT" +
                        " DATE_FORMAT(TUMBLE_START(rowtime,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                        " DATE_FORMAT(TUMBLE_END(rowtime,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                        " keyword," +
                        " COUNT(*) ct," +
                        "'" + MallConstant.KEYWORD_SEARCH + "' source," +
                        " UNIX_TIMESTAMP()*1000 ts " +
                        " FROM " + keywordTable +
                        " group by TUMBLE(rowtime,INTERVAL '10' SECOND),keyword"
        );

//        tEnv.executeSql("select * from res").print();

        tEnv.toAppendStream(res, KeywordStats.class)
                .addSink(ClickHouseUtil.getJdbcSink("insert into keyword_stats (keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));


        env.execute();

    }

}
