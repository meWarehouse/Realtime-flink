package com.at.retime.app.func;


import com.alibaba.fastjson.JSONObject;

import com.at.retime.bean.TableProcess;
import com.at.retime.common.MallConfig;
import com.at.retime.utils.MyPhoenixUtil;
import com.at.retime.utils.MySQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

/**
 * @author zero
 * @create 2021-05-14 21:22
 */
@Slf4j
public class TableDataDiversionFunction extends ProcessFunction<JSONObject, JSONObject> {


    /**
     * TODO
     *  分流逻辑
     *      1 初始化时，先到mysql中查询出哪些表是要导入hbase哪些是写回hbase
     *      2 在查询出来后应当在内存中缓存一份，而不是每来一条数据就查出一下数据库
     *      3 因为mysql中的数据可能发生改变，所以需要定时去更新缓存中的数据
     *      4 根据查出的信息判断是否需要在hbase中创建相应的表
     *      5 标记数据流向
     */

    private OutputTag<JSONObject> tag;

    //缓存mysql中查出的表信息
    private Map<String, TableProcess> tableProcessMap = new HashMap<>();

    //缓存已经创建了的表
    private Set<String> existsTables = new HashSet<>();


    public TableDataDiversionFunction(OutputTag<JSONObject> tag) {
        this.tag = tag;
    }

//    private Connection mysqlConnection;
//    private Connection hbaseConnection;

    /**
     * 初始化获取mysql中的配置信息
     * 并定时调用跟新缓存中的数据
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
//
//        mysqlConnection = MySQLUtil.getConnection(mysqlConnection);
//        hbaseConnection = MyPhoenixUtil.getConnection(hbaseConnection);

        //初始化配置表信息
        refreshMeta();



        //定时调度
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        }, 5000, 5000);

    }

    /**
     * 查询的具体实现
     * 过滤处需要写入hbase中的表信息并在hbase中创建相应的表
     */
    private void refreshMeta() {

//        List<TableProcess> tableList = MySQLUtil.queryList(mysqlConnection,"SELECT * FROM table_process", TableProcess.class, true);
        List<TableProcess> tableList = MySQLUtil.queryList("SELECT * FROM table_process", TableProcess.class, true);

        Optional.ofNullable(tableList).orElseThrow(() -> new RuntimeException("数据库中的表为空！！！"));

//        tableList.stream()
//                .map(table -> {
//                    String key = table.getSourceTable() + "" + table.getOperateType();
//                    tableProcessMap.put(key, table);
//                    return table;
//                }).filter(table -> table.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)&&table.getOperateType().equals("insert"))
//                .map(table -> {
//
//                    //判断该表是否需要在hbase中创建
//                    if(existsTables.add(table.getSinkTable())){
//                        createTableToHbase(table);
//                    }
//
//                    return table;
//                });

        for (int i = 0; i < tableList.size(); i++) {
            TableProcess table = tableList.get(i);
            tableProcessMap.put(table.getSourceTable() + ":" + table.getOperateType(), table);
            if(TableProcess.SINK_TYPE_HBASE.equals(table.getSinkType())){
                if(existsTables.add(table.getSinkTable())){
                    createTableToHbase(table);
                }
            }
//            if (existsTables.add(table.getSinkTable()) && TableProcess.SINK_TYPE_HBASE.equals(table.getSinkType())) {
//                createTableToHbase(table);
//            }
        }


    }

    /**
     * 根据表信息拼接建表sql并在hbase中创建相应的表
     * @param table
     */
    private void createTableToHbase(TableProcess table) {

        String pk = Optional.ofNullable(table.getSinkPk()).orElseGet(() ->"id");
        String ext = Optional.ofNullable(table.getSinkExtend()).orElseGet(() -> "");

        //create table if not exist REALTIME_MALL.xxx(xxx varchar primary key,xxx varchar...)
        StringBuilder sql = new StringBuilder();

        sql.append("create table if not exists "+ MallConfig.HABSE_SCHEMA+"."+table.getSinkTable()+"(");

        String[] columnNames = table.getSinkColumns().split(",");

        for (int i = 0; i < columnNames.length; i++) {
            String column = columnNames[i];

            if(pk.equals(column)){
                sql.append(column).append(" varchar primary key");
            }else{
                sql.append("info.").append(column).append(" varchar ");
            }

            if (i < columnNames.length - 1) {
                sql.append(",");
            }

        }

        sql.append(")").append(ext);

        System.out.println("hbase中的建表语句："+sql);

        //创建表
//        if(sql != null && sql.length() > 0){
//            ThreadPoolUtil.getInstance().submit(() -> {
                MyPhoenixUtil.executeSql(sql.toString());
//            });
//        }




    }

    /**
     *
     * 分流在数据中添加需要发往下一级的信息
     * 并过滤掉相关多余的字段信息
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

        /**
         *
         * 分流
         *  1 kafka 中的数据分到主流
         *  2 habse 中的数据分到侧输出流并过滤掉多余字段
         *
         */

        String type = value.getString("type");
        if ("bootstrap-insert".equals(type)) {
            type = "insert";
            value.put("type", type);
        }

        TableProcess tableProcess = tableProcessMap.get(value.getString("table") + ":" + type);

        if (tableProcess != null) {
            //打上标签
            value.put("sink_table", tableProcess.getSinkTable());

            //判断是不是要存入hbase中的数据，如果是则过滤字段，Kafka中的数据是无需过滤字段
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //hbase 中的数据需要过滤字段
                filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());
            }

        } else
            System.out.println("NO this source table:" + value.getString("table")+":" +type + " in MySQL");


        //分流
        if (tableProcess != null && TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            //hbase
            ctx.output(tag, value);
        } else if (tableProcess != null && TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
            //kafka
            out.collect(value);
        }


    }

    @Override
    public void onTimer(long l, OnTimerContext onTimerContext, Collector<JSONObject> collector) throws Exception {

    }

    /**
     * 过滤掉多余字段
     * @param value
     * @param sinkColumns
     */
    private void filterColumn(JSONObject value, String sinkColumns) {

        if (sinkColumns != null && sinkColumns.length() > 0) {
            List<String> columnList = Arrays.asList(sinkColumns.split(","));

            Iterator<Map.Entry<String, Object>> entryIterator = value.entrySet().iterator();

            while (entryIterator.hasNext()) {
                Map.Entry<String, Object> entry = entryIterator.next();
                if (!columnList.contains(entry.getKey())) {
                    entryIterator.remove();
                }
            }
        }

    }


}
