package com.at.retime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.at.retime.bean.TableProcess;
import com.at.retime.common.Constant;
import com.at.retime.common.MallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @create 2021-10-14
 */
public class CDCTableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {


    private Connection conn = null;
    //定义一个侧输出流标记
    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public CDCTableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //初始化Phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(MallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //取出状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        String table = value.getString("table"); //table -> flink-mysql-01
        String type = value.getString("type");
        JSONObject data = value.getJSONObject("data");

        if ("bootstrap-insert".equals(type)) {
            type = "insert";
            value.put("type", type);
        }

        //从状态中获取配置信息
        String key = table + ":" + type;
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
//            System.out.println("sink_table:" + tableProcess.getSinkType());

            value.put("sink_table", tableProcess.getSinkType());
            if (StringUtils.isNotEmpty(tableProcess.getSinkColumns())) {
                //过滤出需要的列
                filterColumn(data, tableProcess.getSinkColumns());
            }



            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                ctx.output(outputTag, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkTable())) {
                out.collect(value);
            }


        } else {
            System.out.println("NO this Key in TableProce: " + key);
        }



    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fieldArr = sinkColumns.split(",");
        //将数据转换为List集合，方便后面通过判断是否包含key
        List<String> fieldList = Arrays.asList(fieldArr);

        //获取json对象的封装的键值对集合 获取迭代器对象   因为对集合进行遍历的时候，需要使用迭代器进行删除
        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        for (; iterator.hasNext(); ) {
            Map.Entry<String, Object> entry = iterator.next();
            //如果sink_columns中不包含  遍历出的属性    将其删除
            if (!fieldList.contains(entry.getKey())) {
                iterator.remove();
            }
        }


    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //将单条数据转换为JSON对象
        JSONObject jsonObject = JSON.parseObject(value);

        //获取其中的data
        String data = jsonObject.getString("data");
        //将data转换为TableProcess对象
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //获取源表表名
        String sourceTable = tableProcess.getSourceTable();
        //获取操作类型
        String operateType = tableProcess.getOperateType();
        //输出类型      hbase|kafka
        String sinkType = tableProcess.getSinkType();
        //输出目的地表名或者主题名
        String sinkTable = tableProcess.getSinkTable();
        //输出字段
        String sinkColumns = tableProcess.getSinkColumns();
        //表的主键
        String sinkPk = tableProcess.getSinkPk();
        //建表扩展语句
        String sinkExtend = tableProcess.getSinkExtend();


        //拼接保存配置的key
        String key = sourceTable + ":" + operateType;

        //如果是维度数据则需要通过 phoenix 创建表
        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
        }

        //获取状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //将数据进行广播
        broadcastState.put(key, tableProcess);


    }

    //拼接SQL，通过Phoenix创建表
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        sinkPk = Optional.ofNullable(sinkPk).orElseGet(() -> "id");
        sinkExtend = Optional.ofNullable(sinkExtend).orElseGet(() -> "");

        String[] fieldsArr = sinkColumns.split(",");
        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " + Constant.HBASE_SCHEMA + "." + sinkTable + "(");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            //判断当前字段是否为主键字段
            if (sinkPk.equals(field)) {
                createSql.append(field).append(" varchar primary key ");
            } else {
                createSql.append("info.").append(field).append(" varchar ");
            }
            //如果不是最后一个字段  拼接逗号
            if (i < fieldsArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(sinkExtend);
        System.out.println("Phoenix的建表语句：" + createSql);

        //执行SQL语句，通过Phoenix建表
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(createSql.toString());
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("在Phoenix中创建维度表失败");
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
