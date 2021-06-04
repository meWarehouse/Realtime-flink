package com.at.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.at.app.func.DimSinkFunction;
import com.at.app.func.TableDataDiversionFunction;
import com.at.bean.TableProcess;
import com.at.utils.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author zero
 * @create 2021-05-12 15:29
 */
public class OdsToDwdDbCDCApp {

    /**
     * TODO
     *  1 从kafka中读取ODS层的业务数据(ods_base_db_m)
     *  2 ETL 数据清洗，将脏数据清除
     *       例如table字段为空的，data字段为空并且长度为0的
     *  3 动态分流
     *      根据定义的规则将维度表写入hbase 事实表写回kafka形成dwd层数据
     */

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/odstodwddbApp"));

//        重启策略
//         如果说没有开启重启Checkpoint，那么重启策略就是noRestart
//         如果说没有开Checkpoint，那么重启策略会尝试自动帮你进行重启   重启Integer.MaxValue
        env.setRestartStrategy(RestartStrategies.noRestart());


        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {};


        SingleOutputStreamOperator<JSONObject> filterDS = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db_m", "base_db_app_group"))
                //转为json格式
                .map(JSONObject::parseObject)
                //过滤
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getString("table") != null
                                && jsonObject.getJSONObject("data") != null
                                && jsonObject.getJSONObject("data").size() > 0;
                    }
                });


        // ============= 使用 cdc 改进
        Properties props = new Properties();
        props.setProperty("debezium.snapshot.mode","initial");


        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("table_process")// monitor all tables under inventory database
                .tableList("table_process.test_flinkcdc")
                .username("root")
                .password("root")
                .debeziumProperties(props)
                .deserializer(new MySchema()) // converts SourceRecord to String
                .build();
        //读取mysql中的数据
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //将配置信息流作为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = streamSource.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, String> connect = filterDS.connect(broadcastStream);

        SingleOutputStreamOperator<JSONObject> processDS = connect.process(new BroadcastProcessFunction<JSONObject, String, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

                ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                String key = value.getString("table") + value.getString("type");

                TableProcess process = broadcastState.get(key);

                if (process != null) {

                    value.put("sink_table", process.getSinkTable());
                    String sinkTable = process.getSinkTable();
                    if ("kafka".equals(sinkTable)) {
                        out.collect(value);
                    } else if ("hbase".equals(sinkTable)) {
                        ctx.output(hbaseTag, value);
                    }

                } else {
                    System.out.println("没有匹配到 " + key + "的信息");
                }


            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                //获取状态
                BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                JSONObject jsonObject = JSON.parseObject(value);

                String key = jsonObject.getString("table") + ":" + jsonObject.getString("type");

                TableProcess data = JSON.parseObject(jsonObject.getString("data"), TableProcess.class);

                broadcastState.put(key, data);


            }
        });



        processDS.print("kafka>>>>");
        processDS.getSideOutput(hbaseTag).print("hbase>>>");

        //hbase>>>> {"database":"rt_mall_flink","xid":37230,"data":{"tm_name":"14","id":14},"commit":true,"sink_table":"dim_base_trademark","type":"insert","table":"base_trademark","ts":1621058838}
        //kafka>>>>> {"database":"rt_mall_flink","xid":37317,"data":{"tm_name":"15","logo_url":"15","id":15},"commit":true,"sink_table":"dim_base_trademark","type":"insert","table":"base_trademark","ts":1621058872}

        //将数据分别写入hbase与kafka


        env.execute();


    }



    public static class MySchema implements DebeziumDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

            //获取主题信息,包含着数据库和表名 mysql_binlog_source.gmall-flink-200821.z_user_info
            String topic = sourceRecord.topic();
            String[] arr = topic.split("\\.");
            String db = arr[1];
            String tableName = arr[2];

            //获取操作类型 READ DELETE UPDATE CREATE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            //获取值信息并转换为Struct类型
            Struct value = (Struct) sourceRecord.value();

            //获取变化后的数据
            Struct after = value.getStruct("after");

            //创建JSON对象用于存储数据信息
            JSONObject data = new JSONObject();
            if(after != null){
                for (Field field : after.schema().fields()) {
                    Object o = after.get(field);
                    data.put(field.name(), o);
                }
            }


            //创建JSON对象用于封装最终返回值数据信息
            JSONObject result = new JSONObject();
            result.put("operation", operation.toString().toLowerCase());
            result.put("data", data);
            result.put("database", db);
            result.put("table", tableName);

            //发送数据至下游
            collector.collect(result.toJSONString());


        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

}
