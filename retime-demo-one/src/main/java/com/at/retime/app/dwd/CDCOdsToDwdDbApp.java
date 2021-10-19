package com.at.retime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;


import com.at.retime.app.func.CDCTableProcessFunction;
import com.at.retime.bean.FlinkKafkaConsumerRecord;
import com.at.retime.bean.TableProcess;

import com.at.retime.common.Constant;
import com.at.retime.common.MysqlDebeziumDeserializationSchema;
import com.at.retime.utils.MyKafkaUtil;
import com.at.retime.utils.ParseArgsUtil;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @create 2021-10-13
 */
public class CDCOdsToDwdDbApp {

    /**
     * 1 从kafka中读取ODS层的业务数据(ods_base_db_m)
     * 2 ETL 数据清洗，将脏数据清除
     * 例如table字段为空的，data字段为空并且长度为0的
     * 3 动态分流
     * 根据定义的规则将维度表写入hbase 事实表写回kafka形成dwd层数据
     */

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParseArgsUtil.init(args, false);

        StreamExecutionEnvironment env = null;

        System.out.println("envType:" + parameterTool.get(Constant.RUN_TYPE));

        if (Constant.ENV_LOCAL_TYPE.equals(parameterTool.get(Constant.RUN_TYPE)) || Constant.ENV_TYPE.equals(parameterTool.get(Constant.RUN_TYPE))) {
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
            env.setParallelism(1);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(4);
        }

//        CheckpointUtil.enableCheckpoint(env, parameterTool);

        String topic = "ods_base_db_m";
        String groupId = "basedbapp_group";

        FlinkKafkaConsumer<FlinkKafkaConsumerRecord> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        kafkaConsumer.setStartFromEarliest();

        SingleOutputStreamOperator<JSONObject> sourceStream = env.addSource(kafkaConsumer)
                .flatMap(new FlatMapFunction<FlinkKafkaConsumerRecord, JSONObject>() {
                    @Override
                    public void flatMap(FlinkKafkaConsumerRecord record, Collector<JSONObject> out) throws Exception {
                        if (record.getMsg() != null && record.getMsg().length > 0) {
                            JSONObject jsonObject = JSON.parseObject(new String(record.getMsg(), "utf-8"));
                            if (StringUtils.isNotEmpty(jsonObject.getString("table"))
                                    && jsonObject.getJSONObject("data") != null
                                    && jsonObject.getJSONObject("data").size() > 3) {
                                out.collect(jsonObject);
                            }
                        }
                    }
                });


        //获取mysql中的维度表配置文件 并将其广播出去
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall_report")
                .tableList("gmall_report.table_process")
                .deserializer(new MysqlDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();



        MapStateDescriptor<String, TableProcess> mapStateDescriptor  = new MapStateDescriptor<>("table-process", String.class, TableProcess.class);
//
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
//        dataStreamSource.print();
        BroadcastStream<String> broadcastStream = dataStreamSource.broadcast(mapStateDescriptor);

        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag"){};
//
        //.连接主流和广播流
        SingleOutputStreamOperator<JSONObject> resStream = sourceStream.connect(broadcastStream)
                //对数据进行分流操作  维度数据放到侧输出流  事实数据放到主流
                .process(new CDCTableProcessFunction(dimTag, mapStateDescriptor));

//        resStream.getSideOutput(dimTag).print("dimTag>>>");
//        resStream.print("main>>>");

        resStream.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("kafka 序列化");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                String topic = jsonObject.getString("sink_table");
                JSONObject data = jsonObject.getJSONObject("data");
                return new ProducerRecord<>(topic,data.toString().getBytes());
            }
        }));

//        resStream.getSideOutput(dimTag).addSink(new HBaseSinkFunction<>())




        env.execute();

    }


}
