package com.at.retime.app.dwd;

import com.alibaba.fastjson.JSONObject;

import com.at.retime.app.func.DimSinkFunction;
import com.at.retime.app.func.TableDataDiversionFunction;
import com.at.retime.bean.TableProcess;
import com.at.retime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author zero
 * @create 2021-05-12 15:29
 */
public class OdsToDwdDbApp {

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


        SingleOutputStreamOperator<JSONObject> divDS = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db_m", "base_db_app_group"))
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
                })
                //动态分流
                .process(new TableDataDiversionFunction(hbaseTag));


        divDS.print("kafka>>>>");
        divDS.getSideOutput(hbaseTag).print("hbase>>>");

        //hbase>>>> {"database":"rt_mall_flink","xid":37230,"data":{"tm_name":"14","id":14},"commit":true,"sink_table":"dim_base_trademark","type":"insert","table":"base_trademark","ts":1621058838}
        //kafka>>>>> {"database":"rt_mall_flink","xid":37317,"data":{"tm_name":"15","logo_url":"15","id":15},"commit":true,"sink_table":"dim_base_trademark","type":"insert","table":"base_trademark","ts":1621058872}

        //将数据分别写入hbase与kafka

        divDS.getSideOutput(hbaseTag).addSink(new DimSinkFunction());

        divDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
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


        env.execute();


    }

}
