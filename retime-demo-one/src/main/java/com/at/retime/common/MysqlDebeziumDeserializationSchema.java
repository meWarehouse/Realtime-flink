package com.at.retime.common;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Locale;

/**
 * @create 2021-10-13
 */
public class MysqlDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {


    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

//        System.out.println("sourceRecord>>>"+sourceRecord);

        JSONObject result = new JSONObject();

        //获取库名和表名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];
        String table = split[2];

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        //获取数据
        Struct struct = (Struct) sourceRecord.value();
        Struct after = struct.getStruct("after");
        JSONObject value = new JSONObject();
        if(after != null){
            Schema schema = after.schema();
            for (Field field : schema.fields()) {
                String fileName = field.name();
                Object fileValue = after.get(fileName);
                value.put(fileName,fileValue);
            }
        }

        //将数据放入JSON对象
        result.put("database",database);
        result.put("table",table);
        String operationType = operation.toString().toLowerCase(Locale.ROOT);
        if("create".equals(operationType)){
            operationType = "insert";
        }
        result.put("type",operationType);
        result.put("data",value);

        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
