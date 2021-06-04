package com.at.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zero
 * @create 2021-05-21 11:11
 */
public class Test {

    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//
//
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/testApp1"));
//
//
//        env.addSource(new TestSource())
//                .map(new MapFunction<String, String>() {
//                    @Override
//                    public String map(String s) throws Exception {
//                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
//                        return sdf.format(new Date(Long.parseLong(s)));
//                    }
//                })
//                .print(">>>>");
//
//
//        env.execute();


        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date parse = format.parse("2021-06-03 23:00:00");
        System.out.println(parse.getTime());


    }



}
