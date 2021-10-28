package com.at.retime.helper;

import com.at.retime.common.Constant;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import scala.tools.nsc.doc.model.Package;

/**
 * @create 2021-10-28
 */
public class ParallelismHelper {



    public static void setParallelism(SingleOutputStreamOperator streamOperator, ParameterTool parameterTool, int parallelism){

        if(parameterTool.get(Constant.RUN_TYPE).equals(Constant.ENV_PRODUCTION_TYPE)){
            streamOperator.setParallelism(parallelism);
        }else {
            streamOperator.setParallelism(parameterTool.getInt(Constant.DEFAULT_PARALLELISM));
        }

    }

    public static void setParallelism(DataStreamSink streamOperator, ParameterTool parameterTool, int parallelism){


        if(parameterTool.get(Constant.RUN_TYPE).equals(Constant.ENV_PRODUCTION_TYPE)){
            streamOperator.setParallelism(parallelism);
        }else {
            streamOperator.setParallelism(parameterTool.getInt(Constant.DEFAULT_PARALLELISM));
        }

    }



}
