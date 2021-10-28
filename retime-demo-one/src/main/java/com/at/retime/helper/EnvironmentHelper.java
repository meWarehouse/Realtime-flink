package com.at.retime.helper;

import com.at.retime.common.Constant;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2021-10-28
 */
public class EnvironmentHelper {

    public static StreamExecutionEnvironment getStreamExecutionEnvironment(ParameterTool parameterTool){
        StreamExecutionEnvironment env = null;
        if(parameterTool.get(Constant.RUN_TYPE).equals(Constant.ENV_PRODUCTION_TYPE)){
            env= StreamExecutionEnvironment.getExecutionEnvironment();
        }else {
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        }

        return env;

    }

}
