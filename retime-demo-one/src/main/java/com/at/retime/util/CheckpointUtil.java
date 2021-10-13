package com.at.retime.util;


import com.at.retime.common.Constant;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @create 2021-10-12
 */
public class CheckpointUtil {

    private static Logger logger =  LoggerFactory.getLogger(CheckpointUtil.class);

    public static void enableCheckpoint(StreamExecutionEnvironment env, ParameterTool parameterTool){

        env.setStateBackend(new HashMapStateBackend());
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage(
                new Path(Optional.ofNullable(parameterTool.get(Constant.CHECKPOINT_DIR)).orElseGet(() -> "hdfs://hadoop102:8020/flink/tmp/checkpoint"))
        );
        env.enableCheckpointing(Optional.ofNullable(parameterTool.getLong(Constant.CHECKPOINT_INTERVAL)).orElseGet(() -> 2L)*60*1000L, CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMinutes(Optional.ofNullable(parameterTool.getLong(Constant.CHECKPOINT_TIMEOUT)).orElse(3L))*60*1000L);
        //checkpoint 失败不影响程序
        checkpointConfig.setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        //允许检查点不对齐checkpoint
        checkpointConfig.enableUnalignedCheckpoints();
        //清除检查点
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        System.out.printf("checkpoint.dir:%s,checkpoint.interval:%s,checkpoint.timeout:%s",checkpointConfig.getCheckpointStorage(),checkpointConfig.getCheckpointInterval(),checkpointConfig.getCheckpointTimeout());

    }

}
