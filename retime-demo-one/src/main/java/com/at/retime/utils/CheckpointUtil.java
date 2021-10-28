package com.at.retime.utils;


import com.at.retime.common.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
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
@Slf4j
public class CheckpointUtil {

//    private static Logger logger =  LoggerFactory.getLogger(CheckpointUtil.class);

    public static void enableCheckpoint(StreamExecutionEnvironment env, ParameterTool parameterTool){
//
        env.setStateBackend(new HashMapStateBackend());
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage(
                /*Constant.CHECKPOINT_DIR*/
                "hdfs://hadoop102:8020/flink/tmp/checkpoint"
        );
        env.enableCheckpointing(TimeUnit.MINUTES.toMinutes(10), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMinutes(15));
        //指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000L));
//        checkpoint 失败不影响程序   sql模式禁止设置Integer.MAX_VALUE
        checkpointConfig.setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        //允许检查点不对齐checkpoint
        checkpointConfig.enableUnalignedCheckpoints();
        //清除检查点
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);




        log.info("checkpoint.dir:%s,checkpoint.interval:%s,checkpoint.timeout:%s",checkpointConfig.getCheckpointStorage(),checkpointConfig.getCheckpointInterval(),checkpointConfig.getCheckpointTimeout());


    }

}
