package com.at.retime.test;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @author zero
 * @create 2021-05-21 11:17
 */
public class TestSource extends RichSourceFunction<String> {

    private boolean flag = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        while (flag){
            sourceContext.collect(System.currentTimeMillis()+"");
        }

    }

    @Override
    public void cancel() {

        flag = false;
    }
}
