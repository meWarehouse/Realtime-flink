package com.at.retime.helper;

import com.at.retime.common.Constant;
import org.apache.flink.api.java.functions.SampleInCoordinator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import javax.naming.Name;

/**
 * @create 2021-10-28
 */
public class SlotHelper {

    public static void setSlot(SingleOutputStreamOperator streamOperator, ParameterTool parameterTool,String slotName,String slotGroupName){

        streamOperator.name(slotName);

        if(parameterTool.get(Constant.RUN_TYPE).equals(Constant.ENV_PRODUCTION_TYPE)){
            streamOperator.slotSharingGroup(slotGroupName);
        }else {
            streamOperator.slotSharingGroup(parameterTool.get(Constant.DEFAULT_SLOT_GROUP));
        }


    }

    public static void setSlot(DataStreamSink streamOperator, ParameterTool parameterTool, String slotName, String slotGroupName){

        streamOperator.name(slotName);

        if(parameterTool.get(Constant.RUN_TYPE).equals(Constant.ENV_PRODUCTION_TYPE)){
            streamOperator.slotSharingGroup(slotGroupName);
        }else {
            streamOperator.slotSharingGroup(parameterTool.get(Constant.DEFAULT_SLOT_GROUP));
        }


    }



}
