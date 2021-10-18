package com.at.app.dwd

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.{KeyedStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.concurrent.TimeUnit

/**
 * @author zero
 * @create 2021-06-26 22:02
 */
object OdsToDwdLogApp {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()

    env.setParallelism(4);

    val stateBackend = new RocksDBStateBackend("hdfs://hadoop102:8020/mall/checkpoint/odstodwdlogApp", true)
    env.setStateBackend(stateBackend);

    env.enableCheckpointing(TimeUnit.MINUTES.toMillis(5L), CheckpointingMode.EXACTLY_ONCE)

    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(3L))
    checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(10L))
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "base_log_app_group")
    val kafkaSource: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("ods_base_log", new SimpleStringSchema(), properties)

    //转化
    val jsonStream: SingleOutputStreamOperator[JSONObject] = env.addSource(kafkaSource)
      .map(new MapFunction[String, JSONObject] {
        override def map(value: String): JSONObject = JSON.parseObject(value)
      })

    jsonStream.print()

    //分流
    val keyByStream: KeyedStream[JSONObject, String] = jsonStream.keyBy(
      new KeySelector[JSONObject, String] {
        override def getKey(value: JSONObject): String = value.getJSONObject("common").getString("mid")
      }
    )

    //新老访客识别
    val uvStream: SingleOutputStreamOperator[JSONObject] = keyByStream.map(
      new RichMapFunction[JSONObject, JSONObject] {

        //
        lazy val uvState: ValueState[String] = getRuntimeContext.getState(
          new ValueStateDescriptor[String]("uv", classOf[String])
        )
        lazy val dfs: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

        override def map(value: JSONObject): JSONObject = {

          val isNew: String = value.getJSONObject("common").getString("is_new")

          if ("1".equals(isNew)) {
            //可能是
            val isUv = uvState.value()

            if (StringUtils.isNotEmpty(isUv)) {
              val date = dfs.format(new Date(value.getLongValue("ts")))

              if (!date.equals(isUv))
                value.getJSONObject("common").put("is_new", 0)

            }

          }
          value
        }
      }
    )

    //    val startTag: OutputTag[String] = OutputTag[String]("start-tag")
    //    val diaplayTag: OutputTag[String] = OutputTag[String]("display-tag")

    //        val startTag: SingleOutputStreamOperator[String] = new SingleOutputStreamOperator[String]("start-tag"){}
    //        val diaplayTag = new SingleOutputStreamOperator[String]("display-tag"){}

    val startTag: OutputTag[String] = new OutputTag[String]("start-tag")
    val diaplayTag: OutputTag[String] = OutputTag[String]("display-tag")

    //分流
    val divStream = uvStream.process(
      new ProcessFunction[JSONObject, String] {
        override def processElement(value: JSONObject, context: ProcessFunction[JSONObject, String]#Context, collector: Collector[String]): Unit = {

          val start = value.getString("start")
          val streamStr = value.toString()

          if (StringUtils.isNotEmpty(start)) {
            context.output(startTag, streamStr)
          } else {

            collector.collect(streamStr);

            val array: JSONArray = value.getJSONArray("displays")

            if (array != null && array.size() > 0) {
              for (i <- 0 until (array.size())) {
                val nObject: JSONObject = array.getJSONObject(i)
                nObject.put("page_id", value.getJSONObject("page").getString("page_id"));
                context.output(diaplayTag, nObject.toString())
              }
            }

          }

        }
      }
    )

//    divStream.getSideOutput(startTag).print("start>>>")
//    divStream.print("page>>>")
//    divStream.getSideOutput(diaplayTag).print("display>>>")

    //写回 kafka
    val pagLogSink: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](
      "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "dwd_page_log",
      new SimpleStringSchema()
    )

    val displayLogSink: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](
      "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "dwd_display_log",
      new SimpleStringSchema()
    )

    val startLogSink: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](
      "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "dwd_start_log",
      new SimpleStringSchema()
    )


    divStream.getSideOutput(startTag).addSink(startLogSink)
    divStream.getSideOutput(diaplayTag).addSink(displayLogSink)
    divStream.addSink(pagLogSink)


    env.execute("OdsToDwdLogApp")


  }

}
