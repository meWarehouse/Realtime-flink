package com.at.test

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.at.bean.UserBehavior
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * @author zero
 * @create 2021-06-26 15:05
 */
object TestUserBehaviorWriteToKafka {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val sourceDS: DataStream[String] = env.readTextFile("D:\\workspace\\workspace2021\\bigdata\\实时-flink\\Realtime-flink\\real-time-test\\src\\main\\resources\\UserBehavior.csv")

    val stream: DataStream[String] = sourceDS.map(line => {
      val files: Array[String] = line.split(",")
      //case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behaviour: String, timestamp: Long)
      val behavior = UserBehavior(files(0).toLong, files(1).toLong, files(2).toInt, files(3), files(4).toLong)

      /*
          Error:(35, 12) ambiguous reference to overloaded definition,
            使用 fastjson 时 scala 无法发现 toJSONString 的可变长参数(scala中不建议使用变长参数)
            解决：
              SerializerFeature 中任选一个参数 如果是 样列类需要在属性上加上 @BeanProperty 注解让其有有getter和setter方法
       */
      val res: String = JSON.toJSONString(behavior,SerializerFeature.EMPTY:_*)
      res

    })

    val kafkaSink: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](
      "hadoop102:9092,hadoop103:9092,hadoop103:9092",
      "flink-scala-test-topic",
      new SimpleStringSchema()
    )

    stream.addSink(kafkaSink);


    env.execute();


  }

}
