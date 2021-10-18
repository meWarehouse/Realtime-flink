package com.at.test

import com.alibaba.fastjson.JSON
import com.at.bean.{ItemViewCount, UserBehaviorConsumer}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.collection.mutable.ListBuffer

/**
 * @author zero
 * @create 2021-06-26 15:09
 */
object TestUserBehaviorReadFromKafka {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_DOC, "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_DOC, "org.apache.kafka.common.serialization.StringDeserializer")


    val kafkaSource: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("flink-scala-test-topic", new SimpleStringSchema(), props)


    val sourceStream: DataStream[UserBehaviorConsumer] = env.addSource(kafkaSource)
      .map(line => {
        val consumer = JSON.parseObject(line, classOf[UserBehaviorConsumer])

        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        val date: String = format.format(new Date(consumer.timestamp))

        consumer.date = date;

        consumer
      })


    val aggitem: DataStream[ItemViewCount] = sourceStream
      .filter(_.behaviour.equals("pv"))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness[UserBehaviorConsumer](java.time.Duration.ofSeconds(2L)).withTimestampAssigner(
          new SerializableTimestampAssigner[UserBehaviorConsumer] {
            override def extractTimestamp(element: UserBehaviorConsumer, recordTimestamp: Long): Long = element.timestamp * 1000L
          }
        )
      )
      .keyBy(_.itemId)
      .window(TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
      .aggregate(new MyAcc, new MyProcess)

    aggitem
      .keyBy(_.windowEnd)
      .process(new TopN(3))
      .print()



    env.execute();

  }

  class TopN(n:Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{

    lazy val itemState: ListState[ItemViewCount] = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("item-list", Types.of[ItemViewCount])
    )

    override def processElement(ele: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(ele)
      context.timerService().registerEventTimeTimer(ele.windowEnd + 10L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      val buffer: ListBuffer[ItemViewCount] = ListBuffer()

      import scala.collection.JavaConversions._
      for(ele <- itemState.get()){
        buffer.append(ele)
      }

      itemState.clear();

      val sortItems: ListBuffer[ItemViewCount] = buffer.sortBy(-_.count).take(n)

      // 打印结果
      val result = new StringBuilder

      result
        .append("======================================\n")
        .append("窗口结束时间是：")
        // 还原窗口结束时间，所以要减去100ms
        .append(new Timestamp(timestamp - 100L))
        .append("\n")
      for (i <- sortItems.indices) {
        val currItem = sortItems(i)
        result
          .append("第")
          .append(i + 1)
          .append("名的商品ID是：")
          .append(currItem.itemId)
          .append("，浏览量是：")
          .append(currItem.count)
          .append("\n")
      }
      result
        .append("======================================\n\n\n")




      out.collect(result.toString())


    }

  }

  class MyAcc extends AggregateFunction[UserBehaviorConsumer,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehaviorConsumer, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class MyProcess extends ProcessWindowFunction[Long,ItemViewCount,Long,TimeWindow]{
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key,context.window.getEnd,elements.head))
    }
  }

}
