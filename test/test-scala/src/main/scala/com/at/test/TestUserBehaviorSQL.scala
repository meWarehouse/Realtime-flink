package com.at.test

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._


/**
 * @author zero
 * @create 2021-06-26 15:09
 */
object TestUserBehaviorSQL {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()

    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)


    tEnv.executeSql(
      """
        |CREATE TABLE USER_BEHAVIOR (
        |  `userId` BIGINT,
        |  `itemId` BIGINT,
        |  `categoryId` BIGINT,
        |  `behaviour` STRING,
        |  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
        |  WATERMARK FOR ts AS ts-INTERVAL '5' SECOND
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'flink-scala-test-topic',
        |  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop:104:9092',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'timestamp',
        |  'scan.startup.timestamp-millis' = '1624690800000',
        |  'format' = 'json'
        |)
        |
        |""".stripMargin)



//      tEnv.executeSql(
//        """
//          |select * from USER_BEHAVIOR
//          |""".stripMargin).print()


    tEnv.executeSql(
      """
        |select
        |	*
        |from
        |(
        |	select
        |		*,
        |		row_number() over(partition by windowEnd order by itemCount desc) as row_num
        |	from
        |	(
        |		select
        |			itemId,
        |			SUM(itemId) as itemCount,
        |			HOP_END(ts,INTERVAL '5' MINUTE,INTERVAL '1' HOUR) AS windowEnd
        |		from USER_BEHAVIOR
        |		WHERE behaviour='pv'
        |		GROUP BY itemId,HOP(ts,INTERVAL '5' MINUTE,INTERVAL '1' HOUR)
        |	)t
        |)
        |where row_num <= 3
        |""".stripMargin).print()


    env.execute()

  }


}
