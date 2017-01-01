/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala.stream.table.GroupWindowITCase.TimestampWithEqualWatermark
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.junit.Assert._
import org.junit.Test
import org.apache.flink.table.api.scala.stream.utils.StreamTestData
//import org.apache.flink.table.runtime.aggregate.firstUDAF

import scala.collection.mutable

/**
  * We only test some aggregations until better testing of constructed DataStream
  * programs is possible.
  */
class AggregationsITCase extends StreamingMultipleProgramsTestBase {

  val data = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hello"),
    (4L, 2, "Hello"),
    (5L, 2, "Hello"),
    (6L, 2, "Hello"),
    (7L, 2, "Hello"),
    (8L, 2, "Hello"),
    (9L, 2, "Hello"),
    (10L, 2, "Hello"),
    (11L, 3, "Hello world"),
    (12L, 3, "Hello world"))


//    def getSmall3TupleDataStream(env: StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {
//      val data = new mutable.MutableList[(Int, Long, String)]
//      data.+=((1, 1L, "Hi"))
//      data.+=((2, 2L, "Hello"))
//      data.+=((3, 2L, "Hello world"))
//      env.fromCollection(data)
//    }
//  @Test
//  def testGroupedUserDefinedAggregate(): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    val tEnv = TableEnvironment.getTableEnvironment(env)
//    StreamITCase.testResults = mutable.MutableList()
//
//    val first = new firstUDAF
//
//    val t = StreamTestData.getSmall3TupleDataStream(env)
//            .toTable(tEnv, 'a, 'b, 'c)
//            .groupBy('b)
//            .window(Slide over 2.rows every 1.rows)
//            .select('b, first('c))
//    //.select('b, 'c.count)
//
//    val results = t.toDataStream[Row]
//    results.addSink(new StreamITCase.StringSink)
//    env.execute()
//
//    val expected = mutable.MutableList("1,1", "2,1", "2,2")
//
//    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
//  }

  @Test
  def mytest(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val data = List(
      (2L, 2, "Hello"),
      (3L, 3, "Hello"),
      (4L, 4, "Hello"),
      (6L, 6, "Hello"),
      (7L, 7, "Hello"),
      (13L, 13, "Hello"),
      (20L, 20, "Hello"),
      (50L, 50, "Hello"))
    val stream = env.fromCollection(data).assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable =
      table
      .groupBy('string)
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .select('string, 'int.count, 'int.max)
      //.select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    for( data <- StreamITCase.testResults.sorted){
      println(data)
    }
    val expected = Seq("Hello,53,4", "Hello,52,7", "Hello,51,13", "Hello,51,20", "Hello,51,50")
    //val expected = Seq("Hello,53", "Hello,52", "Hello,51", "Hello,51", "Hello,51")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Slide over 2.rows every 1.rows)
      .select('string, 'int.count, 'int.avg)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello world,1,3", "Hello world,2,3", "Hello,1,2", "Hello,2,2", "Hi,1,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Session withGap 7.milli on 'rowtime)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello world,1", "Hello world,1", "Hello,2", "Hi,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows)
      .select('int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("2", "2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeTumblingWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .select('string, 'int.count, 'int.avg, 'w.start, 'w.end)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hello world,1,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01",
      "Hello world,1,3,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02",
      "Hello,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hi,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Slide over 10.milli every 5.milli on 'rowtime as 'w)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.start)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hello world,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,1970-01-01 00:00:00.0",
      "Hello world,1,1970-01-01 00:00:00.005,1970-01-01 00:00:00.015,1970-01-01 00:00:00.005",
      "Hello world,1,1970-01-01 00:00:00.01,1970-01-01 00:00:00.02,1970-01-01 00:00:00.01",
      "Hello world,1,1970-01-01 00:00:00.015,1970-01-01 00:00:00.025,1970-01-01 00:00:00.015",
      "Hello,2,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005,1969-12-31 23:59:59.995",
      "Hello,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,1970-01-01 00:00:00.0",
      "Hi,1,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005,1969-12-31 23:59:59.995",
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01,1970-01-01 00:00:00.0")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

}

object GroupWindowITCase {
  class TimestampWithEqualWatermark extends AssignerWithPunctuatedWatermarks[(Long, Int, String)] {

    override def checkAndGetNextWatermark(
        lastElement: (Long, Int, String),
        extractedTimestamp: Long)
      : Watermark = {
      val a = new Watermark(extractedTimestamp)
      println("==> " + a)
      a
//      new Watermark(extractedTimestamp)
    }

    override def extractTimestamp(
        element: (Long, Int, String),
        previousElementTimestamp: Long): Long = {
      element._1
    }
  }
}
