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
package org.apache.flink.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.stream.utils.{StreamITCase, StreamTestData}
import org.apache.flink.api.table.utils.FirstUDAF
import org.apache.flink.api.table._
import org.apache.flink.api.table._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.utils._
import org.apache.flink.api.table.utils.TableTestUtil._
import org.apache.flink.api.table.expressions.utils._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Assert._
import org.junit.Test

//import org.apache.flink.api.scala.stream.table.GroupWindowITCase.TimestampWithEqualWatermark
//import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
//import org.apache.flink.streaming.api.watermark.Watermark

import scala.collection.mutable

/**
  * Created by wshaoxuan on 15/12/2016.
  */
class UserDefinedAggFunctionTest extends StreamingMultipleProgramsTestBase {

//  def getSmall3TupleDataStream(env: StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {
//    val data = new mutable.MutableList[(Int, Long, String)]
//    data.+=((1, 1L, "Hi"))
//    data.+=((2, 2L, "Hello"))
//    data.+=((3, 2L, "Hello world"))
//    env.fromCollection(data)
//  }

  @Test
  def testGroupedUserDefinedAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val first = new FirstUDAF[String]

    val t = StreamTestData.getSmall3TupleDataStream(env)
            .toTable(tEnv, 'a, 'b, 'c)
            .groupBy('b)
            .window(Slide over 2.rows every 1.rows)
            .select('b, first('c))
            //.select('b, 'c.count)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("1,1", "2,1", "2,2")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
