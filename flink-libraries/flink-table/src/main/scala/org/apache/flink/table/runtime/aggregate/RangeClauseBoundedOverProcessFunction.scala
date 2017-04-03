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
package org.apache.flink.table.runtime.aggregate

import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{ListTypeInfo, RowTypeInfo}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.codegen.{Compiler, GeneratedFunction}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}
import org.slf4j.LoggerFactory

/**
 * Process Function for RANGE clause event-time bounded OVER window
 *
  * @param GeneratedAggregateHelper Generated aggregate helper function
  * @param aggregates               list of all [[AggregateFunction]] used for this aggregation
  * @param aggFields                position (in the input Row) of the input value for each
  *                                 aggregate
  * @param forwardedFieldCount      count of forwarded fields.
  * @param aggregationStateType     row type info of aggregation
  * @param inputRowType             row type info of input row
  * @param precedingOffset          preceding offset
 */
class RangeClauseBoundedOverProcessFunction(
    GeneratedAggregateHelper: GeneratedFunction[AggregateHelper, Row],
    aggregates: Array[AggregateFunction[_]],
    aggFields: Array[Array[Int]],
    forwardedFieldCount: Int,
    aggregationStateType: RowTypeInfo,
    inputRowType: RowTypeInfo,
    precedingOffset: Long)
  extends ProcessFunction[Row, Row]
    with Compiler[AggregateHelper] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)
  Preconditions.checkNotNull(forwardedFieldCount)
  Preconditions.checkNotNull(aggregationStateType)
  Preconditions.checkNotNull(precedingOffset)

  private var output: Row = _

  // the state which keeps the last triggering timestamp
  private var lastTriggeringTsState: ValueState[Long] = _

  // the state which used to materialize the accumulator for incremental calculation
  private var accumulatorState: ValueState[Row] = _

  // the state which keeps all the data that are not expired.
  // The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
  // the second element of tuple is a list that contains the entire data of all the rows belonging
  // to this time stamp.
  private var dataState: MapState[Long, JList[Row]] = _

  val LOG = LoggerFactory.getLogger(this.getClass)
  private var function: AggregateHelper = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $GeneratedAggregateHelper.name \n\n " +
                s"Code:\n$GeneratedAggregateHelper.code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader,
                        GeneratedAggregateHelper.name,
                        GeneratedAggregateHelper.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()

    output = new Row(forwardedFieldCount + aggregates.length)

    val lastTriggeringTsDescriptor: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("lastTriggeringTsState", classOf[Long])
    lastTriggeringTsState = getRuntimeContext.getState(lastTriggeringTsDescriptor)

    val accumulatorStateDescriptor =
      new ValueStateDescriptor[Row]("accumulatorState", aggregationStateType)
    accumulatorState = getRuntimeContext.getState(accumulatorStateDescriptor)

    val keyTypeInformation: TypeInformation[Long] =
      BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]]
    val valueTypeInformation: TypeInformation[JList[Row]] = new ListTypeInfo[Row](inputRowType)

    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]](
        "dataState",
        keyTypeInformation,
        valueTypeInformation)

    dataState = getRuntimeContext.getMapState(mapStateDescriptor)
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    // triggering timestamp for trigger calculation
    val triggeringTs = ctx.timestamp

    val lastTriggeringTs = lastTriggeringTsState.value

    // check if the data is expired, if not, save the data and register event time timer
    if (triggeringTs > lastTriggeringTs) {
      val data = dataState.get(triggeringTs)
      if (null != data) {
        data.add(input)
        dataState.put(triggeringTs, data)
      } else {
        val data = new JArrayList[Row]
        data.add(input)
        dataState.put(triggeringTs, data)
        // register event time timer
        ctx.timerService.registerEventTimeTimer(triggeringTs)
      }
    }
  }

  override def onTimer(
    timestamp: Long,
    ctx: ProcessFunction[Row, Row]#OnTimerContext,
    out: Collector[Row]): Unit = {
    // gets all window data from state for the calculation
    val inputs: JList[Row] = dataState.get(timestamp)

    if (null != inputs) {

      var accumulators = accumulatorState.value
      var dataListIndex = 0
      var aggregatesIndex = 0

      // initialize when first run or failover recovery per key
      if (null == accumulators) {
        accumulators = new Row(aggregates.length)
        aggregatesIndex = 0
        while (aggregatesIndex < aggregates.length) {
          accumulators.setField(aggregatesIndex, aggregates(aggregatesIndex).createAccumulator())
          aggregatesIndex += 1
        }
      }

      // keep up timestamps of retract data
      val retractTsList: JList[Long] = new JArrayList[Long]

      // do retraction
      val dataTimestampIt = dataState.keys.iterator
      while (dataTimestampIt.hasNext) {
        val dataTs: Long = dataTimestampIt.next()
        val offset = timestamp - dataTs
        if (offset > precedingOffset) {
          val retractDataList = dataState.get(dataTs)
          dataListIndex = 0
          while (dataListIndex < retractDataList.size()) {
            val retractRow = retractDataList.get(dataListIndex)
            function.retract(
              accumulators,
              aggregates,
              aggFields,
              retractRow)
            dataListIndex += 1
          }
          retractTsList.add(dataTs)
        }
      }

      // do accumulation
      dataListIndex = 0
      while (dataListIndex < inputs.size()) {
        val curRow = inputs.get(dataListIndex)
        // accumulate current row
        function.accumulate(
          accumulators,
          aggregates,
          aggFields,
          curRow)
        dataListIndex += 1
      }

      // set aggregate in output row
      function.setOutput(
        accumulators,
        aggregates,
        forwardedFieldCount,
        output)

      // copy forwarded fields to output row and emit output row
      dataListIndex = 0
      while (dataListIndex < inputs.size()) {
        aggregatesIndex = 0
        while (aggregatesIndex < forwardedFieldCount) {
          output.setField(aggregatesIndex, inputs.get(dataListIndex).getField(aggregatesIndex))
          aggregatesIndex += 1
        }
        out.collect(output)
        dataListIndex += 1
      }

      // remove the data that has been retracted
      dataListIndex = 0
      while (dataListIndex < retractTsList.size) {
        dataState.remove(retractTsList.get(dataListIndex))
        dataListIndex += 1
      }

      // update state
      accumulatorState.update(accumulators)
      lastTriggeringTsState.update(timestamp)
    }
  }
}


