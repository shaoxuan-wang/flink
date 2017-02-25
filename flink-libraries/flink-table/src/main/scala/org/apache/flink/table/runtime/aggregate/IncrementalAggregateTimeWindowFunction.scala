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

import java.lang.Iterable

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.types.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Computes the final aggregate value from incrementally computed aggreagtes.
  *
  * @param finalRowArity  The arity of the final output row.
  * @param windowStartPos the start position of window
  * @param windowEndPos the end position of window
  */
class IncrementalAggregateTimeWindowFunction(
    private val finalRowArity: Int,
    private val windowStartPos: Option[Int],
    private val windowEndPos: Option[Int])
  extends IncrementalAggregateWindowFunction[TimeWindow](
    finalRowArity) {

  private var collector: TimeWindowPropertyCollector = _

  override def open(parameters: Configuration): Unit = {
    collector = new TimeWindowPropertyCollector(windowStartPos, windowEndPos)
    super.open(parameters)
  }

  override def apply(
    key: Tuple,
    window: TimeWindow,
    records: Iterable[Row],
    out: Collector[Row]): Unit = {

    // set collector and window
    collector.wrappedCollector = out
    collector.windowStart = window.getStart
    collector.windowEnd = window.getEnd

    super.apply(key, window, records, collector)
  }
}
