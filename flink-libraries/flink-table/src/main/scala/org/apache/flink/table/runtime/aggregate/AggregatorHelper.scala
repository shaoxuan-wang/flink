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

import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row

class AggregatorHelper {

  def accumulateAndSetOutput(
      accumulators: Row,
      aggregates: Array[AggregateFunction[_]],
      aggFields: Array[Array[Int]],
      rowOffset: Int,
      input: Row,
      output: Row) {

    var i = 0
    while (i < aggregates.length) {
      val index = rowOffset + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      aggregates(i).accumulate(accumulator, input.getField(aggFields(i)(0)))
      output.setField(index, aggregates(i).getValue(accumulator))
      i += 1
    }
  }

  def accumulate(
      accumulators: Row,
      aggregates: Array[AggregateFunction[_]],
      aggFields: Array[Array[Int]],
      input: Row) {

    var i = 0
    while (i < aggregates.length) {
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      aggregates(i).accumulate(accumulator, input.getField(aggFields(i)(0)))
      i += 1
    }
  }

  def setOutput(
      accumulators: Row,
      aggregates: Array[AggregateFunction[_]],
      rowOffset: Int,
      output: Row) {

    var i = 0
    while (i < aggregates.length) {
      val index = rowOffset + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      output.setField(index, aggregates(i).getValue(accumulator))
      i += 1
    }
  }

  def retract(
      accumulators: Row,
      aggregates: Array[AggregateFunction[_]],
      aggFields: Array[Array[Int]],
      retractRow: Row) {

    var i = 0
    while (i < aggregates.length) {
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      aggregates(i).retract(accumulator, retractRow.getField(aggFields(i)(0)))
      i += 1
    }
  }


}
