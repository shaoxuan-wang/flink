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

import org.apache.flink.api.common.functions.Function
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
  * Base class for Aggregate Helper Function.
  */
abstract class AggregateHelper extends Function {

  /**
    * Calculate the results from accumulators, and set the results to the output
    *
    * @param accumulators the accumulators (saved in a row) which contains the current
    *                     aggregated results
    * @param aggregates   the list of all
    *                     [[org.apache.flink.table.functions.AggregateFunction]]
    *                     used for this aggregation
    * @param rowOffset    offset of the position (in the output row) where the aggregate results
    *                     starts
    * @param output       output results collected in a row
    */
  def setOutput(
      accumulators: Row,
      aggregates: Array[AggregateFunction[_]],
      rowOffset: Int,
      output: Row)

  /**
    * Accumulate the input values to the accumulators, in the meanwhile calculate the results and
    * set the results to the output
    *
    * @param accumulators the accumulators (saved in a row) which contains the current
    *                     aggregated results
    * @param aggregates   the list of all
    *                     [[org.apache.flink.table.functions.AggregateFunction]]
    *                     used for this aggregation
    * @param aggFields    the position (in the input Row) of the input value for each aggregate
    * @param rowOffset    offset of the position (in the output row) where the aggregate results
    *                     starts
    * @param input        input values bundled in a row
    * @param output       output results collected in a row
    */
  def accumulateAndSetOutput(
      accumulators: Row,
      aggregates: Array[AggregateFunction[_]],
      aggFields: Array[Array[Int]],
      rowOffset: Int,
      input: Row,
      output: Row)

  /**
    * Accumulate the input values to the accumulators
    *
    * @param accumulators the accumulators (saved in a row) which contains the current
    *                     aggregated results
    * @param aggregates   the list of all
    *                     [[org.apache.flink.table.functions.AggregateFunction]]
    *                     used for this aggregation
    * @param aggFields    the position (in the input Row) of the input value for each aggregate
    * @param input        input values bundled in a row
    */
  def accumulate(
      accumulators: Row,
      aggregates: Array[AggregateFunction[_]],
      aggFields: Array[Array[Int]],
      input: Row)

  /**
    * Retract the input values from the accumulators
    *
    * @param accumulators the accumulators (saved in a row) which contains the current
    *                     aggregated results
    * @param aggregates   the list of all
    *                     [[org.apache.flink.table.functions.AggregateFunction]]
    *                     used for this aggregation
    * @param aggFields    the position (in the input Row) of the input value for each aggregate
    * @param input        input values bundled in a row
    */
  def retract(
      accumulators: Row,
      aggregates: Array[AggregateFunction[_]],
      aggFields: Array[Array[Int]],
      input: Row)

}
