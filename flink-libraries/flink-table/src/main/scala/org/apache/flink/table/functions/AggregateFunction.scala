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
package org.apache.flink.table.functions

/**
  * Base class for User-Defined Aggregates.
  */
abstract class AggregateFunction[T] extends UserDefinedFunction {
  /**
    * Create and init the Accumulator for this [[AggregateFunction]].
    */
  def createAccumulator(): Accumulator

  /**
    * Called at every time when a aggregation result should be materialized.
    * The returned value could be either a speculative result (periodically
    * emits as data arrive) or the final result of the aggregation (completely
    * remove the state of the aggregation)
    */
  def getValue(accumulator: Accumulator): T

  /**
    * Process the input values and update the provided accumulator instance.
    */
  def accumulate(accumulator: Accumulator, input: Any): Unit

  /**
    * Merge two accumulator instances into one accumulator instance.
    */
  def merge(a: Accumulator, b: Accumulator): Accumulator

  /**
    * A flag indicating whether this [[AggregateFunction]] supports partial
    * merge or not.
    * TODO: Alternatively, we can check if a merge function is provided to
    * decide whether an aggregate function supports partial merge or not.
    * But this needs the refactoring the AggregateFunction interface with
    * code generation. We will remove this interface once codeGen for UDAGG
    * is completed (FLINK-5813).
    */
  def supportPartialMerge: Boolean
}

/**
  * Base interface for aggregate Accumulator.
  * TODO: The accumulator and return types of the future proposed UDAGG
  * functions can be dynamically provided by the users. But this needs the
  * refactoring the AggregateFunction interface with code generation. We will
  * remove this interface once codeGen for UDAGG is completed (FLINK-5813).
  */
trait Accumulator
