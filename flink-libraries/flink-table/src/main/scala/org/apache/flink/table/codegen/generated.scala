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

package org.apache.flink.table.codegen

import org.apache.flink.api.common.functions
import org.apache.flink.api.common.functions.Function
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
  * Describes a generated expression.
  *
  * @param resultTerm term to access the result of the expression
  * @param nullTerm boolean term that indicates if expression is null
  * @param code code necessary to produce resultTerm and nullTerm
  * @param resultType type of the resultTerm
  */
case class GeneratedExpression(
    resultTerm: String,
    nullTerm: String,
    code: String,
    resultType: TypeInformation[_])

object GeneratedExpression {
  val ALWAYS_NULL = "true"
  val NEVER_NULL = "false"
  val NO_CODE = ""
}

/**
  * Describes a generated [[functions.Function]]
  *
  * @param name class name of the generated Function.
  * @param returnType the type information of the result type
  * @param code code of the generated Function.
  * @tparam F type of function
  * @tparam T type of function
  */
case class GeneratedFunction[F <: Function, T <: Any](
  name: String,
  returnType: TypeInformation[T],
  code: String)

/**
  * Describes a generated aggregate helper function
  *
  * @param name class name of the generated Function.
  * @param code code of the generated Function.
  */
case class GeneratedAggregationsFunction(
    name: String,
    code: String)

/**
  * Describes the helper flags for the code-gen of aggregate functions
  *
  * @param setResultsWithKeyOffset flag to indicate if the results in output row has an offset
  * @param mergeWithKeyOffset      flag to indicate if the accumulators (for merge) in
  *                                accumulator row has an offset
  * @param accumulateWithKeyOffset flag to indicate if the accumulators (for accumulate) in
  *                                accumulator row has an offset
  */
case class AggCodeGenCtrlParams(
    setResultsWithKeyOffset: Boolean,
    mergeWithKeyOffset: Boolean,
    accumulateWithKeyOffset: Boolean
)

/**
  * Describes a generated [[InputFormat]].
  *
  * @param name class name of the generated input function.
  * @param returnType the type information of the result type
  * @param code code of the generated Function.
  * @tparam F type of function
  * @tparam T type of function
  */
case class GeneratedInput[F <: InputFormat[_, _], T <: Any](
  name: String,
  returnType: TypeInformation[T],
  code: String)

/**
  * Describes a generated [[org.apache.flink.util.Collector]].
  *
  * @param name class name of the generated Collector.
  * @param code code of the generated Collector.
  */
case class GeneratedCollector(name: String, code: String)
