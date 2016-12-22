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
package org.apache.flink.api.table.functions.utils

import java.lang.reflect.Method

import org.apache.calcite.sql.`type`.{SqlOperandTypeChecker, SqlOperandTypeInference, SqlReturnTypeInference}
import org.apache.calcite.sql.{SqlAggFunction, SqlFunctionCategory, SqlKind}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.table.flinkSqlFunction
import org.apache.flink.api.table.typeutils.TypeConverter
//import org.apache.flink.api.table.AbstractUDF
import org.apache.flink.api.table.functions._

/**
  * Created by wshaoxuan on 16/12/2016.
  */
class AggSqlFunction(
  val name: String,
  val udaf: AggFunction[_],
  val returnTypeInference: SqlReturnTypeInference,
  val operandTypeInference: SqlOperandTypeInference,
  val operandTypeChecker: SqlOperandTypeChecker)
  extends SqlAggFunction(
    name,
    null,
    SqlKind.OTHER_FUNCTION,
    returnTypeInference,
    operandTypeInference,
    operandTypeChecker,
    SqlFunctionCategory.USER_DEFINED_FUNCTION,
    false,
    false)
  with flinkSqlFunction{

    // will never not be called
    override def getMethod: Method = ???

//    override def getUDF: AbstractUDF = {
//      udaf
//    }
    def getFunction: AggFunction[_] = udaf
}

// static helper functions
object AggSqlFunctionObj {

  def create(name: String,
    udaf: AggFunction[_],
    returnType: TypeInformation[_],
    operandType: TypeInformation[_]): AggSqlFunction = {
    new AggSqlFunction(
      name,
      udaf,
      TypeConverter.typeInfoToTypeInference(returnType),
      null,
      TypeConverter.typeInfoToTypeChecker(Seq(operandType))
    )
  }
}
