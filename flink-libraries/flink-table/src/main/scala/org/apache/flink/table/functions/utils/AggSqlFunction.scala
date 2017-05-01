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

package org.apache.flink.table.functions.utils

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.calcite.schema.{AggregateFunction => CalciteAggFunction}
import org.apache.flink.table.functions.utils.AggSqlFunctionObj.createOperandTypeInference
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getParameterTypes,
getEvalMethodSignature, getAccumulateMethodSignature}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction}

/**
  * say something here......
  */
class AggSqlFunction(
    val name: String,
    val udaf: AggregateFunction[_,_],
    val returnTypeInference: SqlReturnTypeInference,
    val operandTypeInference: SqlOperandTypeInference,
    val operandTypeChecker: SqlOperandTypeChecker,
    typeFactory: FlinkTypeFactory,
    calciteAggFunction: CalciteAggFunction
    )
  extends SqlUserDefinedAggFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    returnTypeInference,
    createOperandTypeInference(udaf, typeFactory),
    operandTypeChecker,
    calciteAggFunction) {
//  extends SqlAggFunction(
//    name,
//    null,
//    SqlKind.OTHER_FUNCTION,
//    returnTypeInference,
//    createOperandTypeInference(udaf, typeFactory),
//    operandTypeChecker,
//    SqlFunctionCategory.USER_DEFINED_FUNCTION,
//    false,
//    false) {
  def getFunction: AggregateFunction[_, _] = udaf
}

// static helper functions
object AggSqlFunctionObj {

  def apply(
      name: String,
      udaf: AggregateFunction[_,_],
      returnType: TypeInformation[_],
      operandType: TypeInformation[_],
      typeFactory: FlinkTypeFactory): AggSqlFunction = {

    val calciteAggFunction= new CalciteAggFunction {
      override def getReturnType(typeFactory: RelDataTypeFactory) = null

      override def getParameters = null
    }

    new AggSqlFunction(
      name,
      udaf,
      typeInfoToTypeInference(returnType, typeFactory),
      null,
      typeInfoToTypeChecker(Seq(operandType)),
      typeFactory,
      calciteAggFunction
    )
  }

  //  private def methodReturnTypeToTypeInfo(method: Method): TypeInformation[_] ={
  //    val ret = method.getReturnType
  //    TypeInformation.of(ret)
  //  }
  //
  //  private def methodParameterTypeToTypeInfo(method: Method) : Seq[TypeInformation[_]] = {
  //    val operands = method.getParameterTypes
  //    operands.map(o => TypeInformation.of(o))
  //  }
  private[flink] def getOperandTypeInfo(callBinding: SqlCallBinding): Seq[TypeInformation[_]] = {
    val operandTypes = for (i <- 0 until callBinding.getOperandCount)
      yield callBinding.getOperandType(i)
    operandTypes.map { operandType =>
      if (operandType.getSqlTypeName == SqlTypeName.NULL) {
        null
      } else {
        FlinkTypeFactory.toTypeInfo(operandType)
      }
    }
  }

  private[flink] def createOperandTypeInference(
      aggregateFunction: AggregateFunction[_, _],
      typeFactory: FlinkTypeFactory)
  : SqlOperandTypeInference = {
    /**
      * Operand type inference based on [[ScalarFunction]] given information.
      */
    new SqlOperandTypeInference {
      override def inferOperandTypes(
          callBinding: SqlCallBinding,
          returnType: RelDataType,
          operandTypes: Array[RelDataType]): Unit = {

        val operandTypeInfo = getOperandTypeInfo(callBinding)

        val foundSignature = getAccumulateMethodSignature(aggregateFunction, operandTypeInfo)
          .getOrElse(throw new ValidationException(s"Operand types of could not be inferred."))

        val inferredTypes = getParameterTypes(aggregateFunction, foundSignature.drop(1))
          .map(typeFactory.createTypeFromTypeInfo)

        for (i <- operandTypes.indices) {
          if (i < inferredTypes.length - 1) {
            operandTypes(i) = inferredTypes(i)
          } else if (null != inferredTypes.last.getComponentType) {
            // last argument is a collection, the array type
            operandTypes(i) = inferredTypes.last.getComponentType
          } else {
            operandTypes(i) = inferredTypes.last
          }
        }
      }
    }
  }

  private def typeInfoToTypeChecker(
      typeInfos: Seq[TypeInformation[_]]): SqlSingleOperandTypeChecker = {
    val types: Seq[SqlTypeFamily] = typeInfos.map({
      case o if o.isInstanceOf[NumericTypeInfo[_]] => SqlTypeFamily.NUMERIC
      case BasicTypeInfo.STRING_TYPE_INFO => SqlTypeFamily.STRING
      case BasicTypeInfo.DATE_TYPE_INFO | SqlTimeTypeInfo.DATE => SqlTypeFamily.DATETIME
      case _ => SqlTypeFamily.ANY
    })
    OperandTypes.family(scala.collection.JavaConversions.seqAsJavaList(types))
  }

  private def typeInfoToTypeInference(
      resultType: TypeInformation[_],
      typeFactory: FlinkTypeFactory): SqlReturnTypeInference = {
    resultType match {
      case BasicTypeInfo.LONG_TYPE_INFO => ReturnTypes.BIGINT_NULLABLE
      case BasicTypeInfo.STRING_TYPE_INFO => ReturnTypes.VARCHAR_2000
      case ti if ti.isInstanceOf[IntegerTypeInfo[_]] => ReturnTypes.INTEGER_NULLABLE
      case ti if ti.isInstanceOf[FractionalTypeInfo[_]] => ReturnTypes.DOUBLE_NULLABLE
      case BasicTypeInfo.DATE_TYPE_INFO => ReturnTypes.DATE
      case _ => createReturnTypeInference(resultType, typeFactory)
      //        createReturnTypeInference(
      //        aggFunction.getClass.getCanonicalName,
      //        aggFunction,
      //        typeFactory)
      //throw new TableException(s"failed to infer type from $typeInfo")
    }
  }

  private[flink] def createReturnTypeInference(
      resultType: TypeInformation[_],
      typeFactory: FlinkTypeFactory)
  : SqlReturnTypeInference = {
    /**
      * Return type inference based on [[ScalarFunction]] given information.
      */
    new SqlReturnTypeInference {
      override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
        //        val parameters = opBinding
        //          .collectOperandTypes()
        //          .asScala
        //          .map { operandType =>
        //            if (operandType.getSqlTypeName == SqlTypeName.NULL) {
        //              null
        //            } else {
        //              FlinkTypeFactory.toTypeInfo(operandType)
        //            }
        //          }
        //        val accType = aggFunction.getClass.getMethod("createAccumulator").
        //          getReturnType
        //        accTypeList(index) = accType.getCanonicalName
        //
        //        val aggReturnType = aggFunction.getClass.getMethod("getValue", accType).
        //          getReturnType
        //        aggReturnTypeList(index) = aggReturnType.getCanonicalName
        //
        //        val foundSignature = getSignature("getValue", aggFunction, parameters)
        //        if (foundSignature.isEmpty) {
        //          throw new ValidationException(
        //            s"Given parameters of function '$name' do not match any signature. \n" +
        //              s"Actual: ${signatureToString(parameters)} \n" +
        //              s"Expected: ${signaturesToString(aggFunction, "getValue")}")
        //        }
        //        val resultType = getResultTypeByName("getValue", aggFunction, foundSignature.get)

        typeFactory.createTypeFromTypeInfo(resultType)
      }
    }
  }
}
