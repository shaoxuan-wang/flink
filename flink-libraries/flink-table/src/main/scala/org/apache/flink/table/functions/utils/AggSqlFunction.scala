package org.apache.flink.table.functions.utils

import java.lang.reflect.Method

import org.apache.calcite.sql.{SqlAggFunction, SqlFunctionCategory, SqlKind}
import org.apache.calcite.sql.`type`._
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.AggregateFunction

/**
  * Created by wshaoxuan on 11/03/2017.
  */
class AggSqlFunction(
    val name: String,
    val udaf: AggregateFunction[_],
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
    //          with flinkSqlFunction{
{
  // will never not be called
  //  override def getMethod: Method = ???

  //    override def getUDF: AbstractUDF = {
  //      udaf
  //    }
  def getFunction: AggregateFunction[_] = udaf
}

// static helper functions
object AggSqlFunctionObj {

  def create(name: String,
      udaf: AggregateFunction[_],
      returnType: TypeInformation[_],
      operandType: TypeInformation[_]): AggSqlFunction = {
    new AggSqlFunction(
      name,
      udaf,
      typeInfoToTypeInference(returnType),
      null,
      typeInfoToTypeChecker(Seq(operandType))
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

  private def typeInfoToTypeChecker(typeInfos: Seq[TypeInformation[_]]): SqlSingleOperandTypeChecker = {
    val types: Seq[SqlTypeFamily] = typeInfos.map({
      case o if o.isInstanceOf[NumericTypeInfo[_]] => SqlTypeFamily.NUMERIC
      case BasicTypeInfo.STRING_TYPE_INFO => SqlTypeFamily.STRING
      case BasicTypeInfo.DATE_TYPE_INFO | SqlTimeTypeInfo.DATE => SqlTypeFamily.DATETIME
      case _ => SqlTypeFamily.ANY
    })
    OperandTypes.family(scala.collection.JavaConversions.seqAsJavaList(types))
  }

  private def typeInfoToTypeInference(typeInfo: TypeInformation[_]): SqlReturnTypeInference = {
    typeInfo match {
      case BasicTypeInfo.LONG_TYPE_INFO => ReturnTypes.BIGINT_NULLABLE
      case BasicTypeInfo.STRING_TYPE_INFO => ReturnTypes.VARCHAR_2000
      case ti if ti.isInstanceOf[IntegerTypeInfo[_]] => ReturnTypes.INTEGER_NULLABLE
      case ti if ti.isInstanceOf[FractionalTypeInfo[_]] => ReturnTypes.DOUBLE_NULLABLE
      case BasicTypeInfo.DATE_TYPE_INFO => ReturnTypes.DATE
      case _ => throw new TableException(s"failed to infer type from $typeInfo")
    }
  }

//  private def typeInfoToSqlFunctionCategory(typeInfo: TypeInformation[_]) : SqlFunctionCategory = {
//    typeInfo match {
//      case ti if ti.isInstanceOf[NumericTypeInfo[_]] => SqlFunctionCategory.NUMERIC
//      case BasicTypeInfo.STRING_TYPE_INFO => SqlFunctionCategory.STRING
//      case BasicTypeInfo.DATE_TYPE_INFO | SqlTimeTypeInfo.DATE => SqlFunctionCategory.TIMEDATE
//      case _ => SqlFunctionCategory.SYSTEM
//    }
//  }
}
