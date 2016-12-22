package org.apache.flink.table.functions.utils

import org.apache.calcite.sql.`type`.{SqlOperandTypeChecker, SqlOperandTypeInference, SqlReturnTypeInference}
import org.apache.calcite.sql.{SqlAggFunction, SqlFunctionCategory, SqlKind}
import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.table.flinkSqlFunction
//import org.apache.flink.api.table.AbstractUDF
import org.apache.flink.table.typeutils.TypeConverter
import org.apache.flink.table.runtime.aggregate.Aggregate

/**
  * Created by wshaoxuan on 22/12/2016.
  */
class AggSqlFunction(
    val name: String,
    val udaf: Aggregate[_],
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
  def getFunction: Aggregate[_] = udaf
}

// static helper functions
object AggSqlFunctionObj {

  def create(name: String,
      udaf: Aggregate[_],
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
