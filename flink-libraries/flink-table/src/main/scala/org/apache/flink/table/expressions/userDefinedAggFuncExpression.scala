package org.apache.flink.table.expressions
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.runtime.aggregate.Aggregate
/**
  * Created by wshaoxuan on 22/12/2016.
  */

case class UDAGGExpressionBuilder[T: TypeInformation](udaf: Aggregate[T]) {
  def apply(expression: Expression): UDAFExpr[T] =
    UDAFExpr[T](udaf, expression)
}