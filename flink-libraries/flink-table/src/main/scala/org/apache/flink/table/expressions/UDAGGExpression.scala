package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.functions.AggregateFunction

/**
  * Created by wshaoxuan on 11/03/2017.
  */
case class UDAGGExpression [T: TypeInformation](udaf: AggregateFunction[T]) {
  def apply(expression: Expression): UDAGGExpr[T] =
    UDAGGExpr[T](udaf, expression)
}
