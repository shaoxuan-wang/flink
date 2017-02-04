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
package org.apache.flink.table.expressions

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.AggCall
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.table.typeutils.TypeCheckUtils.isTypeMatch
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}
import org.apache.flink.table.runtime.aggregate.Aggregate
import org.apache.flink.table.functions.utils.AggSqlFunctionObj

abstract sealed class Aggregation extends UnaryExpression {

  override def toString = s"Aggregate($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException("Aggregate cannot be transformed to RexNode")

  /**
    * Convert Aggregate to its counterpart in Calcite, i.e. AggCall
    */
  private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall
}

case class Sum(child: Expression) extends Aggregation {
  override def toString = s"sum($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.SUM, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "sum")
}

case class Min(child: Expression) extends Aggregation {
  override def toString = s"min($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.MIN, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "min")
}

case class Max(child: Expression) extends Aggregation {
  override def toString = s"max($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.MAX, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "max")
}

case class Count(child: Expression) extends Aggregation {
  override def toString = s"count($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.COUNT, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO
}

case class Avg(child: Expression) extends Aggregation {
  override def toString = s"avg($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.AVG, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "avg")
}


case class UDAFExpr[T: TypeInformation](udaf: Aggregate[T], child: Expression) extends Aggregation {

  // Override makeCopy method in TreeNode, to produce vargars properly
  override def makeCopy(newArgs: Array[AnyRef]): this.type = {
    if (newArgs.length < 1) {
      throw new TableException("Invalid constructor params")
    }
    val udtfParam = newArgs.head.asInstanceOf[Aggregate[T]]
    val newArg = newArgs.last.asInstanceOf[Expression]
    new UDAFExpr(udtfParam, newArg).asInstanceOf[this.type]
  }

  override def resultType: TypeInformation[_] = implicitly[TypeInformation[T]]

  override def validateInput(): ValidationResult = {
    if (isTypeMatch(Seq(resultType), Seq(child.resultType), isVarArgs = false)) {
      ValidationSuccess
    } else {
      val inputType = child.resultType.getTypeClass.getSimpleName
      val operandType = resultType.getTypeClass.getSimpleName
      val udafName = udaf.getClass.getSimpleName
      ValidationFailure(
        s"Cannot apply '$udafName' to arguments of type '$udafName(<$inputType>)'." +
          s" Supported form(s): '$udafName(<$operandType>)'")
    }
  }

  override def toString(): String = s"${udaf.getClass.getSimpleName}($child)"

  override def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    val sqlFunction = AggSqlFunctionObj.create(name, udaf, resultType, resultType)
    relBuilder.aggregateCall(sqlFunction, false, null, name, child.toRexNode)
  }
}
