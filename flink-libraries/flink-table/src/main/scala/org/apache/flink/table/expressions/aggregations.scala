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
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.AggCall
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.utils.AggSqlFunctionObj
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.validate.{ValidationResult, ValidationSuccess}

abstract sealed class Aggregation extends Expression {

  override def toString = s"Aggregate"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException("Aggregate cannot be transformed to RexNode")

  /**
    * Convert Aggregate to its counterpart in Calcite, i.e. AggCall
    */
  private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall

  /**
    * Returns the SqlAggFunction for this Aggregation.
    */
  private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder): SqlAggFunction

}

case class Sum(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"sum($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.SUM, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "sum")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    val returnType = relBuilder
      .getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .createTypeFromTypeInfo(resultType)
    new SqlSumAggFunction(returnType)
  }
}

case class Min(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"min($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.MIN, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "min")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    new SqlMinMaxAggFunction(MIN)
  }
}

case class Max(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"max($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.MAX, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "max")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    new SqlMinMaxAggFunction(MAX)
  }
}

case class Count(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"count($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.COUNT, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    new SqlCountAggFunction()
  }
}

case class Avg(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"avg($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.AVG, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "avg")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    new SqlAvgAggFunction(AVG)
  }
}

case class UDAGGFunctionCall[T: TypeInformation, ACC](
    udagg: AggregateFunction[T, ACC],
    args: Seq[Expression])
  extends Aggregation {

  override private[flink] def children: Seq[Expression] = args

  // Override makeCopy method in TreeNode, to produce vargars properly
  override def makeCopy(newArgs: Array[AnyRef]): this.type = {
    if (newArgs.length < 1) {
      throw new TableException("Invalid constructor params")
    }
    val udaggParam = newArgs.head.asInstanceOf[AggregateFunction[T, ACC]]
    val newArg = newArgs.last.asInstanceOf[Seq[Expression]]
    new UDAGGFunctionCall(udaggParam, newArg).asInstanceOf[this.type]
  }

  //  val AccType = udagg.getClass.
  //    getGenericSuperclass.asInstanceOf[ParameterizedTypeImpl].getActualTypeArguments()(1)
  //    .asInstanceOf[Class[_]]
  //  val returnType = udagg.getClass.getMethod("getValue", AccType).getReturnType
  //  override def resultType: TypeInformation[_] = TypeExtractor.createTypeInfo(returnType)
  override def resultType: TypeInformation[_] = implicitly[TypeInformation[T]]

  override def validateInput(): ValidationResult = {
    //这里需要加入valid accumulate等方法的函数
    val signiture = children.map(_.resultType)

    ValidationSuccess
    //    if (isTypeMatch(Seq(resultType), Seq(child.resultType), isVarArgs = false)) {
    //      ValidationSuccess
    //    } else {
    //      val inputType = child.resultType.getTypeClass.getSimpleName
    //      val operandType = resultType.getTypeClass.getSimpleName
    //      val udafName = udagg.getClass.getSimpleName
    //      ValidationFailure(
    //        s"Cannot apply '$udafName' to arguments of type '$udafName(<$inputType>)'." +
    //          s" Supported form(s): '$udafName(<$operandType>)'")
    //    }
  }

  override def toString(): String = s"${udagg.getClass.getSimpleName}($args)"

  override def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val sqlFunction = AggSqlFunctionObj(name, udagg, resultType, resultType, typeFactory)
    relBuilder.aggregateCall(sqlFunction, false, null, name, args.map(_.toRexNode): _*)
  }

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    AggSqlFunctionObj("UDAGG", udagg, resultType, resultType, typeFactory)
  }
}
