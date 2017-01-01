package org.apache.flink.table.functions.utils

import org.apache.flink.table.api.Table
import org.apache.flink.table.expressions.Expression

/**
  * Wrapper class which wraps the expression of an User-Defined Table Function (UDTF), such that
  * UDTF can use the existing join and leftOuterJoin APIs in [[Table]].
  */
class UDTFTable(private[flink] val udtf: Expression) extends Table(null,null)
