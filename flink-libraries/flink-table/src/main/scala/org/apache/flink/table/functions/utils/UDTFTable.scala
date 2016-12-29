package org.apache.flink.table.functions.utils

import org.apache.flink.table.api.Table
import org.apache.flink.table.expressions.Expression

/**
  * Created by wshaoxuan on 29/12/2016.
  */
class UDTFTable(private[flink] val udtf: Expression) extends Table(null,null)
