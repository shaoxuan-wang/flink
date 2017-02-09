package org.apache.flink.table.runtime.aggregate

import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.types.Row

/**
  * Created by wshaoxuan on 22/12/2016.
  */
//class AggFoldFunction(
//  private val aggFields: Array[Int],
//  private val groupKeys: Array[Int],
//  private val aggregateMapping: Array[(Int, Int)],
//  @transient private val returnType: TypeInformation[Row])
//  extends FoldFunction[Row, Row]
//  with ResultTypeQueryable[Row] {
//
//    override def fold(accumulator: Row, value: Row): Row = {
//      val aggregates: Seq[Aggregate[_]] = aggFields.indices.map { index =>
//        accumulator.getField(index + groupKeys.length).asInstanceOf[Aggregate[_]]
//      }
//
//      // Set group keys value to final output.
//      groupKeys.zipWithIndex.foreach {
//        case (previous, after) =>
//          accumulator.setField(after, value.getField(previous))
//      }
//
//      // update aggregate and set to output.
//      aggregates.zipWithIndex.foreach { case (udaf, index) =>
//        val v = value.getField(aggFields(index))
//        udaf.accumulate(v)
//        //accumulator.setField(groupKeys.length + index, udaf)
//      }
//
//      accumulator
//    }
//
//    override def getProducedType: TypeInformation[Row] = {
//      returnType
//    }
//
//}
