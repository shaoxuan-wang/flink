package org.apache.flink.table.runtime.aggregate

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, TypeExtractor}
import org.apache.flink.types.Row

/**
  * Created by wshaoxuan on 11/01/2017.
  */
class AggAggregateFunction(
  private val aggFields: Array[Int],
  private val groupKeys: Array[Int],
  private val aggregateMapping: Array[(Int, Int)],
  private val aggregates: Array[Aggregate[_]],
  private val finalRowArity: Int,
  private val groupKeysMapping: Array[(Int, Int)],
  @transient private val returnType: TypeInformation[Row])
  extends AggregateFunction[Row, Row , Row] {

  //Create the row for Accumulation, it is like the initialization of the first row
  override def createAccumulator(): Row = {
    val row: Row = new Row(groupKeys.length + aggregates.length)
//    aggregates.zipWithIndex.foreach{ case (agg, index) =>
//      row.setField(groupings.length + index, agg)
//      agg.init()
//    }

    aggregates.zipWithIndex.foreach{ case (agg, index) =>
      row.setField(groupKeys.length + index, agg.createAccumulator())
      //agg.init()
    }
    // Set group keys value to final output.
//    groupKeys.zipWithIndex.foreach {
//      case (previous, after) =>
//        accumulator.setField(after, value.getField(previous))
//    }
    row
  }

  override def add(value: Row, accumulator: Row) = {
    aggregates.zipWithIndex.foreach{
      case (agg, index) =>
      {
//        val myType = TypeExtractor.createTypeInfo(agg,
//                                                  classOf[Aggregate[_]],
//                                                  agg.getClass, 1)
        val myAccum = accumulator.getField(index + groupKeys.length).asInstanceOf[Accumulator]
        val myValue = value.getField(1) //should be truct here, 1 because the key is now supposed
        // to be 1
        agg.add(myAccum, myValue)
      }
    }

//    val aggregates: Seq[Aggregate[_,_]] = aggFields.indices.map { index =>
//      accumulator.getField(index + groupKeys.length).asInstanceOf[Aggregate[_]]
//    }
//
////    // Set group keys value to final output.
//    groupKeys.zipWithIndex.foreach {
//      case (previous, after) =>
//        accumulator.setField(after, value.getField(previous))
//    }
//
//    // update aggregate and set to output.
//    aggregates.zipWithIndex.foreach { case (udaf, index) =>
//      val v = value.getField(aggFields(index))
//      udaf.accumulate(v)
//      udaf
//      //accumulator.setField(groupKeys.length + index, udaf)
//    }
//    accumulator
  }

  // prepare for window function
  override def getResult(accumulator: Row): Row = {
    var output = new Row(finalRowArity)

//    groupKeysMapping.foreach {
//      case (after, previous) =>
//        output.setField(after, accumulator.getField(previous))
//    }

    // Set group keys value to final output.
//    groupKeys.zipWithIndex.foreach {
//      case (previous, after) =>
//        accumulator.setField(after, value.getField(previous))
//    }

    aggregates.zipWithIndex.foreach{ case (agg, index) =>
      {
        val myAccum = accumulator.getField(index + groupKeys.length).asInstanceOf[Accumulator]
        output.setField(groupKeys.length + index, agg.getResult(myAccum))
      }
    }

//    groupKeysMapping.foreach {
//      case (after, previous) =>
//        output.setField(after, accumulator.getField(previous))
//    }
//
//    val aggregates: Seq[Aggregate[_]] = aggregateMapping.indices.map { index =>
//      accumulator.getField(index + groupKeysMapping.length).asInstanceOf[Aggregate[_]]
//    }
//
//    // Evaluate final aggregate value and set to output.
//    aggregateMapping.foreach {
//      case (after, previous) =>
//        output.setField(after, aggregates(previous).finish())
//    }
    output
  }

  // like
  override def merge(a: Row, b: Row): Row = {
    aggregates.zipWithIndex.foreach {
      case (agg, index) => {
        val aAccum = a.getField(index + groupKeys.length).asInstanceOf[Accumulator]
        val bAccum = b.getField(index + groupKeys.length).asInstanceOf[Accumulator]
        a.setField(index + groupKeys.length, agg.merge(aAccum, bAccum))
      }
    }
    a
  }

}
