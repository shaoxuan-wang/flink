package org.apache.flink.table.runtime.aggregate

import java.lang.Iterable

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.types.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.{Collector, Preconditions}

/**
  * Created by wshaoxuan on 13/01/2017.
  */
class newAggregateWindowFunction[W <: Window](
    private val aggregates: Array[Aggregate[_ <: Any]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val aggregateMapping: Array[(Int, Int)],
    private val finalRowArity: Int)
  extends RichWindowFunction[Row, Row, Tuple, W] {

  private var output: Row = _

  override def open(parameters: Configuration): Unit = {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupKeysMapping)
    output = new Row(finalRowArity)
  }

  /**
    * Calculate aggregated values output by aggregate buffer, and set them into output
    * Row based on the mapping relation between intermediate aggregate data and output data.
    */
  override def apply(
      key: Tuple,
      window: W,
      records: Iterable[Row],
      out: Collector[Row]): Unit = {

    val iterator = records.iterator

    if (iterator.hasNext) {
      val record = iterator.next()
      // Set group keys value to final output.
//      groupKeysMapping.foreach {
//        case (after, previous) =>
//          output.setField(after, record.getField(previous))
//      }
//
//      val aggregates: Seq[Aggregate[_]] = aggregateMapping.indices.map { index =>
//        record.getField(index + groupKeysMapping.length).asInstanceOf[Aggregate[_]]
//      }
//
//      // Evaluate final aggregate value and set to output.
//      aggregateMapping.foreach {
//        case (after, previous) =>
//          output.setField(after, aggregates(previous).finish())
//      }
//
//      out.collect(output)
      out.collect(record)
    }
  }
}