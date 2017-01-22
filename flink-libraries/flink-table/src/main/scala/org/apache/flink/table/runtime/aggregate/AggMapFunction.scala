package org.apache.flink.table.runtime.aggregate

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

/**
  * Created by wshaoxuan on 22/12/2016.
  */
class AggMapFunction(
    private val groupKeysMapping: Array[(Int, Int)],
    private val aggregateMapping: Array[(Int, Int)],
    @transient private val returnType: TypeInformation[Row])
  extends RichMapFunction[Row, Row] with ResultTypeQueryable[Row] {

  private var output: Row = _

  override def open(config: Configuration) {
    Preconditions.checkNotNull(groupKeysMapping)
    Preconditions.checkNotNull(aggregateMapping)
    val finalRowLength: Int = groupKeysMapping.length + aggregateMapping.length
    output = new Row(finalRowLength)
  }

  override def map(value: Row): Row = {
    // Set group keys value to final output.
    groupKeysMapping.foreach {
      case (after, previous) =>
        output.setField(after, value.getField(previous))
    }

    val aggregates: Seq[Aggregate[_]] = aggregateMapping.indices.map { index =>
      value.getField(index + groupKeysMapping.length).asInstanceOf[Aggregate[_]]
    }

    // Evaluate final aggregate value and set to output.
    aggregateMapping.foreach {
      case (after, previous) =>
        output.setField(after, aggregates(previous).finish())
    }

    output
  }

  override def getProducedType: TypeInformation[Row] = {
    returnType
  }
}
