package org.apache.flink.table.runtime.aggregate

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.types.Row

/**
  * Created by wshaoxuan on 24/12/2016.
  */
class MyAccumulator extends Accumulator{
  var count: Long = 0
}

class myAgg extends Aggregate[Long] {
  private var countIndex: Int = _
  var count: Long = 0

  override def add(accumulator: Accumulator, value: Any) = {
    accumulator.asInstanceOf[MyAccumulator].count += value.asInstanceOf[Long]
  }

  override def getResult(accumulator: Accumulator): Long = {
    accumulator.asInstanceOf[MyAccumulator].count
  }

  override def merge(a: Accumulator, b: Accumulator): Accumulator = {
    b
  }

  override def createAccumulator(): MyAccumulator = {
    new MyAccumulator
  }

  override def initiate(intermediate: Row): Unit = {
    intermediate.setField(countIndex, 0L)
    count = 0
  }

  override def merge(intermediate: Row, buffer: Row): Unit = {
    val partialCount = intermediate.getField(countIndex).asInstanceOf[Long]
    val bufferCount = buffer.getField(countIndex).asInstanceOf[Long]
    buffer.setField(countIndex, partialCount + bufferCount)
  }


  override def evaluate(buffer: Row): Long = {
    buffer.getField(countIndex).asInstanceOf[Long]
  }

  override def prepare(value: Any, intermediate: Row): Unit = {
    if (value == null) {
      intermediate.setField(countIndex, 0L)
    } else {
      intermediate.setField(countIndex, 1L)
    }
  }

  override def intermediateDataType = Array(BasicTypeInfo.LONG_TYPE_INFO)

  override def supportPartial: Boolean = true

  override def setAggOffsetInRow(aggIndex: Int): Unit = {
    countIndex = aggIndex
  }
}
