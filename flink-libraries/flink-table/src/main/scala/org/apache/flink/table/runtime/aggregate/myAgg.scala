package org.apache.flink.table.runtime.aggregate

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.types.Row

/**
  * Created by wshaoxuan on 24/12/2016.
  */
class myAgg extends Aggregate[Long] {
  private var countIndex: Int = _
  var count: Int = 0

  override def initiate(intermediate: Row): Unit = {
    intermediate.setField(countIndex, 0L)
    count = 0
  }

  override def init(): Unit = {
    count = 50
  }

  override def merge(intermediate: Row, buffer: Row): Unit = {
    val partialCount = intermediate.getField(countIndex).asInstanceOf[Long]
    val bufferCount = buffer.getField(countIndex).asInstanceOf[Long]
    buffer.setField(countIndex, partialCount + bufferCount)
  }


  override def evaluate(buffer: Row): Long = {
    buffer.getField(countIndex).asInstanceOf[Long]
  }

  override def accumulate(input: Any): Unit = {
    if (input != null) {
      count += 1
    }
  }

  override def finish(): Long = {
    count
    //iterator
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
