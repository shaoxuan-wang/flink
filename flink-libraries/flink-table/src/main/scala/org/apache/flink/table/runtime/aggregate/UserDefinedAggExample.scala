package org.apache.flink.table.runtime.aggregate

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.types.Row

/**
  * Created by wshaoxuan on 22/12/2016.
  */
//class firstUDAF extends Aggregate[Long]{
//  private var countIndex: Int = _
//
//  override def initiate(intermediate: Row): Unit = {
//    intermediate.setField(countIndex, 0L)
//  }
//
//  override def merge(intermediate: Row, buffer: Row): Unit = {
//    val partialCount = intermediate.getField(countIndex).asInstanceOf[Long]
//    val bufferCount = buffer.getField(countIndex).asInstanceOf[Long]
//    buffer.setField(countIndex, partialCount + bufferCount)
//  }
//
//  override def evaluate(buffer: Row): Long = {
//    buffer.getField(countIndex).asInstanceOf[Long]
//  }
//
//  override def prepare(value: Any, intermediate: Row): Unit = {
//    if (value == null) {
//      intermediate.setField(countIndex, 0L)
//    } else {
//      intermediate.setField(countIndex, 1L)
//    }
//  }
//
//  override def intermediateDataType = Array(BasicTypeInfo.LONG_TYPE_INFO)
//
//  override def supportPartial: Boolean = true
//
//  override def setAggOffsetInRow(aggIndex: Int): Unit = {
//    countIndex = aggIndex
//  }
//
//  private var first: Option[String] = None
//
//  //  def merge(other: AggFunction[T]): Unit = {
//  //    throw new UnsupportedOperationException("FIRST UDAF not support merge")
//  //  }
//
//  def get(): String = {
//    if (first.isEmpty) {
//      null.asInstanceOf[String]
//    } else {
//      first.get
//    }
//  }
//
//  def accumulate(input: Any): Unit = {
//    if (input != null) {
//      val value = input.asInstanceOf[String]
//      if (first.isEmpty) {
//        first = Some(value)
//      }
//    }
//  }
//}
