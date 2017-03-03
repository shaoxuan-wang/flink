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
package org.apache.flink.table.functions.aggfunctions

import java.math.BigDecimal
import java.util.{HashMap => JHashMap, List => JList}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple3 => JTuple3}
import org.apache.flink.api.java.typeutils.{MapTypeInfo, TupleTypeInfo}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

/** The initial accumulator for Min aggregate function */
class MinAccumulator[T] extends JTuple3[T, Long, JHashMap[T, Long]] with Accumulator

/**
  * Base class for built-in Min aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MinAggFunction[T](implicit ord: Ordering[T]) extends AggregateFunction[T] {

  override def createAccumulator(): Accumulator = {
    val acc = new MinAccumulator[T]
    acc.f0 = getInitValue //min
    acc.f1 = 0L //total count
    acc.f2 = new JHashMap[T, Long]() //store the count for each value
    acc
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val a = accumulator.asInstanceOf[MinAccumulator[T]]

      if (a.f1 == 0 || (ord.compare(a.f0, v) > 0)) {
        a.f0 = v
      }

      a.f1 += 1L

      if (!a.f2.containsKey(v)) {
        a.f2.put(v, 1L)
      } else {
        var count = a.f2.get(v)
        count += 1L
        a.f2.put(v, count)
      }
    }
  }

  override def retract(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val a = accumulator.asInstanceOf[MinAccumulator[T]]

      a.f1 -= 1L

      if (!a.f2.containsKey(v)) {
        throw TableException("unexpected retract message")
      } else {
        var count = a.f2.get(v)
        count -= 1L
        if (count == 0) {
          //remove the key v from the map if the number of appearance of the value v is 0
          a.f2.remove(v)
          //if the total count is 0, we could just simply set the f0(min) to the initial value
          if (a.f1 == 0) {
            a.f0 = getInitValue
            return
          }
          //if v is the current min value, we have to iterate the map to find the 2nd smallest
          // value to replace v as the min value
          if (v == a.f0) {
            val iterator = a.f2.keySet().iterator()
            var key = iterator.next()
            a.f0 = key
            while (iterator.hasNext()) {
              key = iterator.next()
              if (ord.compare(a.f0, key) > 0) {
                a.f0 = key
              }
            }
          }
        } else {
          a.f2.put(v, count)
        }
      }
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val a = accumulator.asInstanceOf[MinAccumulator[T]]
    if (a.f1 != 0) {
      a.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0)
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[MinAccumulator[T]]
      if (a.f1 != 0) {
        accumulate(ret.asInstanceOf[MinAccumulator[T]], a.f0)
      }
      i += 1
    }
    ret
  }

  override def getAccumulatorType(): TypeInformation[_] = {
    new TupleTypeInfo(
      new MinAccumulator[T].getClass,
      getValueTypeInfo,
      BasicTypeInfo.LONG_TYPE_INFO,
      new MapTypeInfo(getValueTypeInfo, BasicTypeInfo.LONG_TYPE_INFO))
  }

  def getInitValue: T

  def getValueTypeInfo: TypeInformation[_]
}

/**
  * Built-in Byte Min aggregate function
  */
class ByteMinAggFunction extends MinAggFunction[Byte] {
  override def getInitValue: Byte = 0.toByte
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

/**
  * Built-in Short Min aggregate function
  */
class ShortMinAggFunction extends MinAggFunction[Short] {
  override def getInitValue: Short = 0.toShort
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO
}

/**
  * Built-in Int Min aggregate function
  */
class IntMinAggFunction extends MinAggFunction[Int] {
  override def getInitValue: Int = 0
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO
}

/**
  * Built-in Long Min aggregate function
  */
class LongMinAggFunction extends MinAggFunction[Long] {
  override def getInitValue: Long = 0L
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO
}

/**
  * Built-in Float Min aggregate function
  */
class FloatMinAggFunction extends MinAggFunction[Float] {
  override def getInitValue: Float = 0.0f
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO
}

/**
  * Built-in Double Min aggregate function
  */
class DoubleMinAggFunction extends MinAggFunction[Double] {
  override def getInitValue: Double = 0.0d
  override def getValueTypeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO
}

/**
  * Built-in Boolean Min aggregate function
  */
class BooleanMinAggFunction extends MinAggFunction[Boolean] {
  override def getInitValue: Boolean = false
  override def getValueTypeInfo = BasicTypeInfo.BOOLEAN_TYPE_INFO
}

/**
  * Built-in Big Decimal Min aggregate function
  */
class DecimalMinAggFunction extends MinAggFunction[BigDecimal] {
  override def getInitValue: BigDecimal = BigDecimal.ZERO
  override def getValueTypeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO
}
