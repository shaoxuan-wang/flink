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
package org.apache.flink.api.table.functions

/**
  * Created by wshaoxuan on 15/12/2016.
  *
  *
  *
  * accumulate an input to the aggregate.
  * param input input value. The input type should be [T], user can cast it to T.
  *
  def accumulate(input: Any)
  *
  * When supportPartial is true, we will use merge() function to merge partial aggregate.
  * Aggregate with another partial aggregation result.
  * param other another partial aggregate
  *
  def merge(other: AggFunction[T])
  */
abstract class AggFunction[T] extends Serializable {

  /**
    * initial aggregate
    */
  def initiate(): Unit

  /**
    * aggregation is finished
    * TODO: do we need to return the FINAL aggregation result
    */
  def finish(): Unit

  /**
    * get the current aggregation result
    * @return
    */
  def get(): T

  /**
    * Whether aggregate function support partial aggregate.
    *
    * @return True if the aggregate supports partial aggregation, False otherwise.
    */
  def supportPartial: Boolean = false
}
