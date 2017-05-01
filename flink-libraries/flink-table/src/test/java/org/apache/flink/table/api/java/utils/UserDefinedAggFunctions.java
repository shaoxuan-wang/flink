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
package org.apache.flink.table.api.java.utils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.functions.AggregateFunction;

import java.io.Serializable;
import java.util.Iterator;

public class UserDefinedAggFunctions {

    public static class WeightedAvgAccum extends Tuple2<Long, Integer> {
        public long sum = 0;
        public int count = 0;
    }

    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {
        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        @Override
        public Long getValue(WeightedAvgAccum accumulator) {
            if (accumulator.count == 0)
                return null;
            else
                return accumulator.sum/accumulator.count;
        }

        public void accumulate(WeightedAvgAccum accumulator, long iValue, int iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }

        public void accumulate(WeightedAvgAccum accumulator, int iValue, int iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }

    public static class WeightedAvgWithMerge extends WeightedAvg {
        public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
            Iterator<WeightedAvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                WeightedAvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }
    }

    public static class WeightedAvgWithMergeAndReset extends WeightedAvgWithMerge {
        public void resetAccumulator(WeightedAvgAccum acc) {
            acc.count = 0;
            acc.sum = 0L;
        }
    }

    public static class WeightedAvgWithRetract extends WeightedAvg {
        public void retract(WeightedAvgAccum accumulator, long iValue, int iWeight) {
            accumulator.sum -= iValue * iWeight;
            accumulator.count -= iWeight;
        }

        public void retract(WeightedAvgAccum accumulator, int iValue, int iWeight) {
            accumulator.sum -= iValue * iWeight;
            accumulator.count -= iWeight;
        }
    }

    public static class PojoTestClass implements Serializable {
        public long sum = 0;
        public int count = 0;
    }

    public static class PojoWeightedAvgAccum implements Serializable {
        public long sum = 0;
        public int count = 0;
        PojoTestClass classA = new PojoTestClass();
    }

    public static class PojoWeightedAvg extends AggregateFunction<Long, PojoWeightedAvgAccum> {
        @Override
        public PojoWeightedAvgAccum createAccumulator() {
            return new PojoWeightedAvgAccum();
        }

        @Override
        public Long getValue(PojoWeightedAvgAccum accumulator) {
            if (accumulator.count == 0)
                return null;
            else
                return accumulator.sum/accumulator.count;
        }

        public void accumulate(PojoWeightedAvgAccum accumulator, long iValue, int iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }

    public static class AvgInfo {
        public long avg;
        public long totalNumber = 0; // not generic type, must: private long totalNumber = 0

        public AvgInfo(long avg) {
            this.avg = avg;
        }

        public void incrTotalNumber(){
            this.totalNumber++;
        }
    }

    public static class GenericWeightedAvg extends AggregateFunction<AvgInfo,
            PojoWeightedAvgAccum> {
        @Override
        public PojoWeightedAvgAccum createAccumulator() {
            return new PojoWeightedAvgAccum();
        }

        @Override
        public AvgInfo getValue(PojoWeightedAvgAccum accumulator) {
            if (accumulator.count == 0)
                return null;
            else {
                AvgInfo avgInfo = new AvgInfo(accumulator.sum/accumulator.count);
                avgInfo.incrTotalNumber();
                return avgInfo;
            }
        }

        public void accumulate(PojoWeightedAvgAccum accumulator, long iValue, int iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }
}
