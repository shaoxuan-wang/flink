/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the column name of predicted detailed info.
 *
 * <p>The detailed info is the details of prediction result, such as the probability of each label in classifier.
 */
public interface HasPredDetailColName<T> extends WithParams <T> {

	ParamInfo <String> PRED_DETAIL_COL_NAME = ParamInfoFactory
		.createParamInfo("predDetailColName", String.class)
		.setDescription("Column name of predicted result, it will include detailed info.")
		.build();

	default String getPredDetailColName() {
		return get(PRED_DETAIL_COL_NAME);
	}

	default T setPredDetailColName(String value) {
		return set(PRED_DETAIL_COL_NAME, value);
	}
}
