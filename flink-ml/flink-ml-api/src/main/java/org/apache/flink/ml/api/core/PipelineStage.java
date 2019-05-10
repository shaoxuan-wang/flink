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

package org.apache.flink.ml.api.core;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.ml.api.misc.persist.Persistable;
import org.apache.flink.ml.util.param.ExtractParamInfosUtil;
import org.apache.flink.ml.util.persist.MLStageFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for a stage in a pipeline. The interface is only a concept, and does not have any
 * actual functionality. Its subclasses must be either Estimator or Transformer. No other classes
 * should inherit this interface directly.
 *
 * <p>Each pipeline stage is with parameters and meanwhile persistable.
 *
 * @param <T> The class type of the PipelineStage implementation itself, used by {@link
 *            org.apache.flink.ml.api.misc.param.WithParams}
 * @see WithParams
 */
interface PipelineStage<T extends PipelineStage<T>> extends WithParams<T>, Serializable,
		Persistable {

	default String toJson() {
		Gson gson = new Gson();
		JsonObject jsonObj = new JsonObject();
		MLStageFactory.signWithClass(jsonObj, this);
		String paramJson = getParams().toJson();
		jsonObj.add("params", gson.fromJson(paramJson, JsonObject.class));
		return jsonObj.toString();
	}

	default void loadJson(String json) {
		Gson gson = new Gson();
		JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
		JsonObject paramsObj = jsonObj.getAsJsonObject("params");

		List<ParamInfo> paramInfos = ExtractParamInfosUtil.extractParamInfos(this);
		Map<String, Class<?>> map = new HashMap<>();
		for (ParamInfo i : paramInfos) {
			map.put(i.getName(), i.getValueClass());
		}
		getParams().fromJson(paramsObj.toString(), map);
	}
}
