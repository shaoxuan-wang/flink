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

package org.apache.flink.ml.util.persist;

import org.apache.flink.ml.api.misc.persist.Persistable;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * Utility to restore a PipelineStage from a stage json.
 */
public class MLStageFactory {
	/**
	 * Restores a PipelineStage with a stage json, requiring json is a JsonObject format and has
	 * stageClassName, and class of stage has a non-parameter constructor.
	 *
	 * @param json the stage json to restore a PipelineStage from
	 * @return the restored PipelineStage
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Persistable> T createFromJson(String json) {
		Gson gson = new Gson();
		JsonObject jobj = gson.fromJson(json, JsonObject.class);
		String className = jobj.get("stageClassName").getAsString();
		if (className == null) {
			throw new RuntimeException(
					"Can not create a PipelineStage with a json without stageClassName signed");
		}
		try {
			Persistable s = (Persistable) Class.forName(className).newInstance();
			s.loadJson(json);
			return (T) s;
		} catch (Exception e) {
			throw new RuntimeException("Unknown or illegal class:" + className, e);
		}
	}

	/**
	 * Signs the class name of a PipelineStage into the stage json, which is required when restoring
	 * the PipelineStage. The PipelineStage must have a non-parameter constructor.
	 *
	 * @param json the stage json to sign the class name
	 * @param s    the PipelineStage whose class name will be signed into the json
	 */
	public static void signWithClass(JsonObject json, Object s) {
		json.addProperty("stageClassName", s.getClass().getCanonicalName());
	}
}
