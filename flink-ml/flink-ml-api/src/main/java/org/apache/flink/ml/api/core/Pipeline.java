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

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.util.persist.MLStageFactory;
import org.apache.flink.table.api.Table;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * A pipeline is a linear workflow which chains {@link Estimator}s and {@link Transformer}s to
 * execute an algorithm.
 *
 * <p>A pipeline itself can either act as an Estimator or a Transformer, depending on the stages it
 * includes. More specifically:
 * <ul>
 * <li>
 * If a Pipeline has an {@link Estimator}, one needs to call {@link Pipeline#fit(Table)} before use
 * the pipeline as a {@link Transformer}. In this case the Pipeline is an {@link Estimator} and can
 * produce a Pipeline as a {@link Model}.
 * </li>
 * <li>
 * If a Pipeline has no {@link Estimator}, it is a {@link Transformer} and can be applied to a Table
 * directly. In this case, {@link Pipeline#fit(Table)} will simply return the pipeline itself.
 * </li>
 * </ul>
 *
 * <p>In addition, a pipeline can also be used as a {@link PipelineStage} in another pipeline, just
 * like an ordinary {@link Estimator} or {@link Transformer} as describe above.
 */
public final class Pipeline implements Estimator<Pipeline, Pipeline>, Transformer<Pipeline>,
	Model<Pipeline> {
	private static final long serialVersionUID = 1L;
	private List<PipelineStage> stages;
	private Params params;

	public Pipeline() {
		this(new ArrayList<>());
	}

	public Pipeline(List<PipelineStage> stages) {
		this.stages = stages;
		this.params = new Params();
	}

	private static boolean isStageNeedFit(PipelineStage stage) {
		return (stage instanceof Pipeline && ((Pipeline) stage).needFit()) ||
			(!(stage instanceof Pipeline) && stage instanceof Estimator);
	}

	/**
	 * Appends a PipelineStage to the tail of this pipeline.
	 *
	 * @param stage the stage to be appended
	 */
	public Pipeline appendStage(PipelineStage stage) {
		stages.add(stage);
		return this;
	}

	/**
	 * Returns a list of all stages in this pipeline in order.
	 *
	 * @return a list of all stages in this pipeline in order.
	 */
	public List<PipelineStage> getStages() {
		return stages;
	}

	/**
	 * Check whether the pipeline acts as an {@link Estimator} or not. When the return value is
	 * true, that means this pipeline contains an {@link Estimator} and thus users must invoke
	 * {@link #fit(Table)} before they can use this pipeline as a {@link Transformer}. Otherwise,
	 * the pipeline can be used as a {@link Transformer} directly.
	 *
	 * @return {@code true} if this pipeline has an Estimator, {@code false} otherwise
	 */
	public boolean needFit() {
		return this.getIndexOfLastEstimator() >= 0;
	}

	public Params getParams() {
		return params;
	}

	//find the last Estimator or Pipeline that needs fit in stages, -1 stand for no Estimator in Pipeline
	private int getIndexOfLastEstimator() {
		int lastEstimatorIndex = -1;
		for (int i = 0; i < stages.size(); i++) {
			PipelineStage stage = stages.get(i);
			lastEstimatorIndex = isStageNeedFit(stage) ? i : lastEstimatorIndex;
		}
		return lastEstimatorIndex;
	}

	@Override
	/**
	 * Train the pipeline to fit on the records in the given {@link Table}.
	 *
	 * <p>This method go through all the {@link PipelineStage}s in order and does the following
	 * on each stage until the last {@link Estimator}(inclusive).
	 *
	 * <ul>
	 *     <li>
	 *         If a stage is an {@link Estimator}, invoke {@link Estimator#fit(Table)} with the input table
	 *         to generate a {@link Model}, transform the the input table with the generated {@link Model}
	 *         to get a result table, then pass the result table to the next stage as input.
	 *     </li>
	 *     <li>
	 *         If a stage is a {@link Transformer}, invoke {@link Transformer#transform(Table)} on the input
	 *         table to get a result table, and pass the result table to the next stage as input.
	 *     </li>
	 * </ul>
	 *
	 * After all the {@link Estimator}s are trained to fit their input tables, a new pipeline will be created
	 * with the same stages in this pipeline, except that all the Estimators in the new pipeline are replaced
	 * with their corresponding Models generated in the above process.
	 * <p>If there is no {@link Estimator} in the pipeline, the method returns a copy of this pipeline.
	 */
	public Pipeline fit(Table input) {
		List<PipelineStage> transformStages = new ArrayList<>();
		int lastEstimatorIdx = getIndexOfLastEstimator();
		for (int i = 0; i < stages.size(); i++) {
			PipelineStage s = stages.get(i);
			if (i <= lastEstimatorIdx) {
				Transformer t;
				boolean needFit = isStageNeedFit(s);
				if (needFit) {
					t = ((Estimator) s).fit(input);
				} else if (s instanceof Transformer) {
					t = (Transformer) s;
				} else {
					throw new RuntimeException(
						"All PipelineStages should be Estimator or Transformer, got:" +
							s.getClass().getSimpleName());
				}
				transformStages.add(t);
				input = t.transform(input);
			} else {
				transformStages.add(s);
			}
		}
		return new Pipeline(transformStages);
	}

	/**
	 * Generate a result table by applying all the stages in this pipeline to the input table in
	 * order.
	 *
	 * @param input the table to be transformed
	 * @return a result table with all the stages applied to the input tables in order.
	 */
	@Override
	public Table transform(Table input) {
		if (needFit()) {
			throw new RuntimeException("Pipeline contains Estimator, need to fit first.");
		}
		for (PipelineStage s : stages) {
			input = ((Transformer) s).transform(input);
		}
		return input;
	}

	@Override
	public String toJson() {
		Gson gson = new Gson();
		JsonObject json = new JsonObject();
		JsonArray stageJsons = new JsonArray();
		for (PipelineStage s : stages) {
			String stageJson = s.toJson();
			JsonObject stageJsonObj = gson.fromJson(stageJson, JsonObject.class);
			MLStageFactory.signWithClass(stageJsonObj, s);
			stageJsons.add(stageJsonObj);
		}
		json.add("stages", stageJsons);
		MLStageFactory.signWithClass(json, this);
		return json.toString();
	}

	@Override
	public void loadJson(String json) {
		Gson gson = new Gson();
		JsonArray stageJsons = gson.fromJson(json, JsonObject.class).getAsJsonArray("stages");
		for (int i = 0; i < stageJsons.size(); i++) {
			JsonObject stageJsonObj = stageJsons.get(i).getAsJsonObject();
			stages.add(MLStageFactory.createFromJson(stageJsonObj.toString()));
		}
	}
}
