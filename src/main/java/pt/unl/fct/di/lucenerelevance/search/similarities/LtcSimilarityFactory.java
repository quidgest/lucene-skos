package pt.unl.fct.di.lucenerelevance.search.similarities;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity; // javadoc
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.SimilarityFactory;

/**
 * Factory for {@link LtcSimilarity}
 * <p>
 * LtcSimilarity is based on Lucene's default scoring implementation,
 * but uses logarithmic tf weighting.
 * <p>
 * Optional settings:
 * <ul>
 *   <li>discountOverlaps (bool): Sets
 *       {@link LtcSimilarity#setDiscountOverlaps(boolean)}</li>
 * </ul>
 * @see TFIDFSimilarity
 * @lucene.experimental
 */
public class LtcSimilarityFactory extends SimilarityFactory {
	private boolean discountOverlaps;

	@Override
	public void init(SolrParams params) {
		super.init(params);
		discountOverlaps = params.getBool("discountOverlaps", true);
	}

	@Override
	public Similarity getSimilarity() {
		LtcSimilarity sim = new LtcSimilarity();
		sim.setDiscountOverlaps(discountOverlaps);
		return sim;
	}
}