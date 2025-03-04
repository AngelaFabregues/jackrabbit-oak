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
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import co.elastic.clients.elasticsearch.core.MsearchRequest;
import co.elastic.clients.elasticsearch.core.MsearchResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.msearch.MultiSearchResponseItem;
import co.elastic.clients.elasticsearch.core.msearch.RequestItem;
import co.elastic.clients.elasticsearch.core.search.Hit;

/**
 * This class is in charge to extract spell checked suggestions for a given query.
 * <p>
 * It requires 2 calls to Elastic:
 * <ul>
 *     <li>get all the possible spellchecked suggestions</li>
 *     <li>multi search query to get a sample of 100 results for each suggestion for ACL check</li>
 * </ul>
 */
class ElasticSpellcheckIterator implements Iterator<FulltextResultRow> {

    private static final  Logger LOG = LoggerFactory.getLogger(ElasticSpellcheckIterator.class);
    protected static final String SPELLCHECK_PREFIX = "spellcheck?term=";

    private static final ObjectMapper JSON_MAPPER;

    static {
        JsonFactoryBuilder factoryBuilder = new JsonFactoryBuilder();
        factoryBuilder.disable(JsonFactory.Feature.INTERN_FIELD_NAMES);
        JSON_MAPPER = new ObjectMapper(factoryBuilder.build());
        JSON_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final ElasticIndexNode indexNode;
    private final ElasticRequestHandler requestHandler;
    private final ElasticResponseHandler responseHandler;
    private final String spellCheckQuery;

    private Iterator<FulltextResultRow> internalIterator;
    private boolean loaded = false;

    ElasticSpellcheckIterator(@NotNull ElasticIndexNode indexNode,
                              @NotNull ElasticRequestHandler requestHandler,
                              @NotNull ElasticResponseHandler responseHandler) {
        this.indexNode = indexNode;
        this.requestHandler = requestHandler;
        this.responseHandler = responseHandler;
        this.spellCheckQuery = requestHandler.getPropertyRestrictionQuery().replace(SPELLCHECK_PREFIX, "");
    }

    @Override
    public boolean hasNext() {
        if (!loaded) {
            loadSuggestions();
            loaded = true;
        }
        return internalIterator != null && internalIterator.hasNext();
    }

    @Override
    public FulltextResultRow next() {
        return internalIterator.next();
    }

    private void loadSuggestions() {
        try {
            final ArrayDeque<String> suggestionTexts = new ArrayDeque<>();
            MsearchRequest.Builder multiSearch = suggestions().map(s -> {
                        suggestionTexts.offer(s);
                        return requestHandler.suggestMatchQuery(s);
                    })
                    .map(query -> RequestItem.of(rib ->
                            rib.header(hb -> hb.index(String.join(",", indexNode.getDefinition().getIndexAlias())))
                                    .body(bb -> bb.query(qb -> qb.bool(query)).size(100))))
                    .reduce(
                            new MsearchRequest.Builder().index(String.join(",", indexNode.getDefinition().getIndexAlias())),
                            MsearchRequest.Builder::searches, (ms, ms2) -> ms);

            if (!suggestionTexts.isEmpty()) {
                MsearchResponse<ObjectNode> mSearchResponse = indexNode.getConnection().getClient()
                        .msearch(multiSearch.build(), ObjectNode.class);
                ArrayList<FulltextResultRow> results = new ArrayList<>();
                for (MultiSearchResponseItem<ObjectNode> r : mSearchResponse.responses()) {
                    for (Hit<ObjectNode> hit : r.result().hits().hits()) {
                        if (responseHandler.isAccessible(responseHandler.getPath(hit))) {
                            results.add(new FulltextResultRow(suggestionTexts.poll()));
                            break;
                        }
                    }
                }
                this.internalIterator = results.iterator();
            }
        } catch (IOException e) {
            LOG.error("Error processing suggestions for " + spellCheckQuery, e);
        }

    }

    /**
     * TODO: this query still uses the old RHLC because of this bug in the Elasticsearch Java client
     * <a href="https://github.com/elastic/elasticsearch-java/issues/214">https://github.com/elastic/elasticsearch-java/issues/214</a>
     * Migrate when resolved
     */
    private Stream<String> suggestions() throws IOException {
        final SearchRequest searchReq = SearchRequest.of(sr -> sr
                .index(String.join(",", indexNode.getDefinition().getIndexAlias()))
                .suggest(sb -> sb.text(spellCheckQuery)
                        .suggesters("oak:suggestion", fs -> fs.phrase(requestHandler.suggestQuery()))));

        String endpoint = "/" + String.join(",", searchReq.index()) + "/_search?filter_path=suggest";
        Request request = new Request("POST", endpoint);
        request.setJsonEntity(ElasticIndexUtils.toString(searchReq));

        Response searchRes = indexNode.getConnection().getOldClient().getLowLevelClient().performRequest(request);
        ObjectNode responseNode = JSON_MAPPER.readValue(searchRes.getEntity().getContent(), ObjectNode.class);

        return StreamSupport
                .stream(responseNode.get("suggest").get("oak:suggestion").spliterator(), false)
                .flatMap(node -> StreamSupport.stream(node.get("options").spliterator(), false))
                .map(node -> node.get("text").asText());
    }
}
