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

package org.apache.jackrabbit.oak.plugins.index;

import com.google.common.base.Joiner;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.*;
import static org.junit.Assert.*;

public abstract class RegexTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtils.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Test
    public void testNonException() throws Exception {
        Tree rootTree = root.getTree("/");

        Tree def = rootTree.addChild(INDEX_DEFINITIONS_NAME).addChild("regexTesting");
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType());
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        def.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());

        Tree properties = def.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE);

        Tree analyzedProp = properties.addChild("a");
        analyzedProp.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        Tree indexedProp = properties.addChild("b");
        indexedProp.setProperty(FulltextIndexConstants.PROP_INDEX, true);

        Tree facetsProp = properties.addChild("c");
        facetsProp.setProperty(FulltextIndexConstants.PROP_FACETS, true);

        Tree useInExcerptProp = properties.addChild("d");
        useInExcerptProp.setProperty(FulltextIndexConstants.PROP_USE_IN_EXCERPT, true);

        Tree notIndexedProp = properties.addChild("e");
        notIndexedProp.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        /*Tree allProps = properties.addChild("allProps");
        allProps.setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);
        allProps.setProperty(FulltextIndexConstants.PROP_NAME, FulltextIndexConstants.REGEX_ALL_PROPS);
        allProps.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        allProps.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        allProps.setProperty(FulltextIndexConstants.PROP_USE_IN_EXCERPT, true);*/

        root.commit();

        Tree content = root.getTree("/");
        content.addChild("a").setProperty("a", "ara ve nadal");
        content.addChild("b").setProperty("b", "ara");
        content.addChild("c").setProperty("c", "ve");
        content.addChild("d").setProperty("d", "nadal");
        content.addChild("e").setProperty("e", "menjarem turro");
        root.commit();

        List<String> columns = newArrayList("rep:excerpt", "rep:excerpt(.)", "rep:excerpt(foo)", "rep:excerpt(bar)");
        String selectColumns = Joiner.on(",").join(
                columns.stream().map(col -> "[" + col + "]").collect(Collectors.toList())
        );
        assertEventually(() -> {
            String query = "SELECT " + selectColumns + " FROM [nt:base] WHERE CONTAINS(*, 'fox')";
            try {
                Result result = executeQuery(query, SQL2, NO_BINDINGS);
                Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
                assertTrue(resultIter.hasNext());
                ResultRow firstRow = resultIter.next();

                PropertyValue excerptValue;
                String excerpt;

                for (String col : columns) {
                    excerptValue = firstRow.getValue(col);
                    assertNotNull(col + " not evaluated", excerptValue);
                    excerpt = excerptValue.getValue(STRING);
                    assertFalse(col + " didn't evaluate correctly - got '" + excerpt + "'",
                            excerpt.contains("i<strong>fox</foxing>ing"));
                }
            } catch (ParseException e) {
                fail(e.getMessage());
            }
        });
    }
}
