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
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public abstract class ExcerptTest3 extends AbstractQueryTest {
    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected String getIndexProvider() {
        return "lucene:";
    }

    protected void postCommitHook(){
        // does nothing by default
    }

    @Test
    public void getAllSelectedColumns() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "is fox ifoxing");
        contentRoot.setProperty("bar", "ifoxing fox");
        contentRoot.setProperty("baz", "fox ifoxing");
        root.commit();

        List<String> columns = newArrayList("rep:excerpt", "rep:excerpt(.)", "rep:excerpt(foo)", "rep:excerpt(bar)");
        String selectColumns = Joiner.on(",").join(
                columns.stream().map(col -> "[" + col + "]").collect(Collectors.toList())
        );
        String query = "SELECT " + selectColumns + " FROM [nt:base] WHERE CONTAINS(*, 'fox')";
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
    }

    @Test
    public void nodeExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "is fox ifoxing");
        contentRoot.setProperty("bar", "ifoxing fox");
        root.commit();

        String query = "SELECT [rep:excerpt],[rep:excerpt(.)] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        Result result = executeQuery(query, SQL2, NO_BINDINGS);
        Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
        assertTrue(resultIter.hasNext());
        ResultRow firstRow = resultIter.next();

        PropertyValue nodeExcerpt;
        String excerpt1, excerpt2;


        nodeExcerpt = firstRow.getValue("rep:excerpt");
        assertNotNull("rep:excerpt not evaluated", nodeExcerpt);
        excerpt1 = nodeExcerpt.getValue(STRING);
        assertTrue("rep:excerpt didn't evaluate correctly - got '" + excerpt1 + "'",
                "is <strong>fox</strong> ifoxing".equals(excerpt1) || "ifoxing <strong>fox</strong>".equals(excerpt1));

        nodeExcerpt = firstRow.getValue("rep:excerpt(.)");
        assertNotNull("rep:excerpt(.) not evaluated", nodeExcerpt);
        excerpt2 = nodeExcerpt.getValue(STRING);
        assertEquals("excerpt extracted via rep:excerpt not same as rep:excerpt(.)", excerpt1, excerpt2);
    }

    @Test
    public void nonIndexedRequestedPropExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "fox");
        contentRoot.setProperty("baz", "is fox ifoxing");
        root.commit();

        String query = "SELECT [rep:excerpt(baz)] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        Result result = executeQuery(query, SQL2, NO_BINDINGS);
        Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
        assertTrue(resultIter.hasNext());
        ResultRow firstRow = resultIter.next();

        PropertyValue nodeExcerpt = firstRow.getValue("rep:excerpt(baz)");
        assertNull("rep:excerpt(baz) if requested explicitly must be indexed to be evaluated", nodeExcerpt);
    }

    @Test
    public void propExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "is fox ifoxing");
        root.commit();

        String query = "SELECT [rep:excerpt(foo)] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        Result result = executeQuery(query, SQL2, NO_BINDINGS);
        Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
        assertTrue(resultIter.hasNext());
        ResultRow firstRow = resultIter.next();

        PropertyValue nodeExcerpt;
        String excerpt;

        nodeExcerpt = firstRow.getValue("rep:excerpt(foo)");
        assertNotNull("rep:excerpt(foo) not evaluated", nodeExcerpt);
        excerpt = nodeExcerpt.getValue(STRING);
        assertTrue("rep:excerpt(foo) didn't evaluate correctly - got '" + excerpt + "'",
                "is <strong>fox</strong> ifoxing".equals(excerpt));
    }

    @Test
    public void indexedNonRequestedPropExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "is fox ifoxing");
        root.commit();

        String query = "SELECT [rep:excerpt] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        Result result = executeQuery(query, SQL2, NO_BINDINGS);
        Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
        assertTrue(resultIter.hasNext());
        ResultRow firstRow = resultIter.next();

        PropertyValue nodeExcerpt;
        String excerpt;

        nodeExcerpt = firstRow.getValue("rep:excerpt(foo)");
        assertNotNull("rep:excerpt(foo) not evaluated", nodeExcerpt);
        excerpt = nodeExcerpt.getValue(STRING);
        assertTrue("rep:excerpt(foo) didn't evaluate correctly - got '" + excerpt + "'",
                excerpt.contains("<strong>fox</strong>"));

        assertTrue("rep:excerpt(foo) highlighting inside words - got '" + excerpt + "'",
                !excerpt.contains("i<strong>fox</strong>ing"));
    }

    @Test
    public void nonIndexedNonRequestedPropExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "fox");
        contentRoot.setProperty("baz", "is fox ifoxing");
        root.commit();

        String query = "SELECT [rep:excerpt] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        Result result = executeQuery(query, SQL2, NO_BINDINGS);
        Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
        assertTrue(resultIter.hasNext());
        ResultRow firstRow = resultIter.next();

        PropertyValue nodeExcerpt;
        String excerpt;

        nodeExcerpt = firstRow.getValue("rep:excerpt(baz)");
        assertNotNull("rep:excerpt(baz) not evaluated", nodeExcerpt);
        excerpt = nodeExcerpt.getValue(STRING);
        assertTrue("rep:excerpt(foo) didn't evaluate correctly - got '" + excerpt + "'",
                excerpt.contains("<strong>fox</strong>"));

        assertTrue("rep:excerpt(baz) highlighting inside words - got '" + excerpt + "'",
                !excerpt.contains("i<strong>fox</strong>ing"));
    }

    @Test
    public void relativePropExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.addChild("relative").setProperty("baz", "is fox ifoxing");
        root.commit();

        String query = "SELECT [rep:excerpt(relative/baz)] FROM [nt:base] WHERE CONTAINS([relative/baz], 'fox')";
        Result result = executeQuery(query, SQL2, NO_BINDINGS);
        Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
        assertTrue(resultIter.hasNext());
        ResultRow firstRow = resultIter.next();

        PropertyValue nodeExcerpt;
        String excerpt;

        nodeExcerpt = firstRow.getValue("rep:excerpt(relative/baz)");
        assertNotNull("rep:excerpt(relative/baz) not evaluated", nodeExcerpt);
        excerpt = nodeExcerpt.getValue(STRING);
        assertTrue("rep:excerpt(relative/baz) didn't evaluate correctly - got '" + excerpt + "'",
                "is <strong>fox</strong> ifoxing".equals(excerpt));
    }

    @Test
    public void binaryExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");

        String binaryText = "is fox foxing as a fox cub";
        Blob blob = new ArrayBasedBlob(binaryText.getBytes());
        TestUtil.createFileNode(contentRoot, "binaryNode", blob, "text/plain");
        root.commit();

        String query = "SELECT [rep:excerpt] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        Result result = executeQuery(query, SQL2, NO_BINDINGS);
        Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
        assertTrue(resultIter.hasNext());
        ResultRow firstRow = resultIter.next();

        PropertyValue nodeExcerpt;
        String excerpt;

        nodeExcerpt = firstRow.getValue("rep:excerpt");
        assertNotNull("rep:excerpt not evaluated", nodeExcerpt);
        excerpt = nodeExcerpt.getValue(STRING);
        String expected = binaryText.replaceAll(" fox ", " <strong>fox</strong> ");
        assertTrue("rep:excerpt didn't evaluate correctly - got '" + excerpt + "'",
                excerpt.contains(expected));
    }

    protected String explain(String query) {
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    protected String explainXpath(String query) throws ParseException {
        String explain = "explain " + query;
        Result result = executeQuery(explain, "xpath", NO_BINDINGS);
        ResultRow row = Iterables.getOnlyElement(result.getRows());
        String plan = row.getValue("plan").getValue(Type.STRING);
        return plan;
    }

    private void assertOrderedPlanAndQuery(String query, String planExpectation, List<String> paths) {
        List<String> result = assertPlanAndQuery(query, planExpectation, paths);
        assertEquals("Ordering doesn't match", paths, result);
    }

    private List<String> assertPlanAndQuery(String query, String planExpectation, List<String> paths) {
        assertThat(explain(query), containsString(planExpectation));
        return assertQuery(query, paths);
    }

    protected Tree createIndex(String name, Set<String> propNames) {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    abstract protected Tree createIndex(Tree index, String name, Set<String> propNames);

    abstract protected String getLoggerName();
}
