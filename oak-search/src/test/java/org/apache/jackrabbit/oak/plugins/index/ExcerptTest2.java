package org.apache.jackrabbit.oak.plugins.index;

import com.google.common.base.Joiner;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.query.AbstractJcrTest;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.query.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.*;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.junit.Assert.*;

public abstract class ExcerptTest2 extends AbstractJcrTest {

    protected TestRepository repositoryOptionsUtil;
    protected Node indexNode;
    protected IndexOptions indexOptions;

    protected JackrabbitSession session = null;
    protected Node root = null;

    @Override
    protected void initialize() {
        session = (JackrabbitSession) adminSession;
        try {
            root = adminSession.getRootNode();
            createContent();
            adminSession.save();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private void createContent() throws Exception {
        Node def = JcrUtils.getOrAddNode(root, INDEX_DEFINITIONS_NAME).addNode("testExcerpt", INDEX_DEFINITIONS_NODE_TYPE);
        def.setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType());
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        def.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());

        Node properties = def.addNode(FulltextIndexConstants.INDEX_RULES)
                .addNode("nt:base")
                .addNode(FulltextIndexConstants.PROP_NODE);

        Node notIndexedProp = properties.addNode("baz");
        notIndexedProp.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        Node relativeProp = properties.addNode("relative-baz");
        relativeProp.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        relativeProp.setProperty(FulltextIndexConstants.PROP_USE_IN_EXCERPT, true);
        relativeProp.setProperty(FulltextIndexConstants.PROP_NAME, "relative/baz");

        Node allProps = properties.addNode("allProps");
        allProps.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        allProps.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        allProps.setProperty(FulltextIndexConstants.PROP_USE_IN_EXCERPT, true);
        allProps.setProperty(FulltextIndexConstants.PROP_NAME, FulltextIndexConstants.REGEX_ALL_PROPS);
        allProps.setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);
    }
    private Map<String,String> getExcerpts(String query) throws Exception {
        QueryManager queryManager = session.getWorkspace().getQueryManager();
        QueryResult result = queryManager.createQuery(query, Query.JCR_SQL2).execute();

        Map<String,String> excerpts = new HashMap<String,String>();
        RowIterator it = result.getRows();
        if(it.hasNext()){
            Row row = it.nextRow();
            for(String column: result.getColumnNames()){
                excerpts.put(column, row.getValue(column).getString());
            }
        }
        return excerpts;
    }

    protected void validateExcerpts(String query, Map<String,String> expected) {
        assertEventually(() -> {
            Map<String,String> excerpts;
            try {
                excerpts = getExcerpts(query);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            assertEquals("Incorrect excerpts", expected, excerpts);
        });
    }

    @Test
    public void getAllSelectedColumns() throws Exception {
        Node contentRoot = root.addNode("testRoot");
        contentRoot.setProperty("foo", "is fox ifoxing");
        contentRoot.setProperty("bar", "ifoxing fox");
        contentRoot.setProperty("baz", "fox ifoxing");
        adminSession.save();

        Set<String> columns = newHashSet("rep:excerpt", "rep:excerpt(.)", "rep:excerpt(foo)", "rep:excerpt(bar)");
        String selectColumns = Joiner.on(",").join(
                columns.stream().map(col -> "[" + col + "]").collect(Collectors.toSet())
        );
        String query = "SELECT " + selectColumns + " FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        Map<String, String> expectedExcerpts = new HashMap<String, String>(4);
        expectedExcerpts.put("rep:excerpt", "is <strong>fox</strong> ifoxing...ifoxing <strong>fox</strong>");
        expectedExcerpts.put("rep:excerpt(.)", "is <strong>fox</strong> ifoxing...ifoxing <strong>fox</strong>");
        expectedExcerpts.put("rep:excerpt(foo)", "is <strong>fox</strong> ifoxing");
        expectedExcerpts.put("rep:excerpt(bar)", "ifoxing <strong>fox</strong>");
        validateExcerpts(query, expectedExcerpts);
    }

    private static void assertEventually(Runnable r) {
        TestUtils.assertEventually(r, 3000 * 3);
    }
}
