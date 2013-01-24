/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mongomk.impl.action;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.NodeAssert;
import org.apache.jackrabbit.mongomk.impl.command.CommitCommand;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.apache.jackrabbit.mongomk.util.NodeBuilder;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class FetchNodesActionTest extends BaseMongoMicroKernelTest {

    @Test
    public void invalidFirstRevision() throws Exception {
        Long revisionId1 = addNode("a");
        Long revisionId2 = addNode("b");
        Long revisionId3 = addNode("c");

        invalidateCommit(revisionId1);
        updateBaseRevisionId(revisionId2, 0L);

        List<Node> actuals = createAndExecuteQuery(revisionId3);

        String json = String.format("{\"/#%2$s\" : { \"b#%1$s\" : {}, \"c#%2$s\" : {} }}",
                revisionId2, revisionId3);
        Iterator<Node> expecteds = NodeBuilder.build(json).getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void invalidLastRevision() throws Exception {
        addNode("a");
        addNode("b");
        Long revisionId3 = addNode("c");

        invalidateCommit(revisionId3);
        try {
            createAndExecuteQuery(revisionId3);
            fail("Expected MicroKernelException");
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void invalidMiddleRevision() throws Exception {
        Long revisionId1 = addNode("a");
        Long revisionId2 = addNode("b");
        Long revisionId3 = addNode("c");

        invalidateCommit(revisionId2);
        updateBaseRevisionId(revisionId3, revisionId1);
        List<Node> actuals = createAndExecuteQuery(revisionId3);

        String json = String.format("{\"/#%2$s\" : { \"a#%1$s\" : {}, \"c#%2$s\" : {} }}",
                revisionId1, revisionId3);
        Iterator<Node> expecteds = NodeBuilder.build(json).getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void samePrefix() throws Exception {
        addNode("a");
        addNode("a/b");
        addNode("a/bb");
        long rev = addNode("a/b/c");

        int depth = 0;
        FetchNodesAction action = new FetchNodesAction(getNodeStore(),
                "/a/b", depth, rev);
        Map<String, MongoNode> nodes = action.execute();
        assertEquals(1, nodes.size());
        assertNotNull(nodes.get("/a/b"));

        depth = 1;
        action = new FetchNodesAction(getNodeStore(), "/a/b", depth, rev);
        nodes = action.execute();
        assertEquals(2, nodes.size());
        assertNotNull(nodes.get("/a/b"));
        assertNotNull(nodes.get("/a/b/c"));

        depth = FetchNodesAction.LIMITLESS_DEPTH;
        action = new FetchNodesAction(getNodeStore(), "/a/b", depth, rev);
        nodes = action.execute();
        assertEquals(2, nodes.size());
        assertNotNull(nodes.get("/a/b"));
        assertNotNull(nodes.get("/a/b/c"));
        assertNull(nodes.get("/a/bb"));
    }

    // FIXME - Revisit this test.
    @Test
    public void fetchRootAndAllDepths() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(getNodeStore());
        Long firstRevisionId = scenario.create();
        Long secondRevisionId = scenario.update_A_and_add_D_and_E();

        List<Node> actuals = createAndExecuteQuery(firstRevisionId, 0);
        String json = String.format("{ \"/#%1$s\" : {} }", firstRevisionId);
        Node expected = NodeBuilder.build(json);
        Iterator<Node> expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        actuals = createAndExecuteQuery(secondRevisionId, 0);
        json = String.format("{ \"/#%1$s\" : {} }", firstRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        actuals = createAndExecuteQuery(firstRevisionId, 1);
        json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 } } }", firstRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        actuals = createAndExecuteQuery(secondRevisionId, 1);
        json = String.format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 } } }",
                firstRevisionId, secondRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        actuals = createAndExecuteQuery(firstRevisionId, 2);
        json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1, \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                firstRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        actuals = createAndExecuteQuery(secondRevisionId, 2);
        json = String.format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                firstRevisionId, secondRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        actuals = createAndExecuteQuery(firstRevisionId);
        json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                firstRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        actuals = createAndExecuteQuery(secondRevisionId);
        json = String.format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\", \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                firstRevisionId, secondRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    private Long addNode(String nodeName) throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"" + nodeName + "\" : {}", "Add /" + nodeName);
        CommitCommand command = new CommitCommand(getNodeStore(), commit);
        return command.execute();
    }

    @Test
    public void fetchWithCertainPathsOneRevision() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(getNodeStore());
        Long revisionId = scenario.create();

        List<Node> actuals = createAndExecuteQuery(revisionId, getPathSet("/a", "/a/b", "/a/c", "not_existing"));
        String json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                revisionId);
        Node expected = NodeBuilder.build(json);
        Iterator<Node> expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        actuals = createAndExecuteQuery(revisionId, getPathSet("/a", "not_existing"));
        json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 } } }",
                revisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void fetchWithCertainPathsTwoRevisions() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(getNodeStore());
        Long firstRevisionId = scenario.create();
        Long secondRevisionId = scenario.update_A_and_add_D_and_E();

        List<Node> actuals = createAndExecuteQuery(firstRevisionId, getPathSet("/a", "/a/b", "/a/c", "/a/d", "/a/b/e", "not_existing"));
        String json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                firstRevisionId);
        Node expected = NodeBuilder.build(json);
        Iterator<Node> expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        actuals = createAndExecuteQuery(secondRevisionId, getPathSet("/a", "/a/b", "/a/c", "/a/d", "/a/b/e", "not_existing"));
        json = String.format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                firstRevisionId, secondRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    private List<Node> createAndExecuteQuery(long revisionId) {
        return createAndExecuteQuery(revisionId, -1);
    }

    private List<Node> createAndExecuteQuery(long revisionId, int depth) {
        Set<String> paths = new HashSet<String>();
        paths.add("/");
        return createAndExecuteQuery(revisionId, paths, depth);
    }

    private List<Node> createAndExecuteQuery(long revisionId, Set<String> paths) {
        return createAndExecuteQuery(revisionId, paths, -1);
    }

    private List<Node> createAndExecuteQuery(long revisionId, Set<String> paths, int depth) {
        FetchNodesAction query = new FetchNodesAction(getNodeStore(), paths, revisionId);
        return toNode(query.execute());
    }

    private Set<String> getPathSet(String... paths) {
        return new HashSet<String>(Arrays.asList(paths));
    }

    private void invalidateCommit(Long revisionId) {
        DBCollection commitCollection = getNodeStore().getCommitCollection();
        DBObject query = QueryBuilder.start(MongoCommit.KEY_REVISION_ID)
                .is(revisionId).get();
        DBObject update = new BasicDBObject();
        update.put("$set", new BasicDBObject(MongoCommit.KEY_FAILED, Boolean.TRUE));
        commitCollection.update(query, update);
    }

    private void updateBaseRevisionId(Long revisionId2, Long baseRevisionId) {
        DBCollection commitCollection = getNodeStore().getCommitCollection();
        DBObject query = QueryBuilder.start(MongoCommit.KEY_REVISION_ID)
                .is(revisionId2)
                .get();
        DBObject update = new BasicDBObject("$set",
                new BasicDBObject(MongoCommit.KEY_BASE_REVISION_ID, baseRevisionId));
        commitCollection.update(query, update);
    }

    private List<Node> toNode(Map<String, MongoNode> nodeMongos) {
        List<Node> nodes = new ArrayList<Node>(nodeMongos.size());
        for (MongoNode nodeMongo : nodeMongos.values()) {
            Node node = MongoNode.toNode(nodeMongo);
            nodes.add(node);
        }

        return nodes;
    }
}
