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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.core.AbstractOakTest;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitEditor;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class KernelNodeStoreTest extends AbstractOakTest {

    private NodeState root;

    @Override
    protected NodeState createInitialState(MicroKernel microKernel) {
        String jsop =
                "+\"test\":{\"a\":1,\"b\":2,\"c\":3,"
                        + "\"x\":{},\"y\":{},\"z\":{}}";
        microKernel.commit("/", jsop, null, "test data");
        root = store.getRoot();
        return root;
    }

    @Test
    public void getRoot() {
        assertEquals(root, store.getRoot());
        assertEquals(root.getChildNode("test"), store.getRoot().getChildNode("test"));
        assertEquals(root.getChildNode("test").getChildNode("x"),
                store.getRoot().getChildNode("test").getChildNode("x"));
        assertEquals(root.getChildNode("test").getChildNode("any"),
                store.getRoot().getChildNode("test").getChildNode("any"));
        assertEquals(root.getChildNode("test").getProperty("a"),
                store.getRoot().getChildNode("test").getProperty("a"));
        assertEquals(root.getChildNode("test").getProperty("any"),
                store.getRoot().getChildNode("test").getProperty("any"));
    }

    @Test
    public void branch() throws CommitFailedException {
        NodeStoreBranch branch = store.branch();

        NodeStateBuilder rootBuilder = store.getBuilder(branch.getRoot());
        NodeStateBuilder testBuilder = store.getBuilder(root.getChildNode("test"));

        testBuilder.setNode("newNode", MemoryNodeState.EMPTY_NODE);
        testBuilder.removeNode("x");

        NodeStateBuilder newNodeBuilder = store.getBuilder(
                testBuilder.getNodeState().getChildNode("newNode"));

        CoreValue fortyTwo = store.getValueFactory().createValue(42);
        newNodeBuilder.setProperty("n", fortyTwo);

        testBuilder.setNode("newNode", newNodeBuilder.getNodeState());
        rootBuilder.setNode("test", testBuilder.getNodeState());

        // Assert changes are present in the builder
        NodeState testState = rootBuilder.getNodeState().getChildNode("test");
        assertNotNull(testState.getChildNode("newNode"));
        assertNull(testState.getChildNode("x"));
        assertEquals(fortyTwo, testState.getChildNode("newNode").getProperty("n").getValue());

        // Assert changes are not yet present in the branch
        testState = branch.getRoot().getChildNode("test");
        assertNull(testState.getChildNode("newNode"));
        assertNotNull(testState.getChildNode("x"));

        branch.setRoot(rootBuilder.getNodeState());

        // Assert changes are present in the branch
        testState = branch.getRoot().getChildNode("test");
        assertNotNull(testState.getChildNode("newNode"));
        assertNull(testState.getChildNode("x"));
        assertEquals(fortyTwo, testState.getChildNode("newNode").getProperty("n").getValue());

        // Assert changes are not yet present in the trunk
        testState = store.getRoot().getChildNode("test");
        assertNull(testState.getChildNode("newNode"));
        assertNotNull(testState.getChildNode("x"));

        branch.merge();

        // Assert changes are present in the trunk
        testState = store.getRoot().getChildNode("test");
        assertNotNull(testState.getChildNode("newNode"));
        assertNull(testState.getChildNode("x"));
        assertEquals(fortyTwo, testState.getChildNode("newNode").getProperty("n").getValue());
    }

    @Test
    public void afterCommitHook() throws CommitFailedException {
        final NodeState[] states = new NodeState[2]; // { before, after }
        store.setObserver(new Observer() {
            @Override
            public void contentChanged(
                    NodeStore store, NodeState before, NodeState after) {
                states[0] = before;
                states[1] = after;
            }
        });

        NodeState root = store.getRoot();
        NodeStateBuilder rootBuilder= store.getBuilder(root);

        NodeState test = root.getChildNode("test");
        NodeStateBuilder testBuilder = store.getBuilder(test);

        NodeStateBuilder newNodeBuilder = store.getBuilder(MemoryNodeState.EMPTY_NODE);
        CoreValue fortyTwo = store.getValueFactory().createValue(42);
        newNodeBuilder.setProperty("n", fortyTwo);

        testBuilder.setNode("newNode", newNodeBuilder.getNodeState());
        testBuilder.removeNode("a");

        rootBuilder.setNode("test", testBuilder.getNodeState());
        NodeState newRoot = rootBuilder.getNodeState();

        NodeStoreBranch branch = store.branch();
        branch.setRoot(newRoot);
        branch.merge();
        store.getRoot(); // triggers the observer

        NodeState before = states[0];
        NodeState after = states[1];
        assertNotNull(before);
        assertNotNull(after);

        assertNull(before.getChildNode("test").getChildNode("newNode"));
        assertNotNull(after.getChildNode("test").getChildNode("newNode"));
        assertNull(after.getChildNode("test").getChildNode("a"));
        assertEquals(fortyTwo, after.getChildNode("test").getChildNode("newNode").getProperty("n").getValue());
        assertEquals(newRoot, after);
    }

    @Test
    public void beforeCommitHook() throws CommitFailedException {
        store.setEditor(new CommitEditor() {
            @Override
            public NodeState editCommit(
                    NodeStore store, NodeState before, NodeState after) {
                NodeStateBuilder rootBuilder = store.getBuilder(after);
                NodeStateBuilder testBuilder = store.getBuilder(after.getChildNode("test"));
                testBuilder.setNode("fromHook", MemoryNodeState.EMPTY_NODE);
                rootBuilder.setNode("test", testBuilder.getNodeState());
                return rootBuilder.getNodeState();
            }
        });

        NodeState root = store.getRoot();
        NodeStateBuilder rootBuilder = store.getBuilder(root);

        NodeState test = root.getChildNode("test");
        NodeStateBuilder testBuilder = store.getBuilder(test);

        NodeStateBuilder newNodeBuilder = store.getBuilder(MemoryNodeState.EMPTY_NODE);
        final CoreValue fortyTwo = store.getValueFactory().createValue(42);
        newNodeBuilder.setProperty("n", fortyTwo);

        testBuilder.setNode("newNode", newNodeBuilder.getNodeState());
        testBuilder.removeNode("a");

        rootBuilder.setNode("test", testBuilder.getNodeState());
        NodeState newRoot = rootBuilder.getNodeState();

        NodeStoreBranch branch = store.branch();
        branch.setRoot(newRoot);
        branch.merge();

        test = store.getRoot().getChildNode("test");
        assertNotNull(test.getChildNode("newNode"));
        assertNotNull(test.getChildNode("fromHook"));
        assertNull(test.getChildNode("a"));
        assertEquals(fortyTwo, test.getChildNode("newNode").getProperty("n").getValue());
        assertEquals(test, store.getRoot().getChildNode("test"));
    }

}
