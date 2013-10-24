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

package org.apache.jackrabbit.oak.spi.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.junit.Before;
import org.junit.Test;

public class MoveDetectorTest {
    private NodeState root;

    @Before
    public void setup() {
        NodeBuilder rootBuilder = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder test = rootBuilder.child("test");
        test.setProperty("a", 1);
        test.setProperty("b", 2);
        test.setProperty("c", 3);
        test.child("x");
        test.child("y");
        test.child("z");
        root = rootBuilder.getNodeState();
    }

    /**
     * Test whether we can detect a single move
     * @throws CommitFailedException
     */
    @Test
    public void simpleMove() throws CommitFailedException {
        NodeState moved = move(root.builder(), "/test/x", "/test/y/xx").getNodeState();
        FindSingleMove findSingleMove = new FindSingleMove("/test/x", "/test/y/xx");
        MoveDetector moveDetector = new MoveDetector(findSingleMove);
        CommitFailedException exception = EditorDiff.process(moveDetector, root, moved);
        if (exception != null) {
            throw exception;
        }
        assertTrue(findSingleMove.found());
    }

    /**
     * Moving a moved node is reported as a single move from the original source
     * to the final destination.
     * @throws CommitFailedException
     */
    @Test
    public void moveMoved() throws CommitFailedException {
        NodeBuilder rootBuilder = root.builder();
        move(rootBuilder, "/test/x", "/test/y/xx");
        NodeState moved = move(rootBuilder, "/test/y/xx", "/test/z/xxx").getNodeState();
        FindSingleMove findSingleMove = new FindSingleMove("/test/x", "/test/z/xxx");
        MoveDetector moveDetector = new MoveDetector(findSingleMove);
        CommitFailedException exception = EditorDiff.process(moveDetector, root, moved);
        if (exception != null) {
            throw exception;
        }
        assertTrue(findSingleMove.found());
    }

    //------------------------------------------------------------< private >---

    private static NodeBuilder move(NodeBuilder builder, String source, String dest) {
        NodeBuilder sourceBuilder = getBuilder(builder, source);
        NodeBuilder destParentBuilder = getBuilder(builder, PathUtils.getParentPath(dest));
        sourceBuilder.moveTo(destParentBuilder, PathUtils.getName(dest));
        return builder;
    }

    private static NodeBuilder getBuilder(NodeBuilder builder, String path) {
        for (String name : PathUtils.elements(path)) {
            builder = builder.getChildNode(name);
        }
        return builder;
    }

    private static class FindSingleMove implements MoveValidator {
        private final String sourcePath;
        private final String destPath;

        private boolean found;

        private FindSingleMove(String sourcePath, String destPath) {
            this.sourcePath = sourcePath;
            this.destPath = destPath;
        }

        @Override
        public void move(String sourcePath, String destPath, NodeState moved) throws CommitFailedException {
            if (found) {
                throw new CommitFailedException("Test", 0, "There should only be a single move operation");
            }

            assertEquals(this.sourcePath, sourcePath);
            assertEquals(this.destPath, destPath);
            found = true;
        }

        @Override
        public void enter(NodeState before, NodeState after) throws CommitFailedException {
        }

        @Override
        public void leave(NodeState before, NodeState after) throws CommitFailedException {
        }

        @Override
        public void propertyAdded(PropertyState after) {
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
        }

        @Override
        public void propertyDeleted(PropertyState before) {
        }

        @Override
        public MoveValidator childNodeAdded(String name, NodeState after) {
            return null;
        }

        @Override
        public MoveValidator childNodeChanged(String name, NodeState before, NodeState after) {
            return this;
        }

        @Override
        public MoveValidator childNodeDeleted(String name, NodeState before) {
            return null;
        }

        public boolean found() {
            return found;
        }
    }

}
