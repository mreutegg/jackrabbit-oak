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
package org.apache.jackrabbit.oak.jcr;

import javax.jcr.Node;
import javax.jcr.Session;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * MoveTest... TODO
 */
public class MoveTest extends AbstractJCRTest {

    private void move(String src, String dest, boolean save) throws Exception {
        superuser.move(src, dest);
        if (save) {
            superuser.save();
        }
    }

    @Test
    public void testRename() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        superuser.save();

        String destPath = testRoot + '/' + nodeName2;
        move(node1.getPath(), destPath, true);

        assertEquals(destPath, node1.getPath());
    }

    @Test
    public void testRenameNewNode() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);

        String destPath = testRoot + '/' + nodeName2;
        move(node1.getPath(), destPath, false);

        assertEquals(destPath, node1.getPath());
    }

    @Test
    public void testMove() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        Node node2 = testRootNode.addNode(nodeName2);
        superuser.save();

        String destPath = node2.getPath() + '/' + nodeName1;
        move(node1.getPath(), destPath, true);

        assertEquals(destPath, node1.getPath());
    }

    /**
     * Simulate a 'rename' call using 2 sessions:
     * - 1st create a node that has a '.tmp' extension 
     * - 2nd remove the '.tmp' by issuing a Session#move call on a fresh session
     */
    @Test
    @Ignore("OAK-898")
    public void testMoveTmp() throws Exception {
        String n = "testMoveTmp";

        Node node1 = testRootNode.addNode(n + ".tmp");
        superuser.save();

        String destPath = testRootNode.getPath() + "/" + n;

        Session ts = getHelper().getSuperuserSession();
        try {
            ts.move(node1.getPath(), destPath);
            assertEquals(destPath, node1.getPath());
        } finally {
            ts.logout();
        }
    }

    @Test
    public void testMoveReferenceable() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        node1.addMixin(JcrConstants.MIX_REFERENCEABLE);
        Node node2 = testRootNode.addNode(nodeName2);
        superuser.save();

        String destPath = node2.getPath() + '/' + nodeName1;
        move(node1.getPath(), destPath, true);

        assertEquals(destPath, node1.getPath());
    }

    @Test
    public void testMoveNewNode() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        Node node2 = testRootNode.addNode(nodeName2);

        String destPath = node2.getPath() + '/' + nodeName1;
        move(node1.getPath(), destPath, false);

        assertEquals(destPath, node1.getPath());
    }

    @Test
    public void testMoveNewReferenceable() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        node1.addMixin(JcrConstants.MIX_REFERENCEABLE);
        assertTrue(node1.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        Node node2 = testRootNode.addNode(nodeName2);

        String destPath = node2.getPath() + '/' + nodeName1;
        move(node1.getPath(), destPath, false);

        assertEquals(destPath, node1.getPath());

        superuser.save();
        assertEquals(destPath, node1.getPath());
    }
}