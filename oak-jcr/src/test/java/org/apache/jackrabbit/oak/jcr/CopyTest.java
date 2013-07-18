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
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CopyTest extends AbstractRepositoryTest {

    private static final String TEST_NODE = "test_node";
    private static final String TEST_PATH = '/' + TEST_NODE;

    private Node testNode;

    public CopyTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        testNode = root.addNode(TEST_NODE);
        testNode.addNode("source").addNode("node");
        testNode.addNode("target");
        session.save();
    }

    @After
    public void tearDown() throws RepositoryException {
        Session s = testNode.getSession();
        s.removeItem(TEST_PATH);
        s.save();
    }

    @Test
    public void testCopyNode() throws RepositoryException {
        Session session = getAdminSession();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(testNode.hasNode("source/node"));
        assertTrue(testNode.hasNode("target/copied"));
    }

    @Ignore("OAK-915") // FIXME
    @Test
    public void testCopyReferenceableNode() throws Exception {
        Session session = getAdminSession();

        Node toCopy = session.getNode(TEST_PATH + "/source/node");
        toCopy.addMixin(JcrConstants.MIX_REFERENCEABLE);
        session.save();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(testNode.hasNode("source/node"));
        assertTrue(testNode.hasNode("target/copied"));

        Node copy = testNode.getNode("target/copied");
        assertTrue(copy.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        assertFalse(copy.getUUID().equals(testNode.getNode("source/node").getUUID()));
    }

    @Ignore("OAK-915") // FIXME
    @Test
    public void testCopyReferenceableChildNode() throws Exception {
        Session session = getAdminSession();

        session.getNode(TEST_PATH + "/source/node").addNode("child").addMixin(JcrConstants.MIX_REFERENCEABLE);
        session.save();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(testNode.hasNode("source/node"));
        assertTrue(testNode.hasNode("target/copied"));

        Node childCopy = testNode.getNode("target/copied/child");
        assertTrue(childCopy.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        assertFalse(childCopy.getUUID().equals(testNode.getNode("source/node/child").getUUID()));
    }

    @Ignore("OAK-918") // FIXME
    @Test
    public void testCopyVersionableNode() throws Exception {
        Session session = getAdminSession();
        Node toCopy = session.getNode(TEST_PATH + "/source/node");
        toCopy.addMixin(JcrConstants.MIX_VERSIONABLE);
        session.save();

        Version baseV = toCopy.getBaseVersion();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(testNode.hasNode("source/node"));
        assertTrue(testNode.hasNode("target/copied"));

        Node copy = testNode.getNode("target/copied");

        assertTrue(copy.isNodeType(JcrConstants.MIX_VERSIONABLE));
        VersionHistory copiedVh = copy.getVersionHistory();
        assertFalse(copy.getVersionHistory().isSame(toCopy.getVersionHistory()));

        assertTrue(copiedVh.hasProperty(VersionConstants.JCR_COPIED_FROM));
        Node copiedFrom = copiedVh.getProperty(VersionConstants.JCR_COPIED_FROM).getNode();
        assertTrue(baseV.isSame(copiedFrom));
    }

    @Ignore("OAK-919") // FIXME
    @Test
    public void testCopyLockedNode() throws Exception {
        Session session = getAdminSession();
        Node toCopy = session.getNode(TEST_PATH + "/source/node");
        toCopy.addMixin(JcrConstants.MIX_LOCKABLE);
        session.save();

        session.getWorkspace().getLockManager().lock(toCopy.getPath(), true, true, Long.MAX_VALUE, "my");
        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(testNode.hasNode("source/node"));
        assertTrue(testNode.hasNode("target/copied"));

        Node copy = testNode.getNode("target/copied");

        assertFalse(copy.isNodeType(JcrConstants.MIX_LOCKABLE));
        assertFalse(copy.hasProperty(JcrConstants.JCR_LOCKISDEEP));
        assertFalse(copy.hasProperty(JcrConstants.JCR_LOCKOWNER));
    }
}