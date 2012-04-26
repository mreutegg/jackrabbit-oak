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
package org.apache.jackrabbit.mk.test;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class MicroKernelIT extends AbstractMicroKernelIT {

    public MicroKernelIT(MicroKernelFixture fixture) {
        super(fixture, 1);
    }

    @Override
    protected void addInitialTestContent() {
        mk.commit("/", "+\"test\" : {" +
                "\"stringProp\":\"stringVal\"," +
                "\"intProp\":42," +
                "\"floatProp\":42.2," +
                "\"booleanProp\": true," +
                "\"multiIntProp\":[1,2,3]}", null, "");
    }

    @Test
    public void revisionOps() {
        String head = mk.getHeadRevision();
        assertNotNull(head);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) {
        }

        long now = System.currentTimeMillis();

        // get history since 'now'
        JSONArray array = parseJSONArray(mk.getRevisionHistory(now, -1));
        // history should be empty since there was no commit since 'now'
        assertEquals(0, array.size());

        // get oldest available revision
        array = parseJSONArray(mk.getRevisionHistory(0, 1));
        // there should be exactly 1 revision
        assertEquals(array.size(), 1);

        long ts0 = System.currentTimeMillis();

        final int NUM_COMMITS = 100;

        // perform NUM_COMMITS commits
        for (int i = 0; i < NUM_COMMITS; i++) {
            mk.commit("/test", "+\"child" + i + "\":{}", null, "commit#" + i);
        }

        // get oldest available revision
        array = parseJSONArray(mk.getRevisionHistory(ts0, -1));
        // there should be exactly NUM_COMMITS revisions
        assertEquals(array.size(), NUM_COMMITS);
        long previousTS = ts0;
        for (int i = 0; i < NUM_COMMITS; i++) {
            JSONObject rev = getObjectArrayEntry(array, i);
            assertPropertyExists(rev, "id", String.class);
            assertPropertyExists(rev, "ts", Long.class);
            // verify commit msg
            assertPropertyValue(rev, "msg", "commit#" + i);
            // verify chronological order
            long ts = (Long) resolveValue(rev, "ts");
            assertTrue(previousTS <= ts);
            previousTS = ts;
        }

        // last revision should be the current head revision
        assertPropertyValue(getObjectArrayEntry(array, array.size() - 1), "id", mk.getHeadRevision());

        String fromRev = (String) resolveValue(getObjectArrayEntry(array, 0), "id");
        String toRev = (String) resolveValue(getObjectArrayEntry(array, array.size() - 1), "id");

        // verify journal
        array = parseJSONArray(mk.getJournal(fromRev, toRev, ""));
        // there should be exactly NUM_COMMITS entries
        assertEquals(array.size(), NUM_COMMITS);
        // verify that 1st and last rev match fromRev and toRev
        assertPropertyValue(getObjectArrayEntry(array, 0), "id", fromRev);
        assertPropertyValue(getObjectArrayEntry(array, array.size() - 1), "id", toRev);

        previousTS = ts0;
        for (int i = 0; i < NUM_COMMITS; i++) {
            JSONObject rev = getObjectArrayEntry(array, i);
            assertPropertyExists(rev, "id", String.class);
            assertPropertyExists(rev, "ts", Long.class);
            assertPropertyExists(rev, "changes", String.class);
            // TODO verify json diff
            // verify commit msg
            assertPropertyValue(rev, "msg", "commit#" + i);
            // verify chronological order
            long ts = (Long) resolveValue(rev, "ts");
            assertTrue(previousTS <= ts);
            previousTS = ts;
        }

        // test with 'negative' range (switch from and to)
        array = parseJSONArray(mk.getJournal(toRev, fromRev, ""));
        // there should be exactly 0 entries
        assertEquals(array.size(), 0);
    }

    @Test
    public void addAndMove() {
        String head = mk.getHeadRevision();
        head = mk.commit("",
                "+\"/root\":{}\n" +
                "+\"/root/a\":{}\n" +
                "",
                head, "");

        head = mk.commit("",
                "+\"/root/a/b\":{}\n" +
                ">\"/root/a\":\"/root/c\"\n" +
                "",
                head, "");

        assertFalse(mk.nodeExists("/root/a", head));
    }

    @Test
    public void getNodes() {
        String head = mk.getHeadRevision();

        // verify initial content
        JSONObject obj = parseJSONObject(mk.getNodes("/", head, 1, 0, -1, null));
        assertPropertyValue(obj, "test/stringProp", "stringVal");
        assertPropertyValue(obj, "test/intProp", 42L);
        assertPropertyValue(obj, "test/floatProp", 42.2);
        assertPropertyValue(obj, "test/booleanProp", true);
        assertPropertyValue(obj, "test/multiIntProp", new Object[]{1, 2, 3});
    }

    @Test
    public void getNodesNonExistingPath() {
        String head = mk.getHeadRevision();

        String nonExistingPath = "/test/" + System.currentTimeMillis();
        assertFalse(mk.nodeExists(nonExistingPath, head));

        assertNull(mk.getNodes(nonExistingPath, head, 0, 0, -1, null));
    }

    @Test
    public void getNodesNonExistingRevision() {
        String nonExistingRev = "12345678";

        try {
            mk.nodeExists("/test", nonExistingRev);
            fail("Success with non-existing revision: " + nonExistingRev);
        } catch (MicroKernelException e) {
            // expected
        }

        try {
            mk.getNodes("/test", nonExistingRev, 0, 0, -1, null);
            fail("Success with non-existing revision: " + nonExistingRev);
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void missingName() {
        String head = mk.getHeadRevision();

        assertTrue(mk.nodeExists("/test", head));
        try {
            String path = "/test/";
            mk.getNodes(path, head);
            fail("Success with invalid path: " + path);
        } catch (IllegalArgumentException e) {
            // expected
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void addNodeWithRelativePath() {
        String head = mk.getHeadRevision();

        head = mk.commit("/", "+\"foo\" : {} \n+\"foo/bar\" : {}", head, "");
        assertTrue(mk.nodeExists("/foo", head));
        assertTrue(mk.nodeExists("/foo/bar", head));
    }

    @Test
    public void commitWithEmptyPath() {
        String head = mk.getHeadRevision();

        head = mk.commit("", "+\"/ene\" : {}\n+\"/ene/mene\" : {}\n+\"/ene/mene/muh\" : {}", head, "");
        assertTrue(mk.nodeExists("/ene/mene/muh", head));
    }

    @Test
    public void addPropertyWithRelativePath() {
        String head = mk.getHeadRevision();

        head = mk.commit("/",
                "+\"fuu\" : {} \n" +
                        "^\"fuu/bar\" : 42", head, "");
        JSONObject obj = parseJSONObject(mk.getNodes("/fuu", head));
        assertPropertyValue(obj, "bar", 42L);
    }

    @Test
    public void addMultipleNodes() {
        String head = mk.getHeadRevision();

        long millis = System.currentTimeMillis();
        String node1 = "n1_" + millis;
        String node2 = "n2_" + millis;
        head = mk.commit("/", "+\"" + node1 + "\" : {} \n+\"" + node2 + "\" : {}\n", head, "");
        assertTrue(mk.nodeExists('/' + node1, head));
        assertTrue(mk.nodeExists('/' + node2, head));
    }

    @Test
    public void addDeepNodes() {
        String head = mk.getHeadRevision();

        head = mk.commit("/",
                "+\"a\" : {} \n" +
                        "+\"a/b\" : {} \n" +
                        "+\"a/b/c\" : {} \n" +
                        "+\"a/b/c/d\" : {} \n",
                head, "");

        assertTrue(mk.nodeExists("/a", head));
        assertTrue(mk.nodeExists("/a/b", head));
        assertTrue(mk.nodeExists("/a/b/c", head));
        assertTrue(mk.nodeExists("/a/b/c/d", head));
    }

    @Test
    public void addItemsIncrementally() {
        String head = mk.getHeadRevision();

        String node = "n_" + System.currentTimeMillis();

        head = mk.commit("/",
                "+\"" + node + "\" : {} \n" +
                        "+\"" + node + "/child1\" : {} \n" +
                        "+\"" + node + "/child2\" : {} \n" +
                        "+\"" + node + "/child1/grandchild11\" : {} \n" +
                        "^\"" + node + "/prop1\" : 41\n" +
                        "^\"" + node + "/child1/prop2\" : 42\n" +
                        "^\"" + node + "/child1/grandchild11/prop3\" : 43",
                head, "");

        JSONObject obj = parseJSONObject(mk.getNodes('/' + node, head, 3, 0, -1, null));
        assertPropertyValue(obj, "prop1", 41L);
        assertPropertyValue(obj, ":childNodeCount", 2L);
        assertPropertyValue(obj, "child1/prop2", 42L);
        assertPropertyValue(obj, "child1/:childNodeCount", 1L);
        assertPropertyValue(obj, "child1/grandchild11/prop3", 43L);
        assertPropertyValue(obj, "child1/grandchild11/:childNodeCount", 0L);
        assertPropertyValue(obj, "child2/:childNodeCount", 0L);
    }

    @Test
    public void removeNode() {
        String head = mk.getHeadRevision();
        String node = "removeNode_" + System.currentTimeMillis();

        head = mk.commit("/", "+\"" + node + "\" : {\"child\":{}}", head, "");

        head = mk.commit('/' + node, "-\"child\"", head, "");
        JSONObject obj = parseJSONObject(mk.getNodes('/' + node, head));
        assertPropertyValue(obj, ":childNodeCount", 0L);
    }

    @Test
    public void moveNode() {
        String head = mk.getHeadRevision();
        String node = "moveNode_" + System.currentTimeMillis();
        String movedNode = "movedNode_" + System.currentTimeMillis();
        head = mk.commit("/", "+\"" + node + "\" : {}", head, "");

        head = mk.commit("/", ">\"" + node + "\" : \"" + movedNode + '\"', head, "");
        assertFalse(mk.nodeExists('/' + node, head));
        assertTrue(mk.nodeExists('/' + movedNode, head));
    }

    @Test
    public void overwritingMove() {
        String head = mk.getHeadRevision();

        head = mk.commit("/", "+\"a\" : {} \n+\"b\" : {} \n", head, "");
        try {
            mk.commit("/", ">\"a\" : \"b\"  ", head, "");
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void conflictingMove() {
        String head = mk.getHeadRevision();

        head = mk.commit("/", "+\"a\" : {} \n+\"b\" : {}\n", head, "");

        String r1 = mk.commit("/", ">\"a\" : \"b/a\"", head, "");
        assertFalse(mk.nodeExists("/a", r1));
        assertTrue(mk.nodeExists("/b", r1));
        assertTrue(mk.nodeExists("/b/a", r1));

        try {
            mk.commit("/", ">\"b\" : \"a/b\"", head, "");
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void conflictingAddDelete() {
        String head = mk.getHeadRevision();

        head = mk.commit("/", "+\"a\" : {} \n+\"b\" : {}\n", head, "");

        String r1 = mk.commit("/", "-\"b\" \n +\"a/x\" : {}", head, "");
        assertFalse(mk.nodeExists("/b", r1));
        assertTrue(mk.nodeExists("/a", r1));
        assertTrue(mk.nodeExists("/a/x", r1));

        try {
            mk.commit("/", "-\"a\" \n +\"b/x\" : {}", head, "");
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void removeProperty() {
        String head = mk.getHeadRevision();
        long t = System.currentTimeMillis();
        String node = "removeProperty_" + t;

        head = mk.commit("/", "+\"" + node + "\" : {\"prop\":\"value\"}", head, "");

        head = mk.commit("/", "^\"" + node + "/prop\" : null", head, "");
        JSONObject obj = parseJSONObject(mk.getNodes('/' + node, head));
        assertPropertyValue(obj, ":childNodeCount", 0L);
    }

    @Test
    public void branchAndMerge() {
        // make sure /branch doesn't exist in head
        assertFalse(mk.nodeExists("/branch", null));

        // create a branch on head
        String branchRev = mk.branch(null);

        // add a node /branch in branchRev
        branchRev = mk.commit("", "+\"/branch\":{}", branchRev, "");
        // make sure /branch doesn't exist in head
        assertFalse(mk.nodeExists("/branch", null));
        // make sure /branch does exist in branchRev
        assertTrue(mk.nodeExists("/branch", branchRev));

        // add a node /branch/foo in branchRev
        branchRev = mk.commit("", "+\"/branch/foo\":{}", branchRev, "");

        // make sure branchRev doesn't show up in revision history
        String hist = mk.getRevisionHistory(0, -1);
        JSONArray ar = parseJSONArray(hist);
        for (Object entry : ar) {
            assertTrue(entry instanceof JSONObject);
            JSONObject rev = (JSONObject) entry;
            assertFalse(branchRev.equals(rev.get("id")));
        }

        // add a node /test123 in head
        mk.commit("", "+\"/test123\":{}", null, "");
        // make sure /test123 doesn't exist in branchRev
        assertFalse(mk.nodeExists("/test123", branchRev));

        // merge branchRev with head
        mk.merge(branchRev, "");
        // make sure /test123 still exists in head
        assertTrue(mk.nodeExists("/test123", null));
        // make sure /branch/foo does now exist in head
        assertTrue(mk.nodeExists("/branch/foo", null));
    }
}
