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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COMMIT_ROOT;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setCommitRoot;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setModified;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type.REMOVE_MAP_ENTRY;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NodeDocumentSweeperTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns;
    private DocumentMK mk;
    private DocumentStore store;
    private MissingLastRevSeeker seeker;

    @Before
    public void before() {
        DocumentMK.Builder builder = builderProvider.newBuilder();
        mk = builder.open();
        ns = builder.getNodeStore();
        store = ns.getDocumentStore();
        seeker = builder.createMissingLastRevSeeker();
    }

    @Test
    public void sweepUncommittedBeforeHead() throws Exception {
        Revision sweepStart = ns.getHeadRevision().getRevision(ns.getClusterId());
        Revision uncommitted = ns.newRevision();
        NodeBuilder b = ns.getRoot().builder();
        b.child("test");
        merge(ns, b);

        UpdateOp op = new UpdateOp(getIdFromPath("/test"), false);
        op.setMapEntry("foo", uncommitted, "value");
        setCommitRoot(op, uncommitted, 0);
        setModified(op, uncommitted);
        assertNotNull(store.findAndUpdate(NODES, op));

        List<UpdateOp> ops = Lists.newArrayList();
        Revision nextSweepStart = sweep(sweepStart, ops);

        assertEquals(ns.getHeadRevision().getRevision(ns.getClusterId()), nextSweepStart);
        assertEquals(1, ops.size());
        op = ops.get(0);
        Map<Key, Operation> changes = op.getChanges();
        assertEquals(2, changes.size());
        Operation o = changes.get(new Key(COMMIT_ROOT, uncommitted));
        assertNotNull(o);
        assertEquals(REMOVE_MAP_ENTRY, o.type);
        o = changes.get(new Key("foo", uncommitted));
        assertNotNull(o);
        assertEquals(REMOVE_MAP_ENTRY, o.type);
    }

    @Test
    public void sweepUncommittedAfterHead() throws Exception {
        Revision sweepStart = ns.getHeadRevision().getRevision(ns.getClusterId());
        NodeBuilder b = ns.getRoot().builder();
        b.child("test");
        merge(ns, b);

        Revision uncommitted = ns.newRevision();
        UpdateOp op = new UpdateOp(getIdFromPath("/test"), false);
        op.setMapEntry("foo", uncommitted, "value");
        setCommitRoot(op, uncommitted, 0);
        setModified(op, uncommitted);
        assertNotNull(store.findAndUpdate(NODES, op));

        List<UpdateOp> ops = Lists.newArrayList();
        Revision nextSweepStart = sweep(sweepStart, ops);

        assertEquals(ns.getHeadRevision().getRevision(ns.getClusterId()), nextSweepStart);
        assertEquals(0, ops.size());
    }

    @Test
    public void sweepUnmergedBranchCommit() throws Exception {
        int clusterId = ns.getClusterId();
        Revision sweepStart = ns.getHeadRevision().getRevision(clusterId);
        NodeBuilder b = ns.getRoot().builder();
        b.child("test");
        merge(ns, b);

        String branchRev = mk.branch(null);
        Revision uncommitted = Revision.fromString(
                mk.commit("/test", "^\"foo\":\"value\"", branchRev, null)
        ).asTrunkRevision();

        // force a new head revision newer than branch commit
        b = ns.getRoot().builder();
        b.child("bar");
        merge(ns, b);

        // restart node store. this will make branch eligible for GC
        ns.dispose();
        DocumentMK.Builder builder = builderProvider.newBuilder();
        mk = builder.setDocumentStore(store).setClusterId(clusterId).open();
        ns = mk.getNodeStore();
        seeker = builder.createMissingLastRevSeeker();

        List<UpdateOp> ops = Lists.newArrayList();
        Revision nextSweepStart = sweep(sweepStart, ops);

        assertEquals(ns.getHeadRevision().getRevision(ns.getClusterId()), nextSweepStart);
        assertEquals(1, ops.size());
        UpdateOp op = ops.get(0);
        Map<Key, Operation> changes = op.getChanges();
        assertEquals(2, changes.size());
        Operation o = changes.get(new Key(COMMIT_ROOT, uncommitted));
        assertNotNull(o);
        assertEquals(REMOVE_MAP_ENTRY, o.type);
        o = changes.get(new Key("foo", uncommitted));
        assertNotNull(o);
        assertEquals(REMOVE_MAP_ENTRY, o.type);
    }

    @Test
    public void sweepMergedBranch() throws Exception {
        int clusterId = ns.getClusterId();
        Revision sweepStart = ns.getHeadRevision().getRevision(clusterId);

        String branchRev = mk.branch(null);
        branchRev = mk.commit("/", "+\"foo\":{}", branchRev, null);
        branchRev = mk.commit("/", "+\"bar\":{}", branchRev, null);
        branchRev = mk.commit("/", "+\"baz\":{}", branchRev, null);
        mk.merge(branchRev, null);

        List<UpdateOp> ops = Lists.newArrayList();
        Revision nextSweepStart = sweep(sweepStart, ops);

        assertEquals(ns.getHeadRevision().getRevision(ns.getClusterId()), nextSweepStart);

        for (UpdateOp op : ops) {
            store.findAndUpdate(NODES, op);
        }

        NodeDocument doc = store.find(NODES, getIdFromPath("/foo"));
        assertNotNull(doc);
        assertEquals(0, doc.getLocalCommitRoot().size());
        doc = store.find(NODES, getIdFromPath("/bar"));
        assertNotNull(doc);
        assertEquals(0, doc.getLocalCommitRoot().size());
        doc = store.find(NODES, getIdFromPath("/baz"));
        assertNotNull(doc);
        assertEquals(0, doc.getLocalCommitRoot().size());

        NodeState root = ns.getRoot();
        assertTrue(root.hasChildNode("foo"));
        assertTrue(root.hasChildNode("bar"));
        assertTrue(root.hasChildNode("baz"));
    }

    private Revision sweep(Revision start, final List<UpdateOp> ops) {
        NodeDocumentSweeper sweeper = new NodeDocumentSweeper(
                ns.getHeadRevision().getRevision(ns.getClusterId()),
                start, seeker, ns.getBranches());
        return sweeper.sweep(new NodeDocumentSweepListener() {
            @Override
            public void sweepUpdate(UpdateOp op) {
                ops.add(op);
            }
        });
    }
}
