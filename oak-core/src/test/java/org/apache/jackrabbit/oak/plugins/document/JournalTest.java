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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.mongodb.DB;

public class JournalTest {

//    private static final boolean MONGO_DB = false;
    private static final boolean MONGO_DB = true;
    
    private TestBuilder builder;

    private MemoryDocumentStore ds;
    private MemoryBlobStore bs;

    private List<DocumentMK> mks = Lists.newArrayList();

    class DiffingObserver implements Observer, Runnable, NodeStateDiff {

        final List<DocumentNodeState> incomingRootStates1 = Lists.newArrayList();
        final List<DocumentNodeState> diffedRootStates1 = Lists.newArrayList();
        
        DocumentNodeState oldRoot = null;
        
        DiffingObserver(boolean startInBackground) {
            if (startInBackground) {
                // start the diffing in the background - so as to not
                // interfere with the contentChanged call
                Thread th = new Thread(this);
                th.setDaemon(true);
                th.start();
            }
        }

        public void clear() {
            synchronized(incomingRootStates1) {
                incomingRootStates1.clear();
                diffedRootStates1.clear();
            }
        }
        
        @Override
        public void contentChanged(NodeState root, CommitInfo info) {
            synchronized(incomingRootStates1) {
                incomingRootStates1.add((DocumentNodeState) root);
                incomingRootStates1.notifyAll();
            }
        }
        
        public void processAll() {
            while(processOne()) {
                // continue
            }
        }

        public boolean processOne() {
            DocumentNodeState newRoot;
            synchronized(incomingRootStates1) {
                if (incomingRootStates1.size()==0) {
                    return false;
                }
                newRoot = incomingRootStates1.remove(0);
            }
            if (oldRoot!=null) {
                newRoot.compareAgainstBaseState(oldRoot, this);
            }
            oldRoot = newRoot;
            synchronized(incomingRootStates1) {
                diffedRootStates1.add(newRoot);
            }
            return true;
        }
        
        @Override
        public void run() {
            while(true) {
                DocumentNodeState newRoot;
                synchronized(incomingRootStates1) {
                    while(incomingRootStates1.size()==0) {
                        try {
                            incomingRootStates1.wait();
                        } catch (InterruptedException e) {
                            // ignore
                            continue;
                        }
                    }
                    newRoot = incomingRootStates1.remove(0);
                }
                if (oldRoot!=null) {
                    newRoot.compareAgainstBaseState(oldRoot, this);
                }
                oldRoot = newRoot;
                synchronized(incomingRootStates1) {
                    diffedRootStates1.add(newRoot);
                }
            }
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            return true;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before,
                NodeState after) {
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            return true;
        }

        public int getTotal() {
            synchronized(incomingRootStates1) {
                return incomingRootStates1.size() + diffedRootStates1.size();
            }
        }
        
    }
    
    @Test
    public void cleanupTest() throws Exception {
        DocumentMK mk1 = createMK(1, 0);
        DocumentNodeStore ns1 = mk1.getNodeStore();
        JournalGarbageCollector gc = new JournalGarbageCollector(ns1);
        gc.gc(1, TimeUnit.DAYS);
        gc.gc(6, TimeUnit.HOURS);
        gc.gc(1, TimeUnit.HOURS);
        gc.gc(10, TimeUnit.MINUTES);
        gc.gc(1, TimeUnit.MINUTES);
    }
    
    @Test
    public void journalTest() throws Exception {
        DocumentMK mk1 = createMK(1, 0);
        DocumentNodeStore ns1 = mk1.getNodeStore();
        CountingDocumentStore countingDocStore1 = builder.actualStore;
        CountingTieredDiffCache countingDiffCache1 = builder.actualDiffCache;

        DocumentMK mk2 = createMK(2, 0);
        DocumentNodeStore ns2 = mk2.getNodeStore();
        CountingDocumentStore countingDocStore2 = builder.actualStore;
        CountingTieredDiffCache countingDiffCache2 = builder.actualDiffCache;

        final DiffingObserver observer = new DiffingObserver(false);
        ns1.addObserver(observer);
        
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();
        observer.processAll(); // to make sure we have an 'oldRoot'
        observer.clear();
        countingDocStore1.resetCounters();
        countingDocStore2.resetCounters();
        countingDocStore1.printStacks = true;
        countingDiffCache1.resetLoadCounter();
        countingDiffCache2.resetLoadCounter();

        mk2.commit("/", "+\"regular1\": {}", null, null);
        mk2.commit("/", "+\"regular2\": {}", null, null);
        mk2.commit("/", "+\"regular3\": {}", null, null);
        mk2.commit("/regular2", "+\"regular4\": {}", null, null);
        // flush to journal
        ns2.runBackgroundOperations();
        
        // nothing notified yet
        assertEquals(0, observer.getTotal());
        assertEquals(0, countingDocStore1.getNumFindCalls(Collection.NODES));
        assertEquals(0, countingDocStore1.getNumQueryCalls(Collection.NODES));
        assertEquals(0, countingDocStore1.getNumRemoveCalls(Collection.NODES));
        assertEquals(0, countingDocStore1.getNumCreateOrUpdateCalls(Collection.NODES));
        assertEquals(0, countingDiffCache1.getLoadCount());
        
        // let node 1 read those changes
        System.err.println("run background ops");
        ns1.runBackgroundOperations();
        mk2.commit("/", "+\"regular5\": {}", null, null);
        ns2.runBackgroundOperations();
        ns1.runBackgroundOperations();
        // and let the observer process everything
        observer.processAll();
        countingDocStore1.printStacks = false;
        
        // now expect 1 entry in rootStates
        assertEquals(2, observer.getTotal());
        assertEquals(0, countingDiffCache1.getLoadCount());
        assertEquals(0, countingDocStore1.getNumRemoveCalls(Collection.NODES));
        assertEquals(0, countingDocStore1.getNumCreateOrUpdateCalls(Collection.NODES));
        assertEquals(0, countingDocStore1.getNumQueryCalls(Collection.NODES));
//        assertEquals(0, countingDocStore1.getNumFindCalls(Collection.NODES));
    }
    
    @Test
    public void externalBranchChange() throws Exception {
        DocumentMK mk1 = createMK(1, 0);
        DocumentNodeStore ns1 = mk1.getNodeStore();
        DocumentMK mk2 = createMK(2, 0);
        DocumentNodeStore ns2 = mk2.getNodeStore();
        
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        mk1.commit("/", "+\"regular1\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        mk1.commit("/regular1", "+\"regular1child\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        mk1.commit("/", "+\"regular2\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        mk1.commit("/", "+\"regular3\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        mk1.commit("/", "+\"regular4\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        mk1.commit("/", "+\"regular5\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        String b1 = mk1.branch(null);
        b1 = mk1.commit("/", "+\"branchVisible\": {}", b1, null);
        mk1.merge(b1, null);
        
        // to flush the branch commit either dispose of mk1
        // or run the background operations explicitly 
        // (as that will propagate the lastRev to the root)
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();
        
        String nodes = mk2.getNodes("/", null, 0, 0, 100, null);
        assertEquals("{\"branchVisible\":{},\"regular1\":{},\"regular2\":{},\"regular3\":{},\"regular4\":{},\"regular5\":{},\":childNodeCount\":6}", nodes);
    }

    @Before
    @After
    public void clear() {
        for (DocumentMK mk : mks) {
            mk.dispose();
        }
        mks.clear();
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            MongoUtils.dropCollections(db);
        }
    }

    private final class TestBuilder extends DocumentMK.Builder {
        private CountingDocumentStore actualStore;
        private CountingTieredDiffCache actualDiffCache;

        @Override
        public DocumentStore getDocumentStore() {
            if (actualStore==null) {
                actualStore = new CountingDocumentStore(super.getDocumentStore());
            }
            return actualStore;
        }
        
        @Override
        public DiffCache getDiffCache() {
            if (actualDiffCache==null) {
                actualDiffCache = new CountingTieredDiffCache(this);
            }
            return actualDiffCache;
        }
    }

    private DocumentMK createMK(int clusterId, int asyncDelay) {
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection(/*"oak-observation"*/).getDB();
            builder = newDocumentMKBuilder();
            return register(builder.setMongoDB(db)
                    .setClusterId(clusterId).setAsyncDelay(asyncDelay).open());
        } else {
            if (ds == null) {
                ds = new MemoryDocumentStore();
            }
            if (bs == null) {
                bs = new MemoryBlobStore();
            }
            return createMK(clusterId, asyncDelay, ds, bs);
        }
    }
    
    private TestBuilder newDocumentMKBuilder() {
        return new TestBuilder();
    }

    private DocumentMK createMK(int clusterId, int asyncDelay,
                             DocumentStore ds, BlobStore bs) {
        builder = newDocumentMKBuilder();
        return register(builder.setDocumentStore(ds)
                .setBlobStore(bs).setClusterId(clusterId)
                .setAsyncDelay(asyncDelay).open());
    }

    private DocumentMK register(DocumentMK mk) {
        mks.add(mk);
        return mk;
    }

    private void disposeMK(DocumentMK mk) {
        mk.dispose();
        for (int i = 0; i < mks.size(); i++) {
            if (mks.get(i) == mk) {
                mks.remove(i);
            }
        }
    }
}
