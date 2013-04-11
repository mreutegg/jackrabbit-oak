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
package org.apache.jackrabbit.mongomk.prototype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DB;

/**
 * A set of simple cluster tests.
 */
public class ClusterTest {
    
    private static final boolean MONGO_DB = false;
    // private static final boolean MONGO_DB = true;
    
    private MemoryDocumentStore ds;
    private MemoryBlobStore bs;

    @Test
    public void clusterNodeInfoLease() throws InterruptedException {
        MemoryDocumentStore store = new MemoryDocumentStore();
        ClusterNodeInfo c1, c2;
        c1 = ClusterNodeInfo.getInstance(store, "m1", null);
        assertEquals(1, c1.getId());
        c1.setLeaseTime(1);
        // this will quickly expire
        c1.renewLease(1);
        Thread.sleep(10);
        c2 = ClusterNodeInfo.getInstance(store, "m1", null);
        assertEquals(1, c2.getId());
    }
    
    @Test
    public void clusterNodeId() {
        MongoMK mk1 = createMK(0);
        MongoMK mk2 = createMK(0);
        assertEquals(1, mk1.getClusterInfo().getId());
        assertEquals(2, mk2.getClusterInfo().getId());
        mk1.dispose();
        mk2.dispose();
    }    
    
    @Test
    public void clusterNodeInfo() {
        MemoryDocumentStore store = new MemoryDocumentStore();
        ClusterNodeInfo c1, c2, c3, c4;
        
        c1 = ClusterNodeInfo.getInstance(store, "m1", null);
        assertEquals(1, c1.getId());
        c1.dispose();
        
        // get the same id
        c1 = ClusterNodeInfo.getInstance(store, "m1", null);
        assertEquals(1, c1.getId());
        c1.dispose();
        
        // now try to add another one:
        // must get a new id
        c2 = ClusterNodeInfo.getInstance(store, "m2", null);
        assertEquals(2, c2.getId());
        
        // a different machine
        c3 = ClusterNodeInfo.getInstance(store, "m3", "/a");
        assertEquals(3, c3.getId());
        
        c2.dispose();
        c3.dispose();
        
        c3 = ClusterNodeInfo.getInstance(store, "m3", "/a");
        assertEquals(3, c3.getId());

        c3.dispose();
        
        c4 = ClusterNodeInfo.getInstance(store, "m3", "/b");
        assertEquals(4, c4.getId());

        c1.dispose();
    }
    
    @Test
    public void conflict() {
        MongoMK mk1 = createMK(1);
        MongoMK mk2 = createMK(2);
        
        String m1r0 = mk1.getHeadRevision();
        String m2r0 = mk2.getHeadRevision();
        
        mk1.commit("/", "+\"test\":{}", m1r0, null);
        try {
            mk2.commit("/", "+\"test\":{}", m2r0, null);
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
        
        mk1.dispose();
        mk2.dispose();
    }
    
    @Test
    public void revisionVisibility() throws InterruptedException {
        MongoMK mk1 = createMK(1);
        MongoMK mk2 = createMK(2);
        
        String m2h;
        m2h = mk2.getNodes("/", mk2.getHeadRevision(), 0, 0, 2, null);
        assertEquals("{\":childNodeCount\":0}", m2h);
        
        mk1.commit("/", "+\"test\":{}", null, null);
        String m1h = mk1.getNodes("/", mk1.getHeadRevision(), 0, 0, 1, null);
        assertEquals("{\"test\":{},\":childNodeCount\":1}", m1h);
        
        m2h = mk2.getNodes("/", mk2.getHeadRevision(), 0, 0, 2, null);
        
        // not available yet...
        assertEquals("{\":childNodeCount\":0}", m2h);
        m2h = mk2.getNodes("/test", mk2.getHeadRevision(), 0, 0, 2, null);
        
        // the delay is 10 ms - wait at most 1000 millis
        for (int i = 0; i < 100; i++) {
            Thread.sleep(10);
            if (mk1.getPendingWriteCount() > 0) {
                continue;
            }
            if (mk2.isCached("/")) {
                continue;
            }
            break;
        }
        
        // so now it should be available
        m2h = mk2.getNodes("/", mk2.getHeadRevision(), 0, 0, 5, null);
        assertEquals("{\"test\":{},\":childNodeCount\":1}", m2h);
        
        mk1.dispose();
        mk2.dispose();
    }    
    
    @Test
    public void rollbackAfterConflict() {
        MongoMK mk1 = createMK(1);
        MongoMK mk2 = createMK(2);
        
        String m1r0 = mk1.getHeadRevision();
        String m2r0 = mk2.getHeadRevision();
        
        mk1.commit("/", "+\"test\":{}", m1r0, null);
        try {
            mk2.commit("/", "+\"a\": {} +\"test\":{}", m2r0, null);
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
        mk2.commit("/", "+\"a\": {}", null, null);
        
        mk1.dispose();
        mk2.dispose();
    }

    @Before
    @After
    public void clear() {
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            MongoUtils.dropCollections(db);
        }                    
    }

    private MongoMK createMK(int clusterId) {
        MongoMK.Builder builder = new MongoMK.Builder();
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            builder.setMongoDB(db);
        } else {
            if (ds == null) {
                ds = new MemoryDocumentStore();
            }
            if (bs == null) {
                bs = new MemoryBlobStore();
            }
            builder.setDocumentStore(ds).setBlobStore(bs);
        }
        builder.setAsyncDelay(10);
        return builder.setClusterId(clusterId).open();
    }

}
