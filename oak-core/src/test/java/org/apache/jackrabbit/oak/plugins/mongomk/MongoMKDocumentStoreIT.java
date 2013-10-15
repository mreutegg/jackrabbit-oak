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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;
import org.junit.Test;

import com.google.common.collect.Maps;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

/**
 * Tests {@code MongoDocumentStore} with concurrent updates.
 */
public class MongoMKDocumentStoreIT extends AbstractMongoConnectionTest {

    private static final int NUM_THREADS = 3;
    private static final int UPDATES_PER_THREAD = 10;

    @Test
    public void concurrent() throws Exception {
        final long time = System.currentTimeMillis();
        mk.commit("/", "+\"test\":{}", null, null);
        final String id = Utils.getIdFromPath("/test");
        final DocumentStore docStore = mk.getDocumentStore();
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < NUM_THREADS; i++) {
            final int tId = i;
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    Revision r = new Revision(time, tId, 0);
                    for (int i = 0; i < UPDATES_PER_THREAD; i++) {
                        UpdateOp update = new UpdateOp(id, false);
                        update.setMapEntry("prop", r, String.valueOf(i));
                        docStore.createOrUpdate(Collection.NODES, update);
                    }
                }
            }));
        }
        final List<Exception> exceptions = new ArrayList<Exception>();
        final AtomicBoolean running = new AtomicBoolean(true);
        Thread reader = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Map<Revision, Integer> previous = Maps.newHashMap();
                    while (running.get()) {
                        NodeDocument doc = docStore.find(Collection.NODES, id);
                        if (doc == null) {
                            throw new Exception("document is null");
                        }
                        Map<Revision, String> values = doc.getValueMap("prop");
                        for (Map.Entry<Revision, String> entry : values.entrySet()) {
                            Revision r = entry.getKey();
                            Integer previousValue = previous.get(r);
                            Integer currentValue = Integer.parseInt(entry.getValue());
                            if (previousValue != null &&
                                    previousValue > currentValue) {
                                throw new Exception("inconsistent read for " +
                                        r + ". previous value: " + previousValue +
                                        ", now: " + entry.getValue());
                            }
                            // remember for next round
                            previous.put(r, currentValue);
                        }
                        Thread.yield();
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        });
        reader.start();
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        running.set(false);
        reader.join();
        for (Exception e : exceptions) {
            throw e;
        }
        NodeDocument doc = docStore.find(Collection.NODES, id);
        assertNotNull(doc);
        Map<Revision, String> values = doc.getLocalMap("prop");
        assertNotNull(values);
        for (Map.Entry<Revision, String> entry : values.entrySet()) {
            assertEquals(String.valueOf(UPDATES_PER_THREAD - 1), entry.getValue());
        }
    }

    @Test
    public void concurrentLoop() throws Exception {
        // run for 5 seconds
        long end = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (System.currentTimeMillis() < end) {
            concurrent();
            tearDownConnection();
            setUpConnection();
        }
    }

    @Test
    public void negativeCache() throws Exception {
        String id = Utils.getIdFromPath("/test");
        DocumentStore docStore = mk.getDocumentStore();
        assertNull(docStore.find(Collection.NODES, id));
        mk.commit("/", "+\"test\":{}", null, null);
        assertNotNull(docStore.find(Collection.NODES, id));
    }
}
