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
package org.apache.jackrabbit.oak.benchmark;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Repository;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

abstract class DocumentStoreTest extends Benchmark {

    private static final long WARMUP_MS = TimeUnit.SECONDS.toMillis(Long.getLong("warmup", 5));
    private static long RUN_TIME_MS = TimeUnit.SECONDS.toMillis(Long.getLong("runtime", 60));

    private AtomicLong failures = new AtomicLong();
    private AtomicLong successes = new AtomicLong();
    private AtomicLong duration = new AtomicLong(); // only for successes

    @Override
    public void run(Iterable<RepositoryFixture> fixtures,
                    List<Integer> concurrencyLevels) {
        for (RepositoryFixture f : fixtures) {
            run(f, concurrencyLevels);
        }
    }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        run(fixtures, Collections.singletonList(1));
    }

    abstract Operation generateOperation();

    void store(DocumentStore ds, List<UpdateOp> ops) throws DocumentStoreException {
        if (ops.size() == 1) {
            ds.createOrUpdate(Collection.NODES, ops.get(0));
        } else {
            ds.createOrUpdate(Collection.NODES, ops);
        }
    }

    interface Operation {

        void perform(DocumentStore ds) throws DocumentStoreException;
    }

    static DocumentNodeState newDocumentNodeState(@Nonnull DocumentNodeStore store,
                                                  @Nonnull String path,
                                                  @Nonnull RevisionVector rootRevision,
                                                  Iterable<? extends PropertyState> properties,
                                                  boolean hasChildren,
                                                  @Nullable RevisionVector lastRevision) {
        try {
            Constructor<DocumentNodeState> constructor = DocumentNodeState.class.getDeclaredConstructor(
                    DocumentNodeStore.class, String.class, RevisionVector.class,
                    Iterable.class, boolean.class, RevisionVector.class);
            constructor.setAccessible(true);
            return constructor.newInstance(store, path, rootRevision, properties, hasChildren, lastRevision);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static UpdateOp asOperation(DocumentNodeState ns, Revision commitRev) {
        try {
            Method m = ns.getClass().getDeclaredMethod("asOperation", Revision.class);
            m.setAccessible(true);
            return (UpdateOp) m.invoke(ns, commitRev);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static Revision newRevision(int clusterId) {
        return new Revision(System.currentTimeMillis(), 0, clusterId);
    }

    private void run(RepositoryFixture fixture, List<Integer> concurrencyLevels) {
        if (fixture.isAvailable(1)) {
            try {
                Repository repository = fixture.setUpCluster(1)[0];
                for (int i : concurrencyLevels) {
                    try {
                        run(repository, i);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                fixture.tearDownCluster();
            }
        }
    }

    private void run(Repository repository, int concurrencyLevel) throws Exception {
        ContentRepository cr = getFieldOfType(repository, ContentRepository.class);
        NodeStore ns = getFieldOfType(cr, NodeStore.class);
        DocumentStore ds = getFieldOfType(ns, DocumentStore.class);
        resetCounters();
        run(ds, concurrencyLevel);
        printCounters(concurrencyLevel);
    }

    private void run(DocumentStore ds, int concurrencyLevel) throws Exception {
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < concurrencyLevel; i++) {
            workers.add(new Thread(new Worker(ds)));
        }
        for (Thread t : workers) {
            t.start();
        }
        for (Thread t : workers) {
            t.join();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getFieldOfType(Object obj, Class<T> type) throws Exception {
        Class clazz = obj.getClass();
        while (clazz != null) {
            for (Field f : clazz.getDeclaredFields()) {
                if (type.isAssignableFrom(f.getType())) {
                    f.setAccessible(true);
                    return (T) f.get(obj);
                }
            }
            clazz = clazz.getSuperclass();
        }
        throw new IllegalStateException(obj + " does not have a " + type.getSimpleName() + " field");
    }

    private void resetCounters() {
        failures.set(0);
        successes.set(0);
        duration.set(0);
    }

    private void printCounters(int concurrencyLevel) {
        long durationMs = TimeUnit.NANOSECONDS.toMillis(duration.get());
        double perSuccess = -1;
        if (successes.get() > 0) {
            perSuccess = ((double) durationMs) / successes.get();
        }
        System.out.format("%d, %d, %d, %d, %.1f%n", concurrencyLevel, successes.get(),
                failures.get(), durationMs, perSuccess);
    }

    private class Worker implements Runnable {

        private DocumentStore ds;

        Worker(DocumentStore ds) {
            this.ds = ds;
        }

        @Override
        public void run() {
            long startAt = System.currentTimeMillis() + WARMUP_MS;
            long endAt = startAt + RUN_TIME_MS;
            for (;;) {
                long now = System.currentTimeMillis();
                if (now > endAt) {
                    break;
                }
                try {
                    Operation op = generateOperation();
                    long time = System.nanoTime();
                    op.perform(ds);
                    time = System.nanoTime() - time;
                    if (now > startAt) {
                        duration.addAndGet(time);
                        successes.incrementAndGet();
                    }
                } catch (DocumentStoreException e) {
                    // count all failure, also during warmup
                    failures.incrementAndGet();
                }
            }
        }
    }
}
