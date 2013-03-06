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
package org.apache.jackrabbit.oak.fixture;

import java.io.File;

import javax.jcr.Repository;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.prototype.MongoMK;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.segment.MongoStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;

import com.mongodb.Mongo;

public abstract class OakRepositoryFixture implements RepositoryFixture {

    public static RepositoryFixture getMemory() {
        return new OakRepositoryFixture("Oak-Memory") {
            @Override
            public Repository[] setUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                MicroKernel kernel = new MicroKernelImpl();
                for (int i = 0; i < cluster.length; i++) {
                    Oak oak = new Oak(kernel);
                    cluster[i] = new Jcr(oak).createRepository();
                }
                return cluster;
            }
        };
    }

    public static RepositoryFixture getDefault() {
        return new OakRepositoryFixture("Oak-Default") {
            private MicroKernelImpl[] kernels;
            @Override
            public Repository[] setUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                kernels = new MicroKernelImpl[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    kernels[i] = new MicroKernelImpl(unique);
                    cluster[i] = new Jcr(kernels[i]).createRepository();
                }
                return cluster;
            }
            @Override
            public void tearDownCluster() {
                for (MicroKernelImpl kernel : kernels) {
                    kernel.dispose();
                }
                FileUtils.deleteQuietly(new File(unique));
            }
        };
    }

    public static RepositoryFixture getMongo(final String host, final int port) {
        return new OakRepositoryFixture("Oak-Mongo") {
            private MongoMK[] kernels;
            @Override
            public Repository[] setUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                kernels = new MongoMK[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    MongoConnection mongo =
                            new MongoConnection(host, port, unique);
                    kernels[i] = new MongoMK(mongo.getDB(), i);
                    cluster[i] = new Jcr(kernels[i]).createRepository();
                }
                return cluster;
            }
            @Override
            public void tearDownCluster() {
                for (MongoMK kernel : kernels) {
                    kernel.dispose();
                }
                try {
                    MongoConnection mongo =
                            new MongoConnection(host, port, unique);
                    mongo.getDB().dropDatabase();
                    mongo.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }

    public static RepositoryFixture getSegment(
            final String host, final int port, final long cacheSize) {
        return new OakRepositoryFixture("Oak-Segment") {
            private Mongo mongo;
            @Override
            public Repository[] setUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                mongo = new Mongo(host, port);
                for (int i = 0; i < cluster.length; i++) {
                    SegmentStore store =
                            new MongoStore(mongo.getDB(unique), cacheSize);
                    Oak oak = new Oak(new SegmentNodeStore(store));
                    cluster[i] = new Jcr(oak).createRepository();
                }
                return cluster;
            }
            @Override
            public void tearDownCluster() {
                mongo.getDB(unique).dropDatabase();
                mongo.close();
            }
        };
    }

    private final String name;

    protected final String unique;

    protected OakRepositoryFixture(String name) {
        this.name = name;
        this.unique = String.format("%s-%d", name, System.currentTimeMillis());
    }

    @Override
    public boolean isAvailable(int n) {
        return true;
    }


    @Override
    public void syncRepositoryCluster(Repository... nodes) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void tearDownCluster() {
    }

    @Override
    public String toString() {
        return name;
    }

}
