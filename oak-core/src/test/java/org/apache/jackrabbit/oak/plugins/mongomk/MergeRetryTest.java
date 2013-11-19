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

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

/**
 * Test for OAK-1198
 */
public class MergeRetryTest {

    // this hook adds a 'foo' child if it does not exist
    private static final CommitHook HOOK = new EditorHook(new EditorProvider() {
        @CheckForNull
        @Override
        public Editor getRootEditor(NodeState before,
                                    NodeState after,
                                    final NodeBuilder builder)
                throws CommitFailedException {
            return new DefaultEditor() {
                @Override
                public void enter(NodeState before, NodeState after)
                        throws CommitFailedException {
                    if (!builder.hasChildNode("foo")) {
                        builder.child("foo");
                    }
                }
            };
        }
    });

    @Test
    public void retryOnConflict() throws Exception {
        MemoryDocumentStore ds = new MemoryDocumentStore();
        MemoryBlobStore bs = new MemoryBlobStore();

        MongoMK mk1 = createMK(1, 1000, ds, bs);
        MongoMK mk2 = createMK(2, 1000, ds, bs);

        try {
            NodeStore ns1 = new KernelNodeStore(mk1);
            NodeStore ns2 = new KernelNodeStore(mk2);

            NodeBuilder builder1 = ns1.getRoot().builder();
            builder1.child("bar");

            NodeBuilder builder2 = ns2.getRoot().builder();
            builder2.child("qux");

            ns1.merge(builder1, HOOK, null);
            ns2.merge(builder2, HOOK, null);
        } finally {
            mk1.dispose();
            mk2.dispose();
        }
    }

    private MongoMK createMK(int clusterId, int asyncDelay,
                             DocumentStore ds, BlobStore bs) {
        return new MongoMK.Builder().setDocumentStore(ds).setBlobStore(bs)
                .setClusterId(clusterId).setAsyncDelay(asyncDelay).open();
    }
}
