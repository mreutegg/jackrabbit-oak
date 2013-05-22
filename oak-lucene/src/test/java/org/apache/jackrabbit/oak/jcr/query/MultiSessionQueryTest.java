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
package org.apache.jackrabbit.oak.jcr.query;

import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.core.query.AbstractQueryTest;

/**
 * Tests the Lucene index using multiple threads.
 */
public class MultiSessionQueryTest extends AbstractQueryTest {

    private static final boolean DISABLED = true;

    final static int THREAD_COUNT = 3;

    public void testConcurrent() throws Exception {
        if (DISABLED) { return; } // test disabled for now

        final Exception[] ex = new Exception[1];
        Thread[] threads = new Thread[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            final Session s = superuser.getRepository().login(
                    new SimpleCredentials("admin", "admin".toCharArray()));
            final String node = "node" + i;
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        doTest(s, node);
                    } catch (Exception e) {
                        ex[0] = e;
                    }
                }
            };
            threads[i] = t;
        }
        for (Thread t : threads) {
            t.start();
        }
        Thread.sleep(100);
        for (Thread t : threads) {
            t.join();
        }
        if (ex[0] != null) {
            throw ex[0];
        }
    }

    void doTest(Session s, String node) throws Exception {
        Node root = s.getRootNode();
        if (!root.hasNode(node)) {
            root.addNode(node);
            s.save();
        }
        for (int i = 0; i < 10; i++) {
            // Thread.sleep(100);
            // System.out.println("session " + node + " work");
        }
    }

}
