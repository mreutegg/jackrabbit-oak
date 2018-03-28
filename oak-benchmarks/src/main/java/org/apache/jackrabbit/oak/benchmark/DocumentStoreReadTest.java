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

import java.security.SecureRandom;
import java.util.Random;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;

public class DocumentStoreReadTest extends DocumentStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentStoreReadTest.class);

    private static final int NUM_NODES = Integer.getInteger("num.nodes", 10000);

    private static final String TEST_ID = System.getProperty("test.id", "");

    private static final ThreadLocal<Random> RANDOM = ThreadLocal.withInitial(SecureRandom::new);

    @Override
    Operation generateOperation() {
        String id = getIdFromPath("/test" + TEST_ID + "/path/" + nextNodeName());
        return ds -> {
            if (ds.find(Collection.NODES, id, 0) == null) {
                LOG.warn("document not found: {}", id);
            }
        };
    }

    private static String nextNodeName() {
        return "n" + Integer.toHexString(RANDOM.get().nextInt(NUM_NODES));
    }
}
