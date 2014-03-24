/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class CheckpointsTest {

    private final SimulatedClock clock = new SimulatedClock();

    private final DocumentNodeStore store = new DocumentMK.Builder().clock(clock).getNodeStore();

    @Test
    public void testCheckpointPurge() throws Exception {
        long expiryTime = 1000;
        Revision r1 = Revision.fromString(store.checkpoint(expiryTime));
        assertEquals(r1, store.getCheckpoints().getOldestRevisionToKeep());

        //Trigger expiry by forwarding the clock to future
        clock.forwardPast(expiryTime);
        assertNull(store.getCheckpoints().getOldestRevisionToKeep());
    }

    @Test
    public void testGetOldestRevisionToKeep() throws Exception {
        long et1 = 1000, et2 = et1 + 1000;

        Revision r1 = Revision.fromString(store.checkpoint(et1));

        //Do some commit to change headRevision
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x");
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Revision r2 = Revision.fromString(store.checkpoint(et2));
        assertNotSame(r1, r2);

        //r2 has the later expiry
        assertEquals(r2, store.getCheckpoints().getOldestRevisionToKeep());

        //Trigger expiry by forwarding the clock to future e1
        clock.forwardPast(et1);
        assertEquals(r2, store.getCheckpoints().getOldestRevisionToKeep());

        //Trigger expiry by forwarding the clock to future e2
        //This time no valid checkpoint
        clock.forwardPast(et2);
        assertNull(store.getCheckpoints().getOldestRevisionToKeep());
    }

    private static class SimulatedClock extends Clock {
        private final AtomicLong time = new AtomicLong();

        @Override
        public long getTime() {
            return time.incrementAndGet();
        }

        /**
         * Fast forwards the clock to some future time past the given time
         */
        public void forwardPast(long futureTime) {
            time.set(futureTime + 100);
        }
    }
}
