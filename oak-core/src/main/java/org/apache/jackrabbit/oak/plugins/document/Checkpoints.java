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

import java.util.Map;
import java.util.SortedMap;

import javax.annotation.CheckForNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Checkpoints provide details around which revision are to be kept. These
 * are stored in Settings collection.
 */
class Checkpoints {
    private static final String ID = "checkpoint";

    /**
     * Property name to store all checkpoint data. The data is stored as Revision => expiryTime
     */
    private static final String PROP_CHECKPOINT = "data";

    private final DocumentNodeStore nodeStore;

    private final DocumentStore store;

    private final Logger log = LoggerFactory.getLogger(getClass());

    Checkpoints(DocumentNodeStore store) {
        this.nodeStore = store;
        this.store = store.getDocumentStore();
        createIfNotExist();
    }

    public Revision create(long lifetimeInMillis) {
        Revision r = nodeStore.getHeadRevision();
        UpdateOp op = new UpdateOp(ID, false);
        long endTime = nodeStore.getClock().getTime() + lifetimeInMillis;
        op.setMapEntry(PROP_CHECKPOINT, r, Long.toString(endTime));
        store.createOrUpdate(Collection.SETTINGS, op);
        return r;
    }


    /**
     * Returns the oldest valid checkpoint registered.
     *
     * @return oldest valid checkpoint registered. Might return null if no valid
     * checkpoint found
     */
    @SuppressWarnings("unchecked")
    @CheckForNull
    public Revision getOldestRevisionToKeep() {
        //Get uncached doc
        Document cdoc = store.find(Collection.SETTINGS, ID, 0);
        SortedMap<Revision, String> checkpoints = (SortedMap<Revision, String>) cdoc.get(PROP_CHECKPOINT);

        if(checkpoints == null){
            log.debug("No checkpoint registered so far");
            return null;
        }

        final long currentTime = nodeStore.getClock().getTime();
        UpdateOp op = new UpdateOp(ID, false);
        Revision lastAliveRevision = null;
        long oldestExpiryTime = 0;

        for (Map.Entry<Revision, String> e : checkpoints.entrySet()) {
            final long expiryTime = Long.parseLong(e.getValue());
            if (currentTime > expiryTime) {
                op.removeMapEntry(PROP_CHECKPOINT, e.getKey());
            } else if (expiryTime > oldestExpiryTime) {
                oldestExpiryTime = expiryTime;
                lastAliveRevision = e.getKey();
            }
        }

        if (op.hasChanges()) {
            store.findAndUpdate(Collection.SETTINGS, op);
            log.info("Purged {} expired checkpoints", op.getChanges().size());
        }

        return lastAliveRevision;
    }

    private void createIfNotExist() {
        if (store.find(Collection.SETTINGS, ID) == null) {
            UpdateOp updateOp = new UpdateOp(ID, true);
            updateOp.set(Document.ID, ID);
            store.createOrUpdate(Collection.SETTINGS, updateOp);
        }
    }
}
