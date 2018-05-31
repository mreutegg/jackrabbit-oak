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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.getModifiedInSecs;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getSelectedDocuments;

/**
 * Utilities to retrieve _lastRev missing update candidates.
 */
public class MissingLastRevSeeker {

    protected final String ROOT_PATH = "/";

    private final DocumentStore store;

    protected final Clock clock;

    private final Predicate<ClusterNodeInfoDocument> isRecoveryNeeded =
            new Predicate<ClusterNodeInfoDocument>() {
        @Override
        public boolean apply(ClusterNodeInfoDocument nodeInfo) {
            return isRecoveryNeeded(nodeInfo);
        }
    };

    public MissingLastRevSeeker(DocumentStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    /**
     * Gets the clusters which potentially need _lastRev recovery.
     *
     * @return the clusters
     */
    @Nonnull
    public Iterable<ClusterNodeInfoDocument> getAllClusters() {
        return ClusterNodeInfoDocument.all(store);
    }

    /**
     * Gets the cluster node info for the given cluster node id.
     *
     * @param clusterId the cluster id
     * @return the cluster node info
     */
    @CheckForNull
    public ClusterNodeInfoDocument getClusterNodeInfo(final int clusterId) {
        // Fetch all documents.
        return store.find(CLUSTER_NODES, String.valueOf(clusterId));
    }

    /**
     * Get the candidates with modified time greater than or equal the specified
     * {@code startTime} in milliseconds since the start of the epoch.
     *
     * @param startTime the start time in milliseconds.
     * @return the candidates
     */
    @Nonnull
    public Iterable<NodeDocument> getCandidates(final long startTime) {
        // Fetch all documents where lastmod >= startTime
        Iterable<NodeDocument> nodes = getSelectedDocuments(store,
                MODIFIED_IN_SECS, getModifiedInSecs(startTime));
        return Iterables.filter(nodes, new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument input) {
                Long modified = (Long) input.get(MODIFIED_IN_SECS);
                return (modified != null && (modified >= getModifiedInSecs(startTime)));
            }
        });
    }

    /**
     * Acquire a recovery lock for the given cluster node info document. This
     * method may break a lock when it determines the cluster node holding the
     * recovery lock is no more active or its lease expired.
     * 
     * @param clusterId
     *            id of the cluster that is going to be recovered
     * @param recoveredBy
     *            id of cluster doing the recovery
     * @return whether the lock has been acquired
     */
    public boolean acquireRecoveryLock(int clusterId, int recoveredBy) {
        return new RecoveryLock(store, clock, clusterId)
                .acquireRecoveryLock(recoveredBy);
    }

    /**
     * Releases the recovery lock on the given {@code clusterId}. If
     * {@code success} is {@code true}, the state of the cluster node entry
     * is reset, otherwise it is left as is. That is, for a cluster node which
     * requires recovery and the recovery process failed, the state will still
     * be active, when this release method is called with {@code success} set
     * to {@code false}.
     *
     * @param clusterId the id of the cluster node that was recovered.
     * @param success whether recovery was successful.
     */
    public void releaseRecoveryLock(int clusterId, boolean success) {
        new RecoveryLock(store, clock, clusterId).releaseRecoveryLock(success);
    }

    public NodeDocument getRoot() {
        return store.find(Collection.NODES, Utils.getIdFromPath(ROOT_PATH));
    }

    public boolean isRecoveryNeeded() {
        return Iterables.any(getAllClusters(), isRecoveryNeeded);
    }

    /**
     * Check if _lastRev recovery needed for this cluster node
     * state is Active and currentTime past the leaseEnd time
     */
    public boolean isRecoveryNeeded(@Nonnull ClusterNodeInfoDocument nodeInfo) {
        return nodeInfo.isActive() && clock.getTime() > nodeInfo.getLeaseEndTime();
    }
}
