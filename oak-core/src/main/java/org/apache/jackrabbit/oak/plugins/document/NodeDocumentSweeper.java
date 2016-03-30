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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.CheckForNull;

import com.google.common.cache.Cache;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.filter;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.getModifiedInSecs;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.isDeletedEntry;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.removeCommitRoot;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.removeRevision;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setDeletedOnce;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setRevision;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.PROPERTY_OR_DELETED;

final class NodeDocumentSweeper implements BranchCommitListener {

    private static final Logger LOG = LoggerFactory.getLogger(NodeDocumentSweeper.class);

    private static final int INVALIDATE_ENTRY_SIZE = 1000;

    private static final int REV_CACHE_SIZE = 16 * 1024;

    private static final Revision NULL_REV = new Revision(0, 0, 0);

    private final RevisionContext context;

    private final MissingLastRevSeeker seeker;

    private final int clusterId;

    private final UnmergedBranches branches;

    private final List<String> paths = Lists.newArrayList();

    private Revision headRevision;

    private Revision nextSweepHead;

    private final Cache<Revision, Revision> revCache =
            new CacheLIRS<Revision, Revision>(REV_CACHE_SIZE);

    private final Set<Revision> inProgressBranchCommits = newConcurrentHashSet();

    NodeDocumentSweeper(RevisionContext context,
                        MissingLastRevSeeker seeker) {
        this.context = checkNotNull(context);
        this.seeker = checkNotNull(seeker);
        this.clusterId = context.getClusterId();
        this.branches = context.getBranches();
    }

    @CheckForNull
    Revision sweep(NodeDocumentSweepListener listener)
            throws DocumentStoreException {
        paths.clear();
        revCache.invalidateAll();
        inProgressBranchCommits.clear();

        branches.addListener(this);
        try {
            return performSweep(listener);
        } finally {
            branches.removeListener(this);
        }
    }

    //---------------------< BranchCommitListener >-----------------------------

    @Override
    public void branchRevisionCreated(Revision branchCommitRevision) {
        inProgressBranchCommits.add(branchCommitRevision.asTrunkRevision());
    }

    //----------------------------< internal >----------------------------------

    @CheckForNull
    private Revision performSweep(NodeDocumentSweepListener listener)
            throws DocumentStoreException {
        headRevision = context.getHeadRevision().getRevision(clusterId);
        if (headRevision == null) {
            LOG.warn("Head revision {} does not have an entry for " +
                    "clusterId {}. Skipping background sweeping of " +
                    "documents.", context.getHeadRevision(), clusterId);
            return null;
        }
        NodeDocument rootDoc = seeker.getRoot();
        RevisionVector sweepRevs = rootDoc.getSweepRevisions();
        Revision lastSweepHead = sweepRevs.getRevision(clusterId);
        if (lastSweepHead == null) {
            // sweep all
            lastSweepHead = new Revision(0, 0, clusterId);
        }
        // only sweep documents when the _modified time changed
        long lastSweepTick = getModifiedInSecs(lastSweepHead.getTimestamp());
        long currentTick = getModifiedInSecs(context.getClock().getTime());
        if (lastSweepTick == currentTick) {
            return null;
        }

        LOG.info("Starting document sweep. Head: {}, starting at {}",
                headRevision, lastSweepHead);

        nextSweepHead = headRevision;
        for (NodeDocument doc : seeker.getCandidates(lastSweepHead.getTimestamp())) {
            UpdateOp op = sweepOne(doc);
            if (op != null) {
                listener.sweepUpdate(op);
                paths.add(doc.getPath());
                if (paths.size() >= INVALIDATE_ENTRY_SIZE) {
                    listener.invalidate(paths);
                    paths.clear();
                }
            }
        }
        if (!paths.isEmpty()) {
            listener.invalidate(paths);
            paths.clear();
        }
        LOG.info("Document sweep finished");
        return nextSweepHead;
    }

    private UpdateOp sweepOne(NodeDocument doc) throws DocumentStoreException {
        UpdateOp op = createUpdateOp(doc);
        for (String property : filter(doc.keySet(), PROPERTY_OR_DELETED)) {
            Map<Revision, String> valueMap = doc.getLocalMap(property);
            for (Map.Entry<Revision, String> entry : valueMap.entrySet()) {
                Revision rev = entry.getKey();
                // only consider change for this cluster node
                if (rev.getClusterId() != clusterId) {
                    continue;
                }
                Revision cRev = getCommitRevision(doc, rev);
                if (cRev == null) {
                    uncommitted(doc, property, rev, op);
                } else if (cRev.equals(rev)) {
                    committed(property, rev, op);
                } else {
                    committedBranch(doc, property, rev, cRev, op);
                }
            }
        }
        return op.hasChanges() ? op : null;
    }

    private void uncommitted(NodeDocument doc,
                             String property,
                             Revision rev,
                             UpdateOp op) {
        if (headRevision.compareRevisionTime(rev) <= 0) {
            return;
        }
        if (isInProgressBranchCommit(rev)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unmerged branch commit on {}, {} @ {}",
                        op.getId(), property, rev);
            }
            pushBackNextSweepHead(rev);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Uncommitted change on {}, {} @ {}",
                        op.getId(), property, rev);
            }
            op.removeMapEntry(property, rev);
            if (doc.getLocalCommitRoot().containsKey(rev)) {
                removeCommitRoot(op, rev);
            } else {
                removeRevision(op, rev);
            }
            // set _deletedOnce if uncommitted change is a failed create
            // node operation and doc does not have _deletedOnce yet
            if (isDeletedEntry(property)
                    && !doc.wasDeletedOnce()
                    && "false".equals(doc.getLocalDeleted().get(rev))) {
                setDeletedOnce(op);
            }
        }
    }

    private boolean isInProgressBranchCommit(Revision rev) {
        return inProgressBranchCommits.contains(rev);
    }

    private void committed(String property,
                           Revision rev,
                           UpdateOp op) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committed change on {}, {} @ {}",
                    op.getId(), property, rev);
        }
    }

    private void committedBranch(NodeDocument doc,
                                 String property,
                                 Revision rev,
                                 Revision cRev,
                                 UpdateOp op) {
        boolean newerThanHead = cRev.compareRevisionTime(headRevision) > 0;
        if (LOG.isDebugEnabled()) {
            String msg = newerThanHead ? " (newer than head)" : "";
            LOG.debug("Committed branch change on {}, {} @ {}/{}{}",
                    op.getId(), property, rev, cRev, msg);
        }
        // rewrite the change if commit rev is visible from head
        // otherwise partial branch may be rewritten
        if (!newerThanHead) {
            op.removeMapEntry(property, rev);
            op.setMapEntry(property, cRev, doc.getLocalMap(property).get(rev));
            if (doc.getLocalCommitRoot().containsKey(rev)) {
                removeCommitRoot(op, rev);
            }
            setRevision(op, cRev, "c");
        } else {
            pushBackNextSweepHead(rev);
        }
    }

    private static UpdateOp createUpdateOp(NodeDocument doc) {
        return new UpdateOp(doc.getId(), false);
    }

    @CheckForNull
    private Revision getCommitRevision(final NodeDocument doc,
                                       final Revision rev)
            throws DocumentStoreException {
        try {
            Revision cRev = revCache.get(rev, new Callable<Revision>() {
                @Override
                public Revision call() throws Exception {
                    Revision r = doc.getCommitRevision(rev);
                    if (r == null) {
                        r = NULL_REV;
                    }
                    return r;
                }
            });
            return cRev == NULL_REV ? null : cRev;
        } catch (ExecutionException e) {
            throw DocumentStoreException.convert(e.getCause());
        }
    }

    private void pushBackNextSweepHead(Revision rev) {
        // nextSweepHead must be less than rev
        // therefore, decrement the revision timestamp by one
        rev = new Revision(rev.getTimestamp() - 1, 0, rev.getClusterId());
        nextSweepHead = Utils.min(rev, nextSweepHead);
    }
}
