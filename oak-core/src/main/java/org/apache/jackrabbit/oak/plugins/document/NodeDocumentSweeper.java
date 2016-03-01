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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.CheckForNull;

import com.google.common.cache.Cache;

import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.filter;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.isDeletedEntry;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.removeCommitRoot;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.removeRevision;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setDeletedOnce;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setRevision;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.PROPERTY_OR_DELETED;

class NodeDocumentSweeper {

    private static final Logger LOG = LoggerFactory.getLogger(NodeDocumentSweeper.class);

    private static final int REV_CACHE_SIZE = 16 * 1024;

    private static final Revision NULL_REV = new Revision(0, 0, 0);

    private final Revision headRevision;

    private final Revision lastSweepHead;

    private Revision nextSweepHead;

    private final int clusterId;

    private final MissingLastRevSeeker seeker;

    private final UnmergedBranches branches;

    private final Cache<Revision, Revision> revCache =
            new CacheLIRS<Revision, Revision>(REV_CACHE_SIZE);

    NodeDocumentSweeper(Revision headRevision,
                        Revision lastSweepHead,
                        MissingLastRevSeeker seeker,
                        UnmergedBranches branches) {
        checkArgument(headRevision.getClusterId() == lastSweepHead.getClusterId());
        checkArgument(headRevision.compareRevisionTime(lastSweepHead) >= 0);
        this.headRevision = headRevision;
        this.lastSweepHead = lastSweepHead;
        this.clusterId = headRevision.getClusterId();
        this.seeker = seeker;
        this.branches = branches;
    }

    Revision sweep(NodeDocumentSweepListener listener) throws DocumentStoreException {
        nextSweepHead = headRevision;
        for (NodeDocument doc : seeker.getCandidates(lastSweepHead.getTimestamp())) {
            UpdateOp op = sweepOne(doc);
            if (op != null) {
                listener.sweepUpdate(op);
            }
        }
        return nextSweepHead;
    }

    //----------------------------< internal >----------------------------------

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
        if (branches.getBranchCommit(rev) == null) {
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
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unmerged branch commit on {}, {} @ {}",
                        op.getId(), property, rev);
            }
            nextSweepHead = Utils.min(rev, nextSweepHead);
        }
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committed branch change on {}, {} @ {}/{}",
                    op.getId(), property, rev, cRev);
        }
        // rewrite the change
        op.removeMapEntry(property, rev);
        op.setMapEntry(property, cRev, doc.getLocalMap(property).get(rev));
        if (doc.getLocalCommitRoot().containsKey(rev)) {
            removeCommitRoot(op, rev);
        }
        setRevision(op, cRev, "c");
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
}
