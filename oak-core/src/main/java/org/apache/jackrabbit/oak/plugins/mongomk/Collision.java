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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A <code>Collision</code> happens when a commit modifies a node, which was
 * also modified in another branch not visible to the current session. This
 * includes the following situations:
 * <ul>
 * <li>Our commit goes to trunk and another session committed to a branch
 * not yet merged back.</li>
 * <li>Our commit goes to a branch and another session committed to trunk
 * or some other branch.</li>
 * </ul>
 * Other collisions like concurrent commits to trunk are handled earlier and
 * do not require collision marking.
 * See {@link Commit#createOrUpdateNode(DocumentStore, UpdateOp)}.
 */
class Collision {

    private static final Logger LOG = LoggerFactory.getLogger(Collision.class);

    private final NodeDocument document;
    private final String theirRev;
    private final UpdateOp ourOp;
    private final String ourRev;

    Collision(@Nonnull NodeDocument document,
              @Nonnull Revision theirRev,
              @Nonnull UpdateOp ourOp,
              @Nonnull Revision ourRev) {
        this.document = checkNotNull(document);
        this.theirRev = checkNotNull(theirRev).toString();
        this.ourOp = checkNotNull(ourOp);
        this.ourRev = checkNotNull(ourRev).toString();
    }

    /**
     * Marks the collision in the document store. Either our or their
     * revision is annotated with a collision marker. Their revision is
     * marked if it is not yet committed, otherwise our revision is marked.
     * 
     * @param store the document store.
     * @throws MicroKernelException if the mark operation fails.
     */
    void mark(DocumentStore store) throws MicroKernelException {
        // first try to mark their revision
        if (markCommitRoot(document, theirRev, store)) {
            return;
        }
        // their commit wins, we have to mark ourRev
        NodeDocument newDoc = Collection.NODES.newDocument();
        document.deepCopy(newDoc);
        MemoryDocumentStore.applyChanges(newDoc, ourOp);
        if (!markCommitRoot(newDoc, ourRev, store)) {
            throw new MicroKernelException("Unable to annotate our revision "
                    + "with collision marker. Our revision: " + ourRev
                    + ", document:\n" + newDoc.format());
        }
    }

    /**
     * Marks the commit root of the change to the given <code>document</code> in
     * <code>revision</code>.
     * 
     * @param document the MongoDB document.
     * @param revision the revision of the commit to annotated with a collision
     *            marker.
     * @param store the document store.
     * @return <code>true</code> if the commit for the given revision was marked
     *         successfully; <code>false</code> otherwise.
     */
    private static boolean markCommitRoot(@Nonnull NodeDocument document,
                                          @Nonnull String revision,
                                          @Nonnull DocumentStore store) {
        String p = Utils.getPathFromId(document.getId());
        String commitRootPath = null;
        // first check if we can mark the commit with the given revision
        if (document.containsRevision(revision)) {
            if (document.isCommitted(revision)) {
                // already committed
                return false;
            }
            // node is also commit root, but not yet committed
            // i.e. a branch commit, which is not yet merged
            commitRootPath = p;
        } else {
            // next look at commit root
            commitRootPath = document.getCommitRootPath(revision);
            if (commitRootPath == null) {
                throwNoCommitRootException(revision, document);
            }
        }
        // at this point we have a commitRootPath
        UpdateOp op = new UpdateOp(Utils.getIdFromPath(commitRootPath), false);
        document = store.find(Collection.NODES, op.getKey());
        // check commit status of revision
        if (document.isCommitted(revision)) {
            return false;
        }
        op.setMapEntry(NodeDocument.COLLISIONS, revision, true);
        document = store.createOrUpdate(Collection.NODES, op);
        // check again on old document right before our update was applied
        if (document.isCommitted(revision)) {
            return false;
        }
        // otherwise collision marker was set successfully
        LOG.debug("Marked collision on: {} for {} ({})",
                new Object[]{commitRootPath, p, revision});
        return true;
    }
    
    private static void throwNoCommitRootException(@Nonnull String revision,
                                                   @Nonnull Document document)
                                                           throws MicroKernelException {
        throw new MicroKernelException("No commit root for revision: "
                + revision + ", document: " + document.format());
    }
}
