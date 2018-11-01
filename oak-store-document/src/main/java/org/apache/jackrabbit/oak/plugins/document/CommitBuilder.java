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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A builder for a commit, translating modifications into {@link UpdateOp}s.
 */
class CommitBuilder {

    /** A marker revision when the commit is initially built */
    private static final Revision PSEUDO_COMMIT_REVISION = new Revision(Long.MIN_VALUE, 0, 0);

    private final DocumentNodeStore nodeStore;
    private final Revision revision;
    private final RevisionVector baseRevision;
    private final Map<String, UpdateOp> operations = new LinkedHashMap<>();

    private final Set<String> addedNodes = new HashSet<>();
    private final Set<String> removedNodes = new HashSet<>();

    /** Set of all nodes which have binary properties. **/
    private final Set<String> nodesWithBinaries = new HashSet<>();
    private final Map<String, String> bundledNodes = new HashMap<>();

    /**
     * Creates a new builder with a pseudo commit revision. Building the commit
     * must be done by calling {@link #build(Revision)}.
     *
     * @param nodeStore the node store.
     * @param baseRevision the base revision if available.
     */
    CommitBuilder(@NotNull DocumentNodeStore nodeStore,
                  @Nullable RevisionVector baseRevision) {
        this(nodeStore, PSEUDO_COMMIT_REVISION, baseRevision);
    }

    CommitBuilder(@NotNull DocumentNodeStore nodeStore,
                  @NotNull Revision revision,
                  @Nullable RevisionVector baseRevision) {
        this.nodeStore = checkNotNull(nodeStore);
        this.revision = checkNotNull(revision);
        this.baseRevision = baseRevision;
    }

    @NotNull
    Revision getRevision() {
        return revision;
    }

    @Nullable
    RevisionVector getBaseRevision() {
        return baseRevision;
    }

    CommitBuilder addNode(String path) {
        addNode(path, new DocumentNodeState(nodeStore, path, new RevisionVector(revision))
                .asOperation(revision));
        return this;
    }

    CommitBuilder addNode(String path, UpdateOp op) {
        if (operations.containsKey(path)) {
            String msg = "Node already added: " + path;
            throw new DocumentStoreException(msg);
        }
        if (isBranchCommit()) {
            NodeDocument.setBranchCommit(op, revision);
        }
        operations.put(path, op);
        addedNodes.add(path);
        return this;
    }

    CommitBuilder addBundledNode(String path, String bundlingRootPath) {
        bundledNodes.put(path, bundlingRootPath);
        return this;
    }

    CommitBuilder removeNode(String path, NodeState state) {
        removedNodes.add(path);
        UpdateOp op = getUpdateOperationForNode(path);
        op.setDelete(true);
        NodeDocument.setDeleted(op, revision, true);
        for (PropertyState p : state.getProperties()) {
            updateProperty(path, p.getName(), null);
        }
        return this;
    }

    CommitBuilder updateProperty(String path, String propertyName, String value) {
        UpdateOp op = getUpdateOperationForNode(path);
        String key = Utils.escapePropertyName(propertyName);
        op.setMapEntry(key, revision, value);
        return this;
    }

    CommitBuilder markNodeHavingBinary(String path) {
        nodesWithBinaries.add(path);
        return this;
    }

    Commit build() {
        if (PSEUDO_COMMIT_REVISION.equals(revision)) {
            String msg = "Cannot build a commit with a pseudo commit revision";
            throw new IllegalStateException(msg);
        }
        return new Commit(nodeStore, revision, baseRevision, operations,
                addedNodes, removedNodes, nodesWithBinaries, bundledNodes);
    }

    Commit build(@NotNull Revision revision) {
        Revision from = this.revision;
        Map<String, UpdateOp> operations = Maps.transformValues(
                this.operations, op -> rewrite(op, from, revision));
        return new Commit(nodeStore, revision, baseRevision, operations,
                addedNodes, removedNodes, nodesWithBinaries, bundledNodes);
    }

    //-------------------------< internal >-------------------------------------

    private UpdateOp getUpdateOperationForNode(String path) {
        UpdateOp op = operations.get(path);
        if (op == null) {
            op = createUpdateOp(path, revision, isBranchCommit());
            operations.put(path, op);
        }
        return op;
    }

    private static UpdateOp createUpdateOp(String path,
                                           Revision revision,
                                           boolean isBranch) {
        String id = Utils.getIdFromPath(path);
        UpdateOp op = new UpdateOp(id, false);
        NodeDocument.setModified(op, revision);
        if (isBranch) {
            NodeDocument.setBranchCommit(op, revision);
        }
        return op;
    }

    /**
     * @return {@code true} if this is a branch commit.
     */
    private boolean isBranchCommit() {
        return baseRevision != null && baseRevision.isBranch();
    }

    private static UpdateOp rewrite(UpdateOp up, Revision from, Revision to) {
        if (up == null) {
            return null;
        }
        Map<UpdateOp.Key, UpdateOp.Operation> changes = Maps.newHashMap();
        for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> entry : up.getChanges().entrySet()) {
            UpdateOp.Key k = entry.getKey();
            UpdateOp.Operation op = entry.getValue();
            if (from.equals(k.getRevision())) {
                k = new UpdateOp.Key(k.getName(), to);
            } else if (NodeDocument.MODIFIED_IN_SECS.equals(k.getName())) {
                op = new UpdateOp.Operation(op.type, NodeDocument.getModifiedInSecs(to.getTimestamp()));
            }
            changes.put(k, op);
        }

        Map<UpdateOp.Key, UpdateOp.Condition> conditions = null;
        if (!up.getConditions().isEmpty()) {
            conditions = Maps.newHashMap();
            for (Map.Entry<UpdateOp.Key, UpdateOp.Condition> entry : up.getConditions().entrySet()) {
                UpdateOp.Key k = entry.getKey();
                if (from.equals(k.getRevision())) {
                    k = new UpdateOp.Key(k.getName(), to);
                }
                conditions.put(k, entry.getValue());
            }
        }

        return new UpdateOp(up.getId(), up.isNew(), up.isDelete(), changes, conditions);
    }
}
