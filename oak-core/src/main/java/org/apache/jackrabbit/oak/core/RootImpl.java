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
package org.apache.jackrabbit.oak.core;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ConflictHandler;
import org.apache.jackrabbit.oak.api.ConflictHandler.Resolution;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.ConflictHandler.Resolution.MERGED;
import static org.apache.jackrabbit.oak.api.ConflictHandler.Resolution.OURS;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.util.Iterators.toList;

public class RootImpl implements Root {
    static final Logger log = LoggerFactory.getLogger(RootImpl.class);

    /**
     * Number of {@link #setCurrentRootState} calls for which changes
     * are kept in memory.
     */
    private static final int PURGE_LIMIT = 100;

    /** The underlying store to which this root belongs */
    private final NodeStore store;

    /** Current branch this root operates on */
    private NodeStoreBranch branch;

    /** Actual root element of the {@code Tree} */
    private TreeImpl root;

    /** Current root node state if changed */
    private NodeState currentRootState;

    /**
     * Number of {@link #setCurrentRootState} occurred so since the lase
     * purge.
     */
    private int modCount;

    /**
     * Listeners which needs to be modified as soon as {@link #purgePendingChanges()}
     * is called. Listeners are removed from this list after being called. If further
     * notifications are required, they need to explicitly re-register.
     *
     * The {@link TreeImpl} instances us this mechanism to dispose of its associated
     * {@link NodeStateBuilder} on purge. Keeping a reference on those {@code TreeImpl}
     * instances {@code NodeStateBuilder} (i.e. those which are modified) prevents them
     * from being prematurely garbage collected.
     */
    private List<PurgeListener> purgePurgeListeners = new ArrayList<PurgeListener>();

    /**
     * Purge listener.
     * @see #purgePurgeListeners
     */
    public interface PurgeListener {
        void purged();
    }

    /**
     * New instance bases on a given {@link NodeStore} and a workspace
     * @param store  node store
     * @param workspaceName  name of the workspace
     * TODO: add support for multiple workspaces. See OAK-118
     */
    @SuppressWarnings("UnusedParameters")
    public RootImpl(NodeStore store, String workspaceName) {
        this.store = store;
        branch = store.branch();
        root = TreeImpl.createRoot(this);
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        purgePendingChanges();
        TreeImpl source = getChild(sourcePath);
        if (source == null) {
            return false;
        }
        TreeImpl destParent = getChild(getParentPath(destPath));
        if (destParent == null) {
            return false;
        }

        String destName = getName(destPath);
        if (source.moveTo(destParent, destName)) {
            currentRootState = null;
            return branch.move(sourcePath, destPath);
        }
        else {
            currentRootState = null;
            return false;
        }
    }

    @Override
    public boolean copy(String sourcePath, String destPath) {
        purgePendingChanges();
        return branch.copy(sourcePath, destPath);
    }

    @Override
    public Tree getTree(String path) {
        return getChild(path);
    }

    @Override
    public void rebase(ConflictHandler conflictHandler) {
        purgePendingChanges();
        NodeState base = getBaseState();
        NodeState head = getCurrentRootState();
        currentRootState = null;
        branch = store.branch();
        root = TreeImpl.createRoot(this);
        merge(base, head, root, conflictHandler);
    }

    @Override
    public void refresh() {
        currentRootState = null;
        branch = store.branch();
        root = TreeImpl.createRoot(this);
    }

    @Override
    public void commit(ConflictHandler conflictHandler) throws CommitFailedException {
        rebase(conflictHandler);
        purgePendingChanges();
        branch.merge();
        branch = store.branch();
        root = TreeImpl.createRoot(this);
    }

    @Override
    public boolean hasPendingChanges() {
        return !getBaseState().equals(getCurrentRootState());
    }

    /**
     * Add a {@code PurgeListener} to this instance. Listeners are automatically
     * unregistered after having been called. If further notifications are required,
     * they need to explicitly re-register.
     * @param purgeListener  listener
     */
    public void addListener(PurgeListener purgeListener) {
        purgePurgeListeners.add(purgeListener);
    }

    /**
     * Returns the current root node state
     * @return root node state
     */
    @Nonnull
    public NodeState getCurrentRootState() {
        if (currentRootState == null) {
            currentRootState = branch.getRoot();
        }
        return currentRootState;
    }

    /**
     * Returns the node state from which the current branch was created.
     * @return base node state
     */
    @Nonnull
    public NodeState getBaseState() {
        return branch.getBase();
    }

    /**
     * Returns a builder for constructing a new or modified node state.
     * The builder is initialized with all the properties and child nodes
     * from the given base node state.
     *
     * @param nodeState  base node state, or {@code null} for building new nodes
     * @return  builder instance
     */
    @Nonnull
    public NodeStateBuilder getBuilder(NodeState nodeState) {
        return store.getBuilder(nodeState);
    }

    /**
     * Set the current root node state
     *
     * @param nodeState  node state representing the modified root state
     */
    public void setCurrentRootState(NodeState nodeState) {
        currentRootState = nodeState;
        modCount++;
        if (needsPurging()) {
            purgePendingChanges();
        }
    }

    /**
     * Purge all pending changes to the underlying {@link NodeStoreBranch}.
     * All registered {@link PurgeListener}s are notified.
     */
    public void purgePendingChanges() {
        if (currentRootState != null) {
            branch.setRoot(currentRootState);
            currentRootState = null;
            notifyListeners();
        }
    }

    /**
     * Compares the given two node states. Any found differences are
     * reported by calling the relevant added, changed or deleted methods
     * of the given handler.
     *
     * @param before node state before changes
     * @param after node state after changes
     * @param diffHandler handler of node state differences
     */
    public void compare(NodeState before, NodeState after, NodeStateDiff diffHandler) {
        store.compare(before, after, diffHandler);
    }

    //------------------------------------------------------------< private >---

    // TODO better way to determine purge limit
    private boolean needsPurging() {
        if (modCount > PURGE_LIMIT) {
            modCount = 0;
            return true;
        }
        else {
            return false;
        }
    }

    private void notifyListeners() {
        List<PurgeListener> purgeListeners = this.purgePurgeListeners;
        this.purgePurgeListeners = new ArrayList<PurgeListener>();

        for (PurgeListener purgeListener : purgeListeners) {
            purgeListener.purged();
        }
    }

    private TreeImpl getRoot() {
        return root;
    }

    /**
     * Get a tree for the child identified by {@code path}
     * @param path  the path to the child
     * @return  a {@link Tree} instance for the child
     *          at {@code path} or {@code null} if no such item exits.
     */
    private TreeImpl getChild(String path) {
        TreeImpl child = getRoot();
        for (String name : elements(path)) {
            child = child.getChild(name);
            if (child == null) {
                return null;
            }
        }
        return child;
    }

    private void merge(NodeState fromState, NodeState toState, final TreeImpl target,
            final ConflictHandler conflictHandler) {

        assert target != null;

        store.compare(fromState, toState, new NodeStateDiff() {
            @Override
            public void propertyAdded(PropertyState after) {
                Resolution resolution;
                PropertyState p = target.getProperty(after.getName());

                if (p == null) {
                    resolution = OURS;
                }
                else {
                    resolution = conflictHandler.addExistingProperty(target, after, p);
                }

                switch (resolution) {
                    case OURS:
                        setProperty(target, after);
                        break;
                    case THEIRS:
                    case MERGED:
                        break;
                }
            }

            @Override
            public void propertyChanged(PropertyState before, PropertyState after) {
                assert before.getName().equals(after.getName());

                Resolution resolution;
                PropertyState p = target.getProperty(after.getName());

                if (p == null) {
                    resolution = conflictHandler.changeDeletedProperty(target, after);
                }
                else if (before.equals(p)) {
                    resolution = OURS;
                }
                else {
                    resolution = conflictHandler.changeChangedProperty(target, after, p);
                }

                switch (resolution) {
                    case OURS:
                        setProperty(target, after);
                        break;
                    case THEIRS:
                    case MERGED:
                        break;
                }
            }

            @Override
            public void propertyDeleted(PropertyState before) {
                Resolution resolution;
                PropertyState p = target.getProperty(before.getName());

                if (before.equals(p)) {
                    resolution = OURS;
                }
                else if (p == null) {
                    resolution = conflictHandler.deleteDeletedProperty(target, before);
                }
                else {
                    resolution = conflictHandler.deleteChangedProperty(target, p);
                }

                switch (resolution) {
                    case OURS:
                        target.removeProperty(before.getName());
                        break;
                    case THEIRS:
                    case MERGED:
                        break;
                }
            }

            @Override
            public void childNodeAdded(String name, NodeState after) {
                Resolution resolution;
                TreeImpl n = target.getChild(name);

                if (n == null) {
                    resolution = OURS;
                }
                else {
                    resolution = conflictHandler.addExistingNode(target, name, after, n.getNodeState());
                }

                switch (resolution) {
                    case OURS:
                        addChild(target, name, after);
                        break;
                    case THEIRS:
                    case MERGED:
                        break;
                }
            }

            @Override
            public void childNodeChanged(String name, NodeState before, NodeState after) {
                Resolution resolution;
                TreeImpl n = target.getChild(name);

                if (n == null) {
                    resolution = conflictHandler.changeDeletedNode(target, name, after);
                }
                else {
                    merge(before, after, n, conflictHandler);
                    resolution = MERGED;
                }

                switch (resolution) {
                    case OURS:
                        addChild(target, name, after);
                        break;
                    case THEIRS:
                    case MERGED:
                        break;
                }
            }

            @Override
            public void childNodeDeleted(String name, NodeState before) {
                Resolution resolution;
                TreeImpl n = target.getChild(name);

                if (n == null) {
                    resolution = conflictHandler.deleteDeletedNode(target, name);
                }
                else if (before.equals(n.getNodeState())) {
                    resolution = OURS;
                }
                else {
                    resolution = conflictHandler.deleteChangedNode(target, name, n.getNodeState());
                }

                switch (resolution) {
                    case OURS:
                        target.removeChild(name);
                        break;
                    case THEIRS:
                    case MERGED:
                        break;
                }
            }

            private void addChild(Tree target, String name, NodeState state) {
                Tree child = target.addChild(name);
                for (PropertyState property : state.getProperties()) {
                    setProperty(child, property);
                }
                for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                    addChild(child, entry.getName(), entry.getNodeState());
                }
            }

            private void setProperty(Tree target, PropertyState property) {
                if (property.isArray()) {
                    target.setProperty(property.getName(),
                            toList(property.getValues(), new ArrayList<CoreValue>()));
                }
                else {
                    target.setProperty(property.getName(), property.getValue());
                }
            }

        });
    }

}
