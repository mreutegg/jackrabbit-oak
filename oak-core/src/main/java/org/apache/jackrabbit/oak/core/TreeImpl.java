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

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

public class TreeImpl implements Tree {

    /**
     * Internal and hidden property that contains the child order
     */
    public static final String OAK_CHILD_ORDER = ":childOrder";

    /**
     * Underlying {@code Root} of this {@code Tree} instance
     */
    private final RootImpl root;

    /**
     * The {@code NodeBuilder} for the underlying node state
     */
    private final NodeBuilder nodeBuilder;

    /**
     * The node state this tree is based on. {@code null} if this is a newly added tree.
     */
    private final NodeState baseState;

    /**
     * Parent of this tree. Null for the root.
     */
    private TreeImpl parent;

    /**
     * Name of this tree
     */
    private String name;

    TreeImpl(RootImpl root) {
        this.root = checkNotNull(root);
        this.name = "";
        this.nodeBuilder = root.createRootBuilder();
        this.baseState = root.getBaseState();
    }

    private TreeImpl(RootImpl root, TreeImpl parent, String name) {
        this.root = checkNotNull(root);
        this.parent = checkNotNull(parent);
        this.name = checkNotNull(name);
        this.nodeBuilder = parent.getNodeBuilder().child(name);

        if (parent.baseState == null) {
            this.baseState = null;
        } else {
            this.baseState = parent.baseState.getChildNode(name);
        }
    }

    @Override
    public String getName() {
        root.checkLive();
        return name;
    }

    @Override
    public boolean isRoot() {
        root.checkLive();
        return parent == null;
    }

    @Override
    public String getPath() {
        root.checkLive();
        if (isRoot()) {
            // shortcut
            return "/";
        }

        StringBuilder sb = new StringBuilder();
        buildPath(sb);
        return sb.toString();
    }

    @Override
    public Tree getParent() {
        root.checkLive();
        if (parent != null && canRead(parent)) {
            return parent;
        } else {
            return null;
        }
    }

    @Override
    public PropertyState getProperty(String name) {
        root.checkLive();
        PropertyState property = internalGetProperty(name);
        if (canRead(property)) {
            return property;
        } else {
            return null;
        }
    }

    @Override
    public Status getPropertyStatus(String name) {
        // TODO: see OAK-212
        root.checkLive();
        Status nodeStatus = getStatus();
        if (nodeStatus == Status.NEW) {
            return (hasProperty(name)) ? Status.NEW : null;
        } else if (nodeStatus == Status.DISCONNECTED) {
            return Status.DISCONNECTED;
        } else {
            PropertyState head = internalGetProperty(name);
            if (head != null && !canRead(head)) {
                // no permission to read status information for existing property
                return null;
            }

            NodeState parentBase = getBaseState();
            PropertyState base = parentBase == null ? null : parentBase.getProperty(name);
            if (head == null) {
                return (base == null) ? null : Status.DISCONNECTED;
            } else {
                if (base == null) {
                    return Status.NEW;
                } else if (head.equals(base)) {
                    return Status.EXISTING;
                } else {
                    return Status.MODIFIED;
                }
            }
        }
    }

    @Override
    public boolean hasProperty(String name) {
        root.checkLive();
        return getProperty(name) != null;
    }

    @Override
    public long getPropertyCount() {
        root.checkLive();
        return Iterables.size(getProperties());
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        root.checkLive();
        return Iterables.filter(nodeBuilder.getProperties(),
                new Predicate<PropertyState>() {
                    @Override
                    public boolean apply(PropertyState propertyState) {
                        return canRead(propertyState);
                    }
                });
    }

    @Override
    public TreeImpl getChild(@Nonnull String name) {
        checkNotNull(name);
        root.checkLive();
        TreeImpl child = internalGetChild(name);
        if (child != null && canRead(child)) {
            return child;
        } else {
            return null;
        }
    }

    private boolean isDisconnected() {
        if (isRoot()) {
            return false;
        }
        if (parent.nodeBuilder == null) {
            return false;
        }
        if (!parent.nodeBuilder.isConnected()) {
            return true;
        }
        return !nodeBuilder.isConnected();
    }

    @Override
    public Status getStatus() {
        root.checkLive();

        if (isDisconnected()) {
            return Status.DISCONNECTED;
        }

        if (nodeBuilder.isNew()) {
            return Status.NEW;
        } else if (nodeBuilder.isModified()) {
            return Status.MODIFIED;
        } else {
            return Status.EXISTING;
        }
    }

    @Override
    public boolean hasChild(@Nonnull String name) {
        return getChild(name) != null;
    }

    @Override
    public long getChildrenCount() {
        // TODO: make sure cnt respects access control
        root.checkLive();
        return nodeBuilder.getChildNodeCount();
    }

    @Override
    public Iterable<Tree> getChildren() {
        root.checkLive();
        Iterable<String> childNames;
        if (hasOrderableChildren()) {
            childNames = getOrderedChildNames();
        } else {
            childNames = nodeBuilder.getChildNodeNames();
        }
        return Iterables.filter(Iterables.transform(
                childNames,
                new Function<String, Tree>() {
                    @Override
                    public Tree apply(String input) {
                        return new TreeImpl(root, TreeImpl.this, input);
                    }
                }),
                new Predicate<Tree>() {
                    @Override
                    public boolean apply(Tree tree) {
                        return tree != null && canRead(tree);
                    }
                });
    }

    @Override
    public Tree addChild(String name) {
        root.checkLive();
        if (!hasChild(name)) {
            nodeBuilder.child(name);
            if (hasOrderableChildren()) {
                nodeBuilder.setProperty(
                        MemoryPropertyBuilder.copy(Type.STRING, internalGetProperty(OAK_CHILD_ORDER))
                                .addValue(name)
                                .getPropertyState());
            }
            root.updated();
        }

        TreeImpl child = new TreeImpl(root, this, name);

        // Make sure to allocate the node builder for new nodes in order to correctly
        // track removes and moves. See OAK-621
        return child;
    }

    @Override
    public void setOrderableChildren(boolean enable) {
        root.checkLive();
        if (enable) {
            ensureChildOrderProperty();
        } else {
            nodeBuilder.removeProperty(OAK_CHILD_ORDER);
        }
    }

    @Override
    public boolean remove() {
        root.checkLive();
        if (isDisconnected()) {
            throw new IllegalStateException("Cannot remove a disconnected tree");
        }

        if (!isRoot() && parent.hasChild(name)) {
            NodeBuilder parentBuilder = parent.nodeBuilder;
            parentBuilder.removeNode(name);
            if (parent.hasOrderableChildren()) {
                parentBuilder.setProperty(
                        MemoryPropertyBuilder.copy(Type.STRING, parent.internalGetProperty(OAK_CHILD_ORDER))
                                .removeValue(name)
                                .getPropertyState()
                );
            }
            root.updated();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean orderBefore(final String name) {
        root.checkLive();
        if (isRoot()) {
            // root does not have siblings
            return false;
        }
        if (name != null && !parent.hasChild(name)) {
            // so such sibling or not accessible
            return false;
        }
        // perform the reorder
        parent.ensureChildOrderProperty();
        // all siblings but not this one
        Iterable<String> filtered = Iterables.filter(
                parent.getOrderedChildNames(),
                new Predicate<String>() {
                    @Override
                    public boolean apply(@Nullable String input) {
                        return !TreeImpl.this.getName().equals(input);
                    }
                });
        // create head and tail
        Iterable<String> head;
        Iterable<String> tail;
        if (name == null) {
            head = filtered;
            tail = Collections.emptyList();
        } else {
            int idx = Iterables.indexOf(filtered, new Predicate<String>() {
                @Override
                public boolean apply(@Nullable String input) {
                    return name.equals(input);
                }
            });
            head = Iterables.limit(filtered, idx);
            tail = Iterables.skip(filtered, idx);
        }
        // concatenate head, this name and tail
        parent.nodeBuilder.setProperty(MultiStringPropertyState.stringProperty(OAK_CHILD_ORDER, Iterables.concat(head, Collections.singleton(getName()), tail))
        );
        root.updated();
        return true;
    }

    @Override
    public void setProperty(PropertyState property) {
        root.checkLive();
        nodeBuilder.setProperty(property);
        root.updated();
    }

    @Override
    public <T> void setProperty(String name, T value) {
        root.checkLive();
        nodeBuilder.setProperty(name, value);
        root.updated();
    }

    @Override
    public <T> void setProperty(String name, T value, Type<T> type) {
        root.checkLive();
        nodeBuilder.setProperty(name, value, type);
        root.updated();
    }

    @Override
    public void removeProperty(String name) {
        root.checkLive();
        nodeBuilder.removeProperty(name);
        root.updated();
    }

    @Override
    public TreeLocation getLocation() {
        root.checkLive();
        return new NodeLocation(this);
    }

    //-----------------------------------------------------------< internal >---

    @Nonnull
    NodeBuilder getNodeBuilder() {
        return nodeBuilder;
    }

    @CheckForNull
    NodeState getBaseState() {
        return baseState;
    }

    @Nonnull
    NodeState getNodeState() {
        return nodeBuilder.getNodeState();
    }

    /**
     * Move this tree to the parent at {@code destParent} with the new name
     * {@code destName}.
     *
     * @param destParent new parent for this tree
     * @param destName   new name for this tree
     */
    void moveTo(TreeImpl destParent, String destName) {
        if (isDisconnected()) {
            throw new IllegalStateException("Cannot move a disconnected tree");
        }

        name = destName;
        parent = destParent;
    }

    /**
     * Get a tree for the tree identified by {@code path}.
     *
     * @param path the path to the child
     * @return a {@link Tree} instance for the child at {@code path} or
     *         {@code null} if no such tree exits or if the tree is not accessible.
     */
    @CheckForNull
    TreeImpl getTree(String path) {
        checkArgument(path.startsWith("/"));
        TreeImpl child = this;
        for (String name : elements(path)) {
            child = child.internalGetChild(name);
            if (child == null) {
                return null;
            }
        }
        return (canRead(child)) ? child : null;
    }

    /**
     * Update the child order with children that have been removed or added.
     * Added children are appended to the end of the {@link #OAK_CHILD_ORDER}
     * property.
     */
    void updateChildOrder() {
        if (!hasOrderableChildren()) {
            return;
        }
        Set<String> names = Sets.newLinkedHashSet();
        for (String name : getOrderedChildNames()) {
            if (nodeBuilder.hasChildNode(name)) {
                names.add(name);
            }
        }
        for (String name : nodeBuilder.getChildNodeNames()) {
            names.add(name);
        }
        PropertyBuilder<String> builder = MemoryPropertyBuilder.array(
                Type.STRING, OAK_CHILD_ORDER);
        builder.setValues(names);
        nodeBuilder.setProperty(builder.getPropertyState());
    }

    @Nonnull
    String getIdentifier() {
        PropertyState property = internalGetProperty(JcrConstants.JCR_UUID);
        if (property != null) {
            return property.getValue(STRING);
        } else if (parent == null) {
            return "/";
        } else {
            return PathUtils.concat(parent.getIdentifier(), name);
        }
    }

    //------------------------------------------------------------< private >---

    private TreeImpl internalGetChild(String childName) {
        return nodeBuilder.hasChildNode(childName)
                ? new TreeImpl(root, this, childName)
                : null;
    }

    private PropertyState internalGetProperty(String propertyName) {
        return nodeBuilder.getProperty(propertyName);
    }

    private void buildPath(StringBuilder sb) {
        if (!isRoot()) {
            parent.buildPath(sb);
            sb.append('/').append(name);
        }
    }

    private boolean canRead(Tree tree) {
        // FIXME: access control eval must have full access to the tree
        // FIXME: special handling for access control item and version content
        return root.getPermissionProvider().canRead(tree);
    }

    private boolean canRead(PropertyState property) {
        // FIXME: access control eval must have full access to the tree/property
        // FIXME: special handling for access control item and version content
        return (property != null)
                && root.getPermissionProvider().canRead(this, property)
                && !NodeStateUtils.isHidden(property.getName());
    }

    /**
     * @return {@code true} if this tree has orderable children;
     *         {@code false} otherwise.
     */
    private boolean hasOrderableChildren() {
        return internalGetProperty(OAK_CHILD_ORDER) != null;
    }

    /**
     * Returns the ordered child names. This method must only be called when
     * this tree {@link #hasOrderableChildren()}.
     *
     * @return the ordered child names.
     */
    private Iterable<String> getOrderedChildNames() {
        assert hasOrderableChildren();
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    final PropertyState childOrder = internalGetProperty(OAK_CHILD_ORDER);
                    int index = 0;

                    @Override
                    public boolean hasNext() {
                        return index < childOrder.count();
                    }

                    @Override
                    public String next() {
                        return childOrder.getValue(Type.STRING, index++);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Ensures that the {@link #OAK_CHILD_ORDER} exists. This method will create
     * the property if it doesn't exist and initialize the value with the names
     * of the children as returned by {@link NodeBuilder#getChildNodeNames()}.
     */
    private void ensureChildOrderProperty() {
        PropertyState childOrder = nodeBuilder.getProperty(OAK_CHILD_ORDER);
        if (childOrder == null) {
            nodeBuilder.setProperty(
                    MultiStringPropertyState.stringProperty(OAK_CHILD_ORDER, nodeBuilder.getChildNodeNames()));
        }
    }

    //-------------------------------------------------------< TreeLocation >---

    private final class NodeLocation extends AbstractNodeLocation<TreeImpl> {

        private NodeLocation(TreeImpl tree) {
            super(tree);
        }

        @Override
        protected NodeLocation createNodeLocation(TreeImpl tree) {
            return new NodeLocation(tree);
        }

        @Override
        protected TreeLocation createPropertyLocation(AbstractNodeLocation<TreeImpl> parentLocation, String name) {
            return new PropertyLocation(parentLocation, name);
        }

        @Override
        protected TreeImpl getParentTree() {
            return tree.parent;
        }

        @Override
        protected TreeImpl getChildTree(String name) {
            return tree.internalGetChild(name);
        }

        @Override
        protected PropertyState getPropertyState(String name) {
            return tree.internalGetProperty(name);
        }

        @Override
        protected boolean canRead(TreeImpl tree) {
            return TreeImpl.this.canRead(tree);
        }
    }

    private final class PropertyLocation extends AbstractPropertyLocation<TreeImpl> {

        private PropertyLocation(AbstractNodeLocation<TreeImpl> parentLocation, String name) {
            super(parentLocation, name);
        }

        @Override
        protected boolean canRead(PropertyState property) {
            return TreeImpl.this.canRead(property);
        }

    }

}


