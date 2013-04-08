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
package org.apache.jackrabbit.oak.core;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * SecureNodeState...
 *
 * TODO: clarify if HIDDEN items should be filtered by this NodeState implementation
 * TODO: clarify usage of ReadStatus in getChildNodeEntries
 * TODO: add proper equals/hashcode implementation
 * TODO: should be package-private
 */
public class SecureNodeState extends AbstractNodeState {

    /**
     * Underlying node state.
     */
    private final NodeState state;

    /**
     * Immutable tree based on the underlying node state.
     */
    private final ImmutableTree base;

    private final PermissionProvider permissionProvider;

    private ReadStatus readStatus;
    private long childNodeCount = -1;
    private long propertyCount = -1;

    public SecureNodeState(@Nonnull NodeState rootState,
                           @Nonnull PermissionProvider permissionProvider,
                           @Nonnull ImmutableTree.TypeProvider typeProvider) {
        this.state = checkNotNull(rootState);
        this.base = new ImmutableTree(rootState, typeProvider);
        this.permissionProvider = permissionProvider;
    }

    private SecureNodeState(
            @Nonnull SecureNodeState parent,
            @Nonnull String name, @Nonnull NodeState nodeState) {
        this.state = checkNotNull(nodeState);
        this.base = new ImmutableTree(parent.base, name, nodeState);
        this.permissionProvider = parent.permissionProvider;
        if (base.getType() == parent.base.getType()) {
            this.readStatus = (ReadStatus.getChildStatus(parent.readStatus));
        }
    }

    @Override
    public boolean exists() {
        return getReadStatus().includes(ReadStatus.ALLOW_THIS);
    }

    @Override @CheckForNull
    public PropertyState getProperty(String name) {
        PropertyState property = state.getProperty(name);
        if (property != null && canReadProperty(property)) {
            return property;
        } else {
            return null;
        }
    }

    @Override
    public synchronized long getPropertyCount() {
        if (propertyCount == -1) {
            ReadStatus rs = getReadStatus();
            if (rs.includes(ReadStatus.ALLOW_PROPERTIES)) {
                propertyCount = state.getPropertyCount();
            } else if (rs.includes(ReadStatus.DENY_PROPERTIES)) {
                propertyCount = 0;
            } else {
                propertyCount = count(Iterables.filter(
                        state.getProperties(), isPropertyReadable()));
            }
        }
        return propertyCount;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        ReadStatus rs = getReadStatus();
        if (rs.includes(ReadStatus.ALLOW_PROPERTIES)) {
            return state.getProperties();
        } else if (rs.includes(ReadStatus.DENY_PROPERTIES)) {
            return Collections.emptySet();
        } else {
            return Iterables.filter(
                    state.getProperties(), isPropertyReadable());
        }
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return getChildNode(name).exists();
    }

    @Override
    public NodeState getChildNode(@Nonnull String name) {
        NodeState child = state.getChildNode(name);
        if (child.exists()) {
            return new SecureNodeState(this, name, child);
        } else {
            // a non-existing child node
            return child;
        }
    }

    @Override
    public synchronized long getChildNodeCount() {
        if (childNodeCount == -1) {
            childNodeCount = super.getChildNodeCount();
        }
        return childNodeCount;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return Iterables.transform(getChildNodeEntries(), new Function<ChildNodeEntry, String>() {
            @Override
            public String apply(@Nullable ChildNodeEntry cnEntry) {
                return (cnEntry == null) ? null : cnEntry.getName();
            }
        });
    }

    @Override
    @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        ReadStatus rs = getReadStatus();
        if (rs.includes(ReadStatus.DENY_CHILDREN)) {
            return Collections.emptySet();
        } else {
            // TODO: review if ALLOW_CHILDREN could be used as well although we
            // don't know the type of all child-nodes where ac node would need special treatment
            Iterable<ChildNodeEntry> readable = Iterables.transform(
                    state.getChildNodeEntries(),
                    new ReadableChildNodeEntries());
            return Iterables.filter(readable, Predicates.notNull());
        }
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    @Override
    public void compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        // FIXME: should not bypass access controls
        state.compareAgainstBaseState(base, diff);
    }

    //--------------------------------------------------------------------------

    private ReadStatus getReadStatus() {
        if (readStatus == null) {
            readStatus = permissionProvider.getReadStatus(base, null);
        }
        return readStatus;
    }

    private boolean canReadProperty(@Nonnull PropertyState property) {
        if (readStatus == null || readStatus.appliesToThis()) {
            ReadStatus rs = permissionProvider.getReadStatus(this.base, property);
            if (rs.appliesToThis()) {
                // status applies to this property only -> recalc for others
                return rs.isAllow();
            } else {
                readStatus = rs;
            }
        }
        return readStatus.includes(ReadStatus.ALLOW_PROPERTIES);
    }

    /**
     * Returns a predicate for testing whether a given property is readable.
     *
     * @return predicate
     */
    private Predicate<PropertyState> isPropertyReadable() {
        return new Predicate<PropertyState>() {
            @Override
            public boolean apply(@Nonnull PropertyState property) {
                return canReadProperty(property);
            }
        };
    }

    private class ReadableChildNodeEntries implements Function<ChildNodeEntry, ChildNodeEntry> {
        @Override
        public ChildNodeEntry apply(ChildNodeEntry input) {
            String name = input.getName();
            NodeState child = new SecureNodeState(SecureNodeState.this, name, input.getNodeState());
            if (child.exists()) {
                return new MemoryChildNodeEntry(name, child);
            } else {
                return null;
            }
        }
    }

    //-------------------------------------------------------------< Object >---
    // FIXME: add proper equals/hashcode implementation (see OAK-709)
    @Override
    public boolean equals(Object obj) {
        return state.equals(obj);
    }
}
