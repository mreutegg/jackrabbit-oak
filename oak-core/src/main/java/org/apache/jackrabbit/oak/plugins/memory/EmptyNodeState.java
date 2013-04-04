/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.memory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * Singleton instances of empty and non-existent node states, i.e. ones
 * with neither properties nor child nodes.
 */
public final class EmptyNodeState implements NodeState {

    public static final NodeState EMPTY_NODE = new EmptyNodeState(true);

    public static final NodeState MISSING_NODE = new EmptyNodeState(false);

    private final boolean exists;

    private EmptyNodeState(boolean exists) {
        this.exists = exists;
    }

    @Override
    public boolean exists() {
        return exists;
    }

    @Override
    public long getPropertyCount() {
        return 0;
    }

    @Override @CheckForNull
    public PropertyState getProperty(String name) {
        return null;
    }

    @Override @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        return Collections.emptyList();
    }

    @Override
    public long getChildNodeCount() {
        return 0;
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        checkArgument(!checkNotNull(name).isEmpty());
        return false;
    }

    @Override @Nonnull
    public NodeState getChildNode(@Nonnull String name) {
        checkArgument(!checkNotNull(name).isEmpty());
        return MISSING_NODE;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return Collections.emptyList();
    }

    @Override @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return Collections.emptyList();
    }

    @Override @Nonnull
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    @Override
    public void compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base != EMPTY_NODE && base.exists()) {
            for (PropertyState before : base.getProperties()) {
                diff.propertyDeleted(before);
            }
            for (ChildNodeEntry before : base.getChildNodeEntries()) {
                diff.childNodeDeleted(before.getName(), before.getNodeState());
            }
        }
    }

    public static void compareAgainstEmptyState(
            NodeState state, NodeStateDiff diff) {
        if (state != EMPTY_NODE && state.exists()) {
            for (PropertyState after : state.getProperties()) {
                diff.propertyAdded(after);
            }
            for (ChildNodeEntry after : state.getChildNodeEntries()) {
                diff.childNodeAdded(after.getName(), after.getNodeState());
            }
        }
    }

    //------------------------------------------------------------< Object >--

    public String toString() {
        return "{ }";
    }

    public boolean equals(Object object) {
        if (object == EMPTY_NODE || object == MISSING_NODE) {
            return exists == (object == EMPTY_NODE);
        } else if (object instanceof NodeState) {
            NodeState that = (NodeState) object;
            return that.getPropertyCount() == 0
                    && that.getChildNodeCount() == 0;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return 0;
    }

}
