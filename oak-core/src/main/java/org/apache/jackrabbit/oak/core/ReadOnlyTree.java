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

import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class ReadOnlyTree implements Tree {

    /** Parent of this tree, {@code null} for the root */
    private Tree parent;

    /** Name of this tree */
    private String name;

    /** Underlying node state */
    protected NodeState state;

    public ReadOnlyTree(NodeState root) {
        this(null, "", root);
    }

    private ReadOnlyTree(Tree parent, String name, NodeState state) {
        assert name != null;
        assert name.length() > 0 || parent == null;
        assert state != null;
        this.parent = parent;
        this.name = name;
        this.state = state;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPath() {
        if (parent == null) {
            return "";
        } else {
            return parent.getPath() + "/" + name;
        }
    }

    @Override
    public Tree getParent() {
        return parent;
    }

    @Override
    public PropertyState getProperty(String name) {
        return state.getProperty(name);
    }

    @Override
    public Status getPropertyStatus(String name) {
        if (hasProperty(name)) {
            return Status.EXISTING;
        } else {
            return null;
        }
    }

    @Override
    public boolean hasProperty(String name) {
        return state.getProperty(name) != null;
    }

    @Override
    public long getPropertyCount() {
        return state.getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return state.getProperties();
    }

    @Override
    public ReadOnlyTree getChild(String name) {
        NodeState child = state.getChildNode(name);
        if (child != null) {
            return new ReadOnlyTree(this, name, child);
        } else {
            return null;
        }
    }

    @Override
    public Status getChildStatus(String name) {
        if (hasChild(name)) {
            return Status.EXISTING;
        } else {
            return null;
        }
    }

    @Override
    public boolean hasChild(String name) {
        return state.getChildNode(name) != null;
    }

    @Override
    public long getChildrenCount() {
        return state.getChildNodeCount();
    }

    @Override
    public Iterable<Tree> getChildren() {
        return new Iterable<Tree>() {
            @Override
            public Iterator<Tree> iterator() {
                final Iterator<? extends ChildNodeEntry> iterator =
                        state.getChildNodeEntries().iterator();
                return new Iterator<Tree>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }
                    @Override
                    public Tree next() {
                        ChildNodeEntry entry = iterator.next();
                        return new ReadOnlyTree(
                                ReadOnlyTree.this,
                                entry.getName(), entry.getNodeState());
                    }
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public Tree addChild(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeChild(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PropertyState setProperty(String name, CoreValue value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PropertyState setProperty(String name, List<CoreValue> values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeProperty(String name) {
        throw new UnsupportedOperationException();
    }

}
