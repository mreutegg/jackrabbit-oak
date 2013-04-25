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

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;

/**
 * AbstractNodeLocation... TODO
 */
abstract class AbstractNodeLocation<T extends Tree> extends AbstractTreeLocation {
    protected final T tree;

    AbstractNodeLocation(T tree) {
        this.tree = checkNotNull(tree);
    }

    protected abstract TreeLocation createNodeLocation(T tree);

    protected abstract TreeLocation createPropertyLocation(AbstractNodeLocation<T> parentLocation, String name);

    protected abstract T getParentTree();

    protected abstract T getChildTree(String name);

    protected abstract PropertyState getPropertyState(String name);

    protected boolean canRead(T tree) {
        return true;
    }

    @Override
    public TreeLocation getParent() {
        T parentTree = getParentTree();
        return parentTree == null
            ? NullLocation.NULL
            : createNodeLocation(parentTree);
    }

    @Override
    public TreeLocation getChild(String name) {
        PropertyState prop = getPropertyState(name);
        if (prop != null) {
            return createPropertyLocation(this, name);
        }

        T child = getChildTree(name);
        if (child != null) {
            return createNodeLocation(child);
        }
        return new NullLocation(this, name);
    }

    @Override
    public boolean exists() {
        return tree.isConnected() && getTree() != null;
    }

    @Override
    public Tree getTree() {
        return canRead(tree) ? tree : null;
    }

    @Override
    public Tree.Status getStatus() {
        return tree.getStatus();
    }

    @Override
    public String getPath() {
        return tree.getPath();
    }

    @Override
    public boolean remove() {
        return tree.remove();
    }

    @Override
    public boolean set(PropertyState property) {
        return false;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("tree", tree).toString();
    }
}