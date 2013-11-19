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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO PathFilter...
 * TODO Clarify: filter applies to parent
 */
public class PathFilter implements Filter {
    private final ImmutableTree beforeTree;
    private final ImmutableTree afterTree;
    private final String path;
    private final boolean deep;

    public PathFilter(@Nonnull ImmutableTree beforeTree, @Nonnull ImmutableTree afterTree,
            @Nonnull String path, boolean deep) {
        this.beforeTree = checkNotNull(beforeTree);
        this.afterTree = checkNotNull(afterTree);
        this.path = checkNotNull(path);
        this.deep = deep;
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return includeByPath();
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return includeByPath();
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return includeByPath();
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return includeByPath();
    }

    @Override
    public boolean includeChange(String name, NodeState before, NodeState after) {
        return includeByPath();
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return includeByPath();
    }

    @Override
    public boolean includeMove(String sourcePath, String destPath, NodeState moved) {
        return includeByPath();
    }

    @Override
    public Filter create(String name, NodeState before, NodeState after) {
        if (includeChildren(name)) {
            return new PathFilter(beforeTree, afterTree.getChild(name), path, deep);
        } else {
            return null;
        }
    }

    //------------------------------------------------------------< private >---

    private boolean includeByPath() {
        String path = afterTree.exists()
            ? afterTree.getPath()
            : beforeTree.getPath();

        boolean equalPaths = this.path.equals(path);
        if (!deep && !equalPaths) {
            return false;
        }

        if (deep && !(PathUtils.isAncestor(this.path, path) || equalPaths)) {
            return false;
        }
        return true;
    }

    private boolean includeChildren(String name) {
        String path = afterTree.exists()
            ? PathUtils.concat(afterTree.getPath(), name)
            : PathUtils.concat(beforeTree.getPath(), name);

        return PathUtils.isAncestor(path, this.path) ||
                path.equals((this.path)) ||
                deep && PathUtils.isAncestor(this.path, path);
    }


}
