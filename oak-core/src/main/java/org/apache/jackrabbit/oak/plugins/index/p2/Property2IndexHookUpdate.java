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
package org.apache.jackrabbit.oak.plugins.index.p2;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.p2.Property2Index.encode;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.p2.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Takes care of applying the updates to the index content.
 */
class Property2IndexHookUpdate {

    private final IndexStoreStrategy store;

    /**
     * The path of the index definition (where the index data is stored).
     */
    private final String path;

    /**
     * The node types that this index applies to. If <code>null</code> or
     * <code>empty</code> then the node type of the indexed node is ignored
     * 
     */
    private final List<String> nodeTypeNames;

    /**
     * The node where the index definition is stored.
     */
    private final NodeBuilder node;

    /**
     * The node where the index content is stored.
     */
    private final NodeBuilder index;

    private final boolean unique;

    private final Set<String> modifiedKeys = Sets.newHashSet();

    public Property2IndexHookUpdate(String path, NodeBuilder node,
            IndexStoreStrategy store, List<String> nodeTypeNames) {
        this.path = path;
        this.node = node;
        this.store = store;
        this.nodeTypeNames = nodeTypeNames;
        index = this.node.child(":index");
        PropertyState uniquePS = node.getProperty("unique");
        unique = uniquePS != null && !uniquePS.isArray()
                && uniquePS.getValue(Type.BOOLEAN);
    }

    String getPath() {
        return path;
    }

    List<String> getNodeTypeNames() {
        return nodeTypeNames;
    }

    /**
     * A property value was added at the given path.
     * 
     * @param path
     *            the path
     * @param value
     *            the value
     */
    void insert(String path, PropertyState value) throws CommitFailedException {
        Preconditions.checkArgument(path.startsWith(this.path));
        if (value.getType().tag() == PropertyType.BINARY) {
            return;
        }
        for (String key : encode(PropertyValues.create(value))) {
            store.insert(index, key, ImmutableSet.of(trimm(path)));
            modifiedKeys.add(key);
        }
    }

    /**
     * A property value was removed at the given path.
     * 
     * @param path
     *            the path
     * @param value
     *            the value
     */
    public void remove(String path, PropertyState value)
            throws CommitFailedException {
        Preconditions.checkArgument(path.startsWith(this.path));
        if (value.getType().tag() == PropertyType.BINARY) {
            return;
        }
        for (String key : encode(PropertyValues.create(value))) {
            store.remove(index, key, ImmutableSet.of(trimm(path)));
            modifiedKeys.add(key);
        }
    }

    public void checkUniqueKeys() throws CommitFailedException {
        if (unique && !modifiedKeys.isEmpty()) {
            NodeState state = index.getNodeState();
            for (String key : modifiedKeys) {
                if (store.count(state, Collections.singletonList(key), 2) > 1) {
                    throw new CommitFailedException(
                            "Uniqueness constraint violated for key " + key);
                }
            }
        }
    }

    private String trimm(String path) {
        path = path.substring(this.path.length());
        if ("".equals(path)) {
            return "/";
        }
        return path;
    }

    boolean getAndResetReindexFlag() {
        PropertyState reindexPS = node.getProperty(REINDEX_PROPERTY_NAME);
        boolean reindex = reindexPS == null
                || (reindexPS != null && reindexPS.getValue(Type.BOOLEAN));
        node.setProperty(REINDEX_PROPERTY_NAME, false);
        return reindex;
    }

}
