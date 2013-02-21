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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.plugins.index.p2.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.p2.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.p2.Property2Index.TYPE;

/**
 * {@link IndexHook} implementation that is responsible for keeping the
 * {@link Property2Index} up to date.
 * <p/>
 * There is a tree of PropertyIndexDiff objects, each object represents the
 * changes at a given node.
 *
 * @see Property2Index
 * @see Property2IndexLookup
 */
class Property2IndexDiff implements IndexHook {

    protected static String propertyNames = "propertyNames";

    protected static String declaringNodeTypes = "declaringNodeTypes";


    private final IndexStoreStrategy store = new ContentMirrorStoreStrategy();

    /**
     * The parent (null if this is the root node).
     */
    private final Property2IndexDiff parent;

    /**
     * The node (never null).
     */
    private final NodeBuilder node;

    /**
     * The node name (the path element). Null for the root node.
     */
    private final String nodeName;

    /**
     * The path of the changed node (built lazily).
     */
    private String path;

    /**
     * The map of known indexes.
     * Key: the property name. Value: the list of indexes (it is possible to
     * have multiple indexes for the same property name).
     */
    private final Map<String, List<Property2IndexUpdate>> indexMap;

    public Property2IndexDiff(NodeBuilder root) {
        this(null, root, null, "/",
                new HashMap<String, List<Property2IndexUpdate>>());
    }

    private Property2IndexDiff(Property2IndexDiff parent, String nodeName) {
        this(parent, getChildNode(parent.node, nodeName),
                nodeName, null, parent.indexMap);
    }

    private Property2IndexDiff(
            Property2IndexDiff parent,
            NodeBuilder node, String nodeName, String path,
            Map<String, List<Property2IndexUpdate>> indexMap) {
        this.parent = parent;
        this.node = node;
        this.nodeName = nodeName;
        this.path = path;
        this.indexMap = indexMap;

        if (node != null && node.hasChildNode(INDEX_DEFINITIONS_NAME)) {
            NodeBuilder index = node.child(INDEX_DEFINITIONS_NAME);
            for (String indexName : index.getChildNodeNames()) {
                NodeBuilder child = index.child(indexName);
                if (isIndexNode(child)) {
                    addIndexes(child, indexName);
                }
            }
        }
    }

    private static NodeBuilder getChildNode(NodeBuilder node, String name) {
        if (node != null && node.hasChildNode(name)) {
            return node.child(name);
        } else {
            return null;
        }
    }

    @Override
    public String getPath() {
        // build the path lazily
        if (path == null) {
            path = concat(parent.getPath(), nodeName);
        }
        return path;
    }

    /**
     * Get all the indexes for the given property name.
     *
     * @param propertyName the property name
     * @return the indexes
     */
    private Iterable<Property2IndexUpdate> getIndexes(String propertyName) {
        List<Property2IndexUpdate> indexes = indexMap.get(propertyName);
        if (indexes == null) {
            return ImmutableList.of();
        }
        List<Property2IndexUpdate> filtered = new ArrayList<Property2IndexUpdate>();
        for (Property2IndexUpdate pi : indexes) {
            if (node == null || pi.getNodeTypeNames() == null
                    || pi.getNodeTypeNames().isEmpty()) {
                filtered.add(pi);
                continue;
            }
            PropertyState ps = node.getProperty(JCR_PRIMARYTYPE);
            String type = ps != null && !ps.isArray() ? ps
                    .getValue(Type.STRING) : null;
            if (type != null) {
                for (String typeName : pi.getNodeTypeNames()) {
                    if (typeName.equals(type)) {
                        filtered.add(pi);
                        break;
                    }
                }
            }
        }
        return filtered;
    }

    /**
     * Add the index definitions to the in-memory set of known index definitions.
     * 
     * @param builder the node builder that contains the index definition
     * @param indexName the name of the index
     */
    private void addIndexes(NodeBuilder builder, String indexName) {
        List<String> typeNames = ImmutableList.of();
        PropertyState appliesTo = builder.getProperty(declaringNodeTypes);
        if (appliesTo != null) {
            typeNames = newArrayList(appliesTo.getValue(Type.STRINGS));
        }
        PropertyState ps = builder.getProperty(propertyNames);

        Iterable<String> propertyNames = ps != null ? ps.getValue(Type.STRINGS)
                : ImmutableList.of(indexName);
        for (String pname : propertyNames) {
            List<Property2IndexUpdate> list = this.indexMap.get(pname);
            if (list == null) {
                list = newArrayList();
                this.indexMap.put(pname, list);
            }
            boolean exists = false;
            for (Property2IndexUpdate piu : list) {
                if (piu.getPath().equals(getPath())) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                list.add(new Property2IndexUpdate(getPath(), builder, store, typeNames));
            }
        }
    }

    private static boolean isIndexNode(NodeBuilder node) {
        PropertyState ps = node.getProperty(JCR_PRIMARYTYPE);
        boolean isNodeType = ps != null && !ps.isArray()
                && ps.getValue(Type.STRING).equals(INDEX_DEFINITIONS_NODE_TYPE);
        if (!isNodeType) {
            return false;
        }
        PropertyState type = node.getProperty(TYPE_PROPERTY_NAME);
        boolean isIndexType = type != null && !type.isArray()
                && type.getValue(Type.STRING).equals(TYPE);
        return isIndexType;
    }

    //-----------------------------------------------------< NodeStateDiff >--

    @Override
    public void propertyAdded(PropertyState after) {
        for (Property2IndexUpdate update : getIndexes(after.getName())) {
            update.insert(getPath(), after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        for (Property2IndexUpdate update : getIndexes(after.getName())) {
            update.remove(getPath(), before);
            update.insert(getPath(), after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        for (Property2IndexUpdate update : getIndexes(before.getName())) {
            update.remove(getPath(), before);
        }
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        childNodeChanged(name, MemoryNodeState.EMPTY_NODE, after);
    }

    @Override
    public void childNodeChanged(
            String name, NodeState before, NodeState after) {
        if (!NodeStateUtils.isHidden(name)) {
            after.compareAgainstBaseState(before, child(name));
        }
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        childNodeChanged(name, before, MemoryNodeState.EMPTY_NODE);
    }

    // -----------------------------------------------------< IndexHook >--

    @Override
    public void apply() throws CommitFailedException {
        for (List<Property2IndexUpdate> updateList : indexMap.values()) {
            for (Property2IndexUpdate update : updateList) {
                update.apply();
            }
        }
    }

    @Override
    public void reindex(NodeBuilder state) throws CommitFailedException {
        boolean reindex = false;
        for (List<Property2IndexUpdate> updateList : indexMap.values()) {
            for (Property2IndexUpdate update : updateList) {
                if (update.getAndResetReindexFlag()) {
                    reindex = true;
                }
            }
        }
        if (reindex) {
            state.getNodeState().compareAgainstBaseState(
                    MemoryNodeState.EMPTY_NODE,
                    new Property2IndexDiff(null, state, null, "/", indexMap));
        }
    }

    @Override
    public IndexHook child(String name) {
        return new Property2IndexDiff(this, name);
    }

    @Override
    public void close() throws IOException {
        indexMap.clear();
    }
}
