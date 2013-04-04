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
package org.apache.jackrabbit.oak.plugins.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

/**
 * TODO document
 */
public class IndexUtils implements IndexConstants {

    public static NodeBuilder getOrCreateOakIndex(NodeBuilder root) {
        NodeBuilder index;
        if (!root.hasChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)) {
            index = root.child(IndexConstants.INDEX_DEFINITIONS_NAME);
            // TODO: use property node type name
            index.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
        } else {
            index = root.child(IndexConstants.INDEX_DEFINITIONS_NAME);
        }
        return index;
    }

    /**
     * Create a new property2 index definition below the given {@code indexNode}.
     *
     * @param index         The oak:index node builder
     * @param indexDefName  The name of the new property index.
     * @param reindex       {@code true} if the the reindex flag should be turned on.
     * @param unique        {@code true} if the index is expected the assert property
     *                      uniqueness.
     * @param propertyNames The property names that should be indexed.
     */
    public static void createIndexDefinition(@Nonnull NodeBuilder index,
                                             @Nonnull String indexDefName,
                                             boolean reindex,
                                             boolean unique,
                                             @Nonnull List<String> propertyNames,
                                             @Nullable List<String> declaringNodeTypeNames) {
        NodeBuilder entry = index.child(indexDefName)
                .setProperty(JCR_PRIMARYTYPE, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, Type.NAME)
                .setProperty(IndexConstants.TYPE_PROPERTY_NAME, "p2")
                .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, reindex);
        if (unique) {
            entry.setProperty(IndexConstants.UNIQUE_PROPERTY_NAME, unique);
        }
        entry.setProperty(PropertyStates.createProperty(IndexConstants.PROPERTY_NAMES, propertyNames, Type.STRINGS));
        if (declaringNodeTypeNames != null && !declaringNodeTypeNames.isEmpty()) {
            entry.setProperty(PropertyStates.createProperty(IndexConstants.DECLARING_NODE_TYPES, declaringNodeTypeNames, Type.STRINGS));
        }
    }

    /**
     * Create a new property2 index definition below the given {@code indexNode}.
     *
     * @param indexNode
     * @param indexDefName
     * @param unique
     * @param propertyNames
     * @param declaringNodeTypeNames
     */
    public static void createIndexDefinition(@Nonnull NodeUtil indexNode,
                                             @Nonnull String indexDefName,
                                             boolean unique,
                                             @Nonnull String[] propertyNames,
                                             @Nullable String[] declaringNodeTypeNames) {
        NodeUtil entry = indexNode.getOrAddChild(indexDefName, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        entry.setString(IndexConstants.TYPE_PROPERTY_NAME, "p2");
        entry.setBoolean(IndexConstants.REINDEX_PROPERTY_NAME, true);
        if (unique) {
            entry.setBoolean(IndexConstants.UNIQUE_PROPERTY_NAME, true);
        }
        if (declaringNodeTypeNames != null && declaringNodeTypeNames.length > 0) {
            entry.setStrings(IndexConstants.DECLARING_NODE_TYPES, declaringNodeTypeNames);
        }
        entry.setStrings(IndexConstants.PROPERTY_NAMES, propertyNames);
    }

    /**
     * Builds a list of the existing index definitions.
     * <p/>
     * Checks only children of the provided state for an index definitions
     * container node, aka a node named {@link #INDEX_DEFINITIONS_NAME}
     *
     * @return
     */
    public static List<IndexDefinition> buildIndexDefinitions(NodeState state,
                                                              String indexConfigPath, String typeFilter) {
        NodeState definitions = state.getChildNode(INDEX_DEFINITIONS_NAME);
        if (!definitions.exists()) {
            return Collections.emptyList();
        }
        indexConfigPath = concat(indexConfigPath, INDEX_DEFINITIONS_NAME);

        List<IndexDefinition> defs = new ArrayList<IndexDefinition>();
        for (ChildNodeEntry c : definitions.getChildNodeEntries()) {
            IndexDefinition def = getDefinition(indexConfigPath, c, typeFilter);
            if (def == null) {
                continue;
            }
            defs.add(def);
        }
        return defs;
    }

    /**
     * Builds an {@link IndexDefinition} out of a {@link ChildNodeEntry}
     */
    private static IndexDefinition getDefinition(String path,
                                                 ChildNodeEntry def, String typeFilter) {
        String name = def.getName();
        NodeState ns = def.getNodeState();
        PropertyState typeProp = ns.getProperty(TYPE_PROPERTY_NAME);
        String type = TYPE_UNKNOWN;
        if (typeProp != null && !typeProp.isArray()) {
            type = typeProp.getValue(STRING);
        }
        if (typeFilter != null && !typeFilter.equals(type)) {
            return null;
        }
        return new IndexDefinitionImpl(name, type, concat(path, name));
    }

}
