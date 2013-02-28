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
package org.apache.jackrabbit.oak.plugins.nodetype;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.jcr.PropertyType.UNDEFINED;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

/**
 * Base implementation of a {@link NodeTypeManager} with support for reading
 * node types from the {@link Tree} returned by {@link #getTypes()}. Methods
 * related to node type modifications throw
 * {@link UnsupportedRepositoryOperationException}.
 */
public abstract class ReadOnlyNodeTypeManager implements NodeTypeManager, EffectiveNodeTypeProvider, DefinitionProvider {

    /**
     * Returns the internal name for the specified JCR name.
     *
     * @param jcrName JCR node type name.
     * @return the internal representation of the given JCR name.
     * @throws javax.jcr.RepositoryException If there is no valid internal representation
     * of the specified JCR name.
     */
    @Nonnull
    protected final String getOakName(String jcrName) throws RepositoryException {
        return getNamePathMapper().getOakName(jcrName);
    }

    /**
     * @return  {@link org.apache.jackrabbit.oak.api.Tree} instance where the node types
     * are stored or {@code null} if none.
     */
    @CheckForNull
    protected abstract Tree getTypes();

    /**
     * The value factory to be used by {@link org.apache.jackrabbit.oak.plugins.nodetype.PropertyDefinitionImpl#getDefaultValues()}.
     * If {@code null} the former returns {@code null}.
     * @return  {@code ValueFactory} instance or {@code null}.
     */
    @CheckForNull
    protected ValueFactory getValueFactory() {
        return null;
    }

    /**
     * Returns a {@link NameMapper} to be used by this node type manager. This
     * implementation returns the {@link NamePathMapperImpl#DEFAULT} instance. A
     * subclass may override this method and provide a different
     * implementation.
     *
     * @return {@link NameMapper} instance.
     */
    @Nonnull
    protected NamePathMapper getNamePathMapper() {
        return NamePathMapperImpl.DEFAULT;
    }

    //--------------------------------------------------------------------------

    /**
     * Return a new instance of {@code ReadOnlyNodeTypeManager} that reads node
     * type information from the tree at {@link NodeTypeConstants#NODE_TYPES_PATH}.
     *
     * @param root The root to read node types from.
     * @param namePathMapper The {@code NamePathMapper} to use.
     * @return a new instance of {@code ReadOnlyNodeTypeManager}.
     */
    @Nonnull
    public static ReadOnlyNodeTypeManager getInstance(final Root root,
                                                      final NamePathMapper namePathMapper) {
        return new ReadOnlyNodeTypeManager() {
            @Override
            protected Tree getTypes() {
                return root.getTree(NODE_TYPES_PATH);
            }

            @Nonnull
            @Override
            protected NamePathMapper getNamePathMapper() {
                return namePathMapper;
            }
        };
    }

    /**
     * Returns a read-only node type manager based on the types stored within
     * the content tree starting at the given root node state.
     *
     * @param root root node state
     * @return read-only node type manager
     */
    @Nonnull
    public static ReadOnlyNodeTypeManager getInstance(NodeState root) {
        Tree tree = new ReadOnlyTree(root).getLocation()
                .getChild(NODE_TYPES_PATH.substring(1)).getTree();

        final Tree types = tree == null
            ? new ReadOnlyTree(MemoryNodeState.EMPTY_NODE)  // No node types in content, use an empty node
            : tree;

        return new ReadOnlyNodeTypeManager() {
            @Override
            protected Tree getTypes() {
                return types;
            }
        };
    }

    //----------------------------------------------------< NodeTypeManager >---

    @Override
    public boolean hasNodeType(String name) throws RepositoryException {
        Tree types = getTypes();
        return types != null && types.hasChild(getOakName(name));
    }

    @Override
    public NodeType getNodeType(String name) throws RepositoryException {
        return internalGetNodeType(getOakName(name));
    }

    @Override
    public NodeTypeIterator getAllNodeTypes() throws RepositoryException {
        List<NodeType> list = Lists.newArrayList();
        Tree types = getTypes();
        if (types != null) {
            ValueFactory factory = getValueFactory();
            NamePathMapper mapper = getNamePathMapper();
            for (Tree type : types.getChildren()) {
                list.add(new NodeTypeImpl(type, factory, mapper));
            }
        }
        return new NodeTypeIteratorAdapter(list);
    }

    @Override
    public NodeTypeIterator getPrimaryNodeTypes() throws RepositoryException {
        List<NodeType> list = Lists.newArrayList();
        NodeTypeIterator iterator = getAllNodeTypes();
        while (iterator.hasNext()) {
            NodeType type = iterator.nextNodeType();
            if (!type.isMixin()) {
                list.add(type);
            }
        }
        return new NodeTypeIteratorAdapter(list);
    }

    @Override
    public NodeTypeIterator getMixinNodeTypes() throws RepositoryException {
        List<NodeType> list = Lists.newArrayList();
        NodeTypeIterator iterator = getAllNodeTypes();
        while (iterator.hasNext()) {
            NodeType type = iterator.nextNodeType();
            if (type.isMixin()) {
                list.add(type);
            }
        }
        return new NodeTypeIteratorAdapter(list);
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate()
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate(NodeTypeDefinition ntd)
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeDefinitionTemplate createNodeDefinitionTemplate()
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public PropertyDefinitionTemplate createPropertyDefinitionTemplate()
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    /**
     * This implementation always throws a {@link UnsupportedRepositoryOperationException}.
     */
    @Override
    public NodeType registerNodeType(NodeTypeDefinition ntd, boolean allowUpdate) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    /**
     * This implementation always throws a {@link UnsupportedRepositoryOperationException}.
     */
    @Override
    public NodeTypeIterator registerNodeTypes(NodeTypeDefinition[] ntds, boolean allowUpdate) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    /**
     * This implementation always throws a {@link UnsupportedRepositoryOperationException}.
     */
    @Override
    public void unregisterNodeType(String name) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    /**
     * This implementation always throws a {@link UnsupportedRepositoryOperationException}.
     */
    @Override
    public void unregisterNodeTypes(String[] names) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    //------------------------------------------< EffectiveNodeTypeProvider >---

    @Override
    public boolean isNodeType(Tree tree, String oakNtName) {
        // shortcuts for common cases
        if (JcrConstants.NT_BASE.equals(oakNtName)) {
            return true;
        } else if (JcrConstants.MIX_REFERENCEABLE.equals(oakNtName)
                && !tree.hasProperty(JcrConstants.JCR_UUID)) {
            return false;
        } else if (JcrConstants.MIX_VERSIONABLE.equals(oakNtName)
                && !tree.hasProperty(JcrConstants.JCR_ISCHECKEDOUT)) {
            return false;
        }

        Set<String> typeNames = Sets.newHashSet();

        PropertyState primary = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        if (primary != null && primary.getType() == Type.NAME) {
            String name = primary.getValue(Type.NAME);
            if (oakNtName.equals(name)) {
                return true;
            } else {
                typeNames.add(name);
            }
        }

        PropertyState mixins = tree.getProperty(JcrConstants.JCR_MIXINTYPES);
        if (mixins != null && mixins.getType() == Type.NAMES) {
            for (String name : mixins.getValue(Type.NAMES)) {
                if (oakNtName.equals(name)) {
                    return true;
                } else {
                    typeNames.add(name);
                }
            }
        }

        Tree types = getTypes();
        LinkedList<String> queue = Lists.newLinkedList(typeNames);
        while (!queue.isEmpty()) {
            Tree type = types.getChild(queue.removeFirst());
            if (type != null) {
                PropertyState supertypes =
                        type.getProperty(JcrConstants.JCR_SUPERTYPES);
                if (supertypes != null && supertypes.getType() == Type.NAMES) {
                    for (String name : supertypes.getValue(Type.NAMES)) {
                        if (oakNtName.equals(name)) {
                            return true;
                        } else if (typeNames.add(name)) {
                            queue.addLast(name);
                        }
                    }
                }
            }
        }

        return false;
    }

    /**
     * Returns all the node types of the given node, in a breadth-first
     * traversal order of the type hierarchy.
     *
     * @param node node instance
     * @return all types of the given node
     * @throws RepositoryException if the type information can not be accessed
     * @param node
     * @return
     * @throws RepositoryException
     */
    @Override
    public EffectiveNodeType getEffectiveNodeType(Node node) throws RepositoryException {
        Queue<NodeType> queue = Queues.newArrayDeque();
        queue.add(node.getPrimaryNodeType());
        queue.addAll(Arrays.asList(node.getMixinNodeTypes()));

        return getEffectiveNodeType(queue);
    }

    @Override
    public EffectiveNodeType getEffectiveNodeType(Tree tree) throws RepositoryException {
        Queue<NodeType> queue = Queues.newArrayDeque();

        NodeType primaryType;
        PropertyState jcrPrimaryType = tree.getProperty(JCR_PRIMARYTYPE);
        if (jcrPrimaryType != null) {
            String ntName = jcrPrimaryType.getValue(STRING);
            primaryType = internalGetNodeType(ntName);
        } else {
            throw new RepositoryException("Node at "+tree.getPath()+" has no primary type.");
        }
        queue.add(primaryType);

        List<NodeType> mixinTypes = Lists.newArrayList();
        PropertyState jcrMixinType = tree.getProperty(JCR_MIXINTYPES);
        if (jcrMixinType != null) {
            for (String ntName : jcrMixinType.getValue(STRINGS)) {
                mixinTypes.add(internalGetNodeType(ntName));
            }
        }
        queue.addAll(mixinTypes);

        return getEffectiveNodeType(queue);
    }

    //-------------------------------------------------< DefinitionProvider >---

    @Override
    public NodeDefinition getRootDefinition() throws RepositoryException {
        return new RootNodeDefinition(this);
    }

    @Nonnull
    @Override
    public NodeDefinition getDefinition(@Nonnull Node parent, @Nonnull String nodeName)
            throws RepositoryException {
        checkNotNull(parent);
        checkNotNull(nodeName);

        return getNodeDefinition(getEffectiveNodeType(parent), nodeName, null);
    }

    @Override
    public NodeDefinition getDefinition(@Nonnull Node parent, @Nonnull Node targetNode)
            throws RepositoryException {
        checkNotNull(parent);
        checkNotNull(targetNode);

        String name = targetNode.getName();
        EffectiveNodeType eff = getEffectiveNodeType(parent);
        return getNodeDefinition(eff, name, getEffectiveNodeType(targetNode));
    }

    @Override
    public NodeDefinition getDefinition(Iterable<NodeType> parentNodeTypes,
                                        String nodeName, NodeType nodeType)
            throws ConstraintViolationException {
        EffectiveNodeType eff = getEffectiveNodeType(Queues.newArrayDeque(parentNodeTypes));
        return getNodeDefinition(eff, nodeName, getEffectiveNodeType(Queues.newArrayDeque(Collections.singleton(nodeType))));
    }

    @Override
    public PropertyDefinition getDefinition(Node parent, Property targetProperty) throws RepositoryException {
        String name = targetProperty.getName();
        boolean isMultiple = targetProperty.isMultiple();
        int type = UNDEFINED;
        if (isMultiple) {
            Value[] values = targetProperty.getValues();
            if (values.length > 0) {
                type = values[0].getType();
            }
        } else {
            type = targetProperty.getValue().getType();
        }

        return getPropertyDefinition(getEffectiveNodeType(parent), name, isMultiple, type, true);
    }

    @Nonnull
    @Override
    public PropertyDefinition getDefinition(Tree parent, PropertyState propertyState) throws RepositoryException {
        return getDefinition(parent, propertyState.getName(), propertyState.isArray(), propertyState.getType().tag(), true);
    }

    @Nonnull
    @Override
    public PropertyDefinition getDefinition(Node parent, String propertyName, boolean isMultiple, int type, boolean exactTypeMatch) throws RepositoryException {
        return getPropertyDefinition(getEffectiveNodeType(parent), propertyName, isMultiple, type, exactTypeMatch);
    }

    @Nonnull
    @Override
    public PropertyDefinition getDefinition(Tree parent, String propertyName, boolean isMultiple, int type, boolean exactTypeMatch) throws RepositoryException {
        return getPropertyDefinition(getEffectiveNodeType(parent), propertyName, isMultiple, type, exactTypeMatch);
    }

    @Nonnull
    @Override
    public PropertyDefinition getDefinition(Iterable<NodeType> nodeTypes, String propertyName, boolean isMultiple,
            int type, boolean exactTypeMatch) throws RepositoryException {
        Queue<NodeType> queue = Queues.newArrayDeque(nodeTypes);
        return getPropertyDefinition(getEffectiveNodeType(queue), propertyName, isMultiple, type, exactTypeMatch);
    }

    //-----------------------------------------------------------< internal >---

    NodeTypeImpl internalGetNodeType(String oakName) throws NoSuchNodeTypeException {
        ValueFactory factory = getValueFactory();
        NamePathMapper mapper = getNamePathMapper();

        Tree types = getTypes();
        if (types != null) {
            Tree type = types.getChild(oakName);
            if (type != null) {
                return new NodeTypeImpl(type, factory, mapper);
            }
        }
        throw new NoSuchNodeTypeException(mapper.getJcrName(oakName));
    }

    //------------------------------------------------------------< private >---

    private EffectiveNodeType getEffectiveNodeType(Queue<NodeType> queue) throws ConstraintViolationException {
        Map<String, NodeType> types = Maps.newHashMap();
        while (!queue.isEmpty()) {
            NodeType type = queue.remove();
            String name = type.getName();
            if (!types.containsKey(name)) {
                types.put(name, type);
                queue.addAll(Arrays.asList(type.getDeclaredSupertypes()));
            }
        }
        return EffectiveNodeTypeImpl.create(types.values(), this);
    }

    /**
     *
     * @param effectiveNodeType
     * @param propertyName The internal oak name of the property.
     * @param isMultiple
     * @param type
     * @param exactTypeMatch
     * @return
     * @throws ConstraintViolationException
     */
    private static PropertyDefinition getPropertyDefinition(EffectiveNodeType effectiveNodeType,
            String propertyName, boolean isMultiple,
            int type, boolean exactTypeMatch) throws ConstraintViolationException {
        // TODO: This may need to be optimized
        for (PropertyDefinition def : effectiveNodeType.getNamedPropertyDefinitions(propertyName)) {
            int defType = def.getRequiredType();
            if (isMultiple == def.isMultiple()
                    &&(!exactTypeMatch || (type == defType || UNDEFINED == type || UNDEFINED == defType))) {
                return def;
            }
        }

        // try if there is a residual definition
        for (PropertyDefinition def : effectiveNodeType.getResidualPropertyDefinitions()) {
            int defType = def.getRequiredType();
            if (isMultiple == def.isMultiple()
                    && (!exactTypeMatch || (type == defType || UNDEFINED == type || UNDEFINED == defType))) {
                return def;
            }
        }

        throw new ConstraintViolationException("No matching property definition found for " + propertyName);
    }

    /**
     *
     * @param effectiveNodeType
     * @param childName The internal oak name of the target node.
     * @param childEffective
     * @return
     * @throws ConstraintViolationException
     */
    private static NodeDefinition getNodeDefinition(EffectiveNodeType effectiveNodeType,
                                                    String childName,
                                                    EffectiveNodeType childEffective) throws ConstraintViolationException {
        for (NodeDefinition def : effectiveNodeType.getNamedNodeDefinitions(childName)) {
            boolean match = true;
            if (childEffective != null && !childEffective.includesNodeTypes(def.getRequiredPrimaryTypeNames())) {
                match = false;
            }
            if (match) {
                return def;
            }
        }

        for (NodeDefinition def : effectiveNodeType.getResidualNodeDefinitions()) {
            boolean match = true;
            if (childEffective != null && !childEffective.includesNodeTypes(def.getRequiredPrimaryTypeNames())) {
                match = false;
            }
            if (match) {
                return def;
            }
        }
        throw new ConstraintViolationException("No matching node definition found for " + childName);
    }
}
