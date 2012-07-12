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
package org.apache.jackrabbit.oak.jcr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.ItemNotFoundException;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.util.Function1;
import org.apache.jackrabbit.oak.util.Iterators;
import org.apache.jackrabbit.oak.util.Predicate;

/**
 * {@code NodeDelegate} serve as internal representations of {@code Node}s.
 * Most methods of this class throw an {@code InvalidItemStateException}
 * exception if the instance is stale. An instance is stale if the underlying
 * items does not exist anymore.
 */
public class NodeDelegate extends ItemDelegate {

    /**
     * The underlying {@link Tree} instance. In order to ensure the instance
     * is up to date, this field <em>should not be accessed directly</em> but
     * rather the {@link #getTree()} method should be used.
     */
    private Tree tree;

    NodeDelegate(SessionDelegate sessionDelegate, Tree tree) {
        super(sessionDelegate);
        this.tree = tree;
    }

    @Override
    public String getName() throws InvalidItemStateException {
        return getTree().getName();
    }

    @Override
    public String getPath() throws InvalidItemStateException {
        return '/' + getTree().getPath();
    }

    @Override
    public NodeDelegate getParent() throws InvalidItemStateException {
        Tree parent = getTree().getParent();
        return parent == null ? null : new NodeDelegate(sessionDelegate, parent);
    }

    @Override
    public boolean isStale() {
        resolve();
        return tree == null;
    }

    @Override
    public Status getStatus() throws InvalidItemStateException {
        return getTree().getStatus();
    }

    @Override
    public String toString() {
        // don't disturb the state: avoid calling getTree()
        return "NodeDelegate[/" + tree.getPath() + ']';
    }

    @Nonnull
    public String getIdentifier() throws InvalidItemStateException {
        PropertyDelegate pd = getProperty("jcr:uuid");
        if (pd == null) {
            // TODO should find the closest referenceable parent, and build an identifier based on that and the relative path
            return getPath();
        }
        else {
            return pd.getValue().toString();
        }
    }

    /**
     * Determine whether this is the root node
     * @return  {@code true} iff this is the root node
     */
    public boolean isRoot() throws InvalidItemStateException {
        return getTree().isRoot();
    }

    /**
     * Get the number of properties of the node
     * @return  number of properties of the node
     */
    public long getPropertyCount() throws InvalidItemStateException {
        // TODO: Exclude "invisible" internal properties (OAK-182)
        return getTree().getPropertyCount();
    }

    /**
     * Get a property
     * @param relPath  oak path
     * @return  property at the path given by {@code relPath} or {@code null} if
     * no such property exists
     */
    @CheckForNull
    public PropertyDelegate getProperty(String relPath) throws InvalidItemStateException {
        Tree parent = getTree(PathUtils.getParentPath(relPath));
        if (parent == null) {
            return null;
        }

        String name = PathUtils.getName(relPath);
        PropertyState propertyState = parent.getProperty(name);
        return propertyState == null
            ? null
            : new PropertyDelegate(sessionDelegate, parent, propertyState);
    }

    /**
     * Get the properties of the node
     * @return  properties of the node
     */
    @Nonnull
    public Iterator<PropertyDelegate> getProperties() throws InvalidItemStateException {
        return propertyDelegateIterator(getTree().getProperties().iterator());
    }

    /**
     * Get the number of child nodes
     * @return  number of child nodes of the node
     */
    public long getChildCount() throws InvalidItemStateException {
        // TODO: Exclude "invisible" internal child nodes (OAK-182)
        return getTree().getChildrenCount();
    }

    /**
     * Get child node
     * @param relPath  oak path
     * @return  node at the path given by {@code relPath} or {@code null} if
     * no such node exists
     */
    @CheckForNull
    public NodeDelegate getChild(String relPath) throws InvalidItemStateException {
        Tree tree = getTree(relPath);
        return tree == null ? null : new NodeDelegate(sessionDelegate, tree);
    }

    /**
     * Returns an iterator for traversing all the children of this node.
     * If the node is orderable (there is an "<code>oak:childOrder</code>"
     * property) then the iterator will return child nodes in the specified
     * order. Otherwise the ordering of the iterator is undefined.
     *
     * @return  child nodes of the node
     */
    @Nonnull
    public Iterator<NodeDelegate> getChildren() throws InvalidItemStateException {
        Tree tree = getTree();
        long count = tree.getChildrenCount();
        if (count == 0) {
            // Optimise the most common case
            return Collections.<NodeDelegate>emptySet().iterator();
        } else if (count == 1) {
            // Optimise another typical case
            Tree child = tree.getChildren().iterator().next();
            if (!child.getName().startsWith(":")) {
                NodeDelegate delegate = new NodeDelegate(sessionDelegate, child);
                return Collections.singleton(delegate).iterator();
            } else {
                return Collections.<NodeDelegate>emptySet().iterator();
            }
        } else {
            // TODO: Use a proper namespace for this property?
            PropertyState order = tree.getProperty("childOrder");
            if (order == null || !order.isArray()) {
                // No specified ordering
                return nodeDelegateIterator(tree.getChildren().iterator());
            } else {
                // Collect child nodes in the specified order
                final Map<String, NodeDelegate> ordered =
                        new LinkedHashMap<String, NodeDelegate>();

                for (CoreValue value : order.getValues()) {
                    String name = value.getString();
                    Tree child = tree.getChild(name);
                    if (child != null && !name.startsWith(":")) {
                        ordered.put(name, new NodeDelegate(sessionDelegate, child));
                    }
                }

                if (ordered.size() == count) {
                    // We have all the child nodes
                    return ordered.values().iterator();
                } else {
                    // The specified ordering didn't cover all the children,
                    // so return a combined iterator that first iterates
                    // through the ordered subset and then all the remaining
                    // children in an undefined order
                    Iterator<Tree> remaining = Iterators.filter(
                            tree.getChildren().iterator(),
                            new Predicate<Tree>() {
                                @Override
                                public boolean evaluate(Tree tree) {
                                    return !ordered.containsKey(tree.getName());
                                }
                            });
                    return Iterators.chain(
                            ordered.values().iterator(),
                            nodeDelegateIterator(remaining));
                }
            }
        }
    }

    public void orderBefore(String source, String target)
            throws ItemNotFoundException, InvalidItemStateException {
        Tree tree = getTree();
        if (tree.getChild(source) == null) {
            throw new ItemNotFoundException("Not a child: " + source);
        } else if (target != null && tree.getChild(target) == null) {
            throw new ItemNotFoundException("Not a child: " + target);
        } else {
            List<CoreValue> order = new ArrayList<CoreValue>();
            Set<String> added = new HashSet<String>();
            CoreValueFactory factory =
                    sessionDelegate.getContentSession().getCoreValueFactory();

            PropertyState property = tree.getProperty("childOrder");
            if (property != null) {
                for (CoreValue value : property.getValues()) {
                    String name = value.getString();
                    if (!name.equals(source) && !added.contains(property)
                            && !name.startsWith(":")) {
                        if (name.equals(target)) {
                            order.add(factory.createValue(source));
                            added.add(source);
                        }
                        order.add(factory.createValue(name));
                        added.add(name);
                    }
                }
            }

            if (!added.contains(source)) {
                order.add(factory.createValue(source));
            }
            if (target != null && !added.contains(target)) {
                order.add(factory.createValue(source));
            }

            tree.setProperty("childOrder", order);
        }
    }

    /**
     * Set a property
     * @param name  oak name
     * @param value
     * @return  the set property
     */
    @Nonnull
    public PropertyDelegate setProperty(String name, CoreValue value) throws InvalidItemStateException {
        PropertyState propertyState = getTree().setProperty(name, value);
        return new PropertyDelegate(sessionDelegate, getTree(), propertyState);
    }

    /**
     * Set a multi valued property
     * @param name  oak name
     * @param value
     * @return  the set property
     */
    @Nonnull
    public PropertyDelegate setProperty(String name, List<CoreValue> value) throws InvalidItemStateException {
        PropertyState propertyState = getTree().setProperty(name, value);
        return new PropertyDelegate(sessionDelegate, getTree(), propertyState);
    }

    /**
     * Add a child node
     * @param name  oak name
     * @return  the added node or {@code null} if such a node already exists
     */
    @CheckForNull
    public NodeDelegate addChild(String name) throws InvalidItemStateException {
        Tree tree = getTree();
        return tree.hasChild(name)
            ? null
            : new NodeDelegate(sessionDelegate, tree.addChild(name));
    }

    /**
     * Remove the node if not root. Does nothing otherwise
     */
    public void remove() throws InvalidItemStateException {
        getTree().remove();
    }

    // -----------------------------------------------------------< private >---

    private Tree getTree(String relPath) throws InvalidItemStateException {
        String absPath = PathUtils.concat(getPath(), relPath);
        return sessionDelegate.getTree(absPath);
    }

    private synchronized Tree getTree() throws InvalidItemStateException {
        resolve();
        if (tree == null) {
            throw new InvalidItemStateException("Node is stale");
        }

        return tree;
    }

    private synchronized void resolve() {
        if (tree != null) {
            tree = sessionDelegate.getTree(tree.getPath());
        }
    }

    private Iterator<NodeDelegate> nodeDelegateIterator(
            Iterator<Tree> childNodeStates) {
        return Iterators.map(
                Iterators.filter(childNodeStates, new Predicate<Tree>() {
                    @Override
                    public boolean evaluate(Tree tree) {
                        return !tree.getName().startsWith(":");
                    }
                }),
                new Function1<Tree, NodeDelegate>() {
                    @Override
                    public NodeDelegate apply(Tree state) {
                        return new NodeDelegate(sessionDelegate, state);
                    }
                });
    }

    private Iterator<PropertyDelegate> propertyDelegateIterator(
            Iterator<? extends PropertyState> properties) {
        return Iterators.map(
                Iterators.filter(properties, new Predicate<PropertyState>() {
                    @Override
                    public boolean evaluate(PropertyState property) {
                        return !property.getName().startsWith(":");
                    }
                }),
                new Function1<PropertyState, PropertyDelegate>() {
                    @Override
                    public PropertyDelegate apply(PropertyState propertyState) {
                        return new PropertyDelegate(sessionDelegate, tree,
                                propertyState);
                    }
                });
    }
}
