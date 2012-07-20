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
package org.apache.jackrabbit.oak.spi.state;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * Builder interface for constructing new {@link NodeState node states}.
 */
public interface NodeStateBuilder {

    /**
     * Returns an immutable node state that matches the current state of
     * the builder.
     *
     * @return immutable node state
     */
    @Nonnull
    NodeState getNodeState();

    /**
     * Add a sub-tree
     *
     * @param name  name child node containing the sub-tree
     * @param nodeState  sub-tree
     */
    void setNode(String name, NodeState nodeState);

    /**
     * Remove a child node
     * @param name  name of the child node
     */
    void removeNode(String name);

    /**
     * Returns the current number of properties.
     *
     * @return number of properties
     */
    long getPropertyCount();

    /**
     * Returns the current properties.
     *
     * @return current properties
     */
    Iterable<? extends PropertyState> getProperties();

    /**
     * Returns the current state of the named property, or {@code null}
     * if the property is not set.
     *
     * @param name property name
     * @return property state
     */
    PropertyState getProperty(String name);

    /**
     * Set a property.
     *
     * @param name property name
     * @param value
     */
    void setProperty(String name, CoreValue value);

    /**
     * Set a property.
     *
     * @param name property name
     * @param values
     */
    void setProperty(String name, List<CoreValue> values);

    /**
     * Remove the named property
     * @param name  name of the property
     */
    void removeProperty(String name);

    /**
     * Returns a builder for constructing changes to the named child node.
     * If the named child node does not already exist, a new empty child
     * node is automatically created as the base state of the returned
     * child builder. Otherwise the existing child node state is used
     * as the base state of the returned builder.
     * <p>
     * All updates to the returned child builder will implicitly affect
     * also this builder, as if a
     * {@code setNode(name, childBuilder.getNodeState())} method call
     * had been made after each update. Repeated calls to this method with
     * the same name will return the same child builder instance until an
     * explicit {@link #setNode(String, NodeState)} or
     * {@link #removeNode(String)} call is made, at which point the link
     * between this builder and a previously returned child builder for
     * that child node name will get broken.
     *
     * @since Oak 0.4
     * @param name name of the child node
     * @return child builder
     */
    @Nonnull
    NodeStateBuilder getChildBuilder(String name);

}
