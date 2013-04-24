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

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Extension point for plugging in different kinds of IndexHook providers.
 * 
 * @see IndexHook
 */
public interface IndexHookProvider {

    /**
     * 
     * Each provider knows how to produce a certain type of index. If the
     * <code>type</code> param is of an unknown value, the provider is expected
     * to return an empty list.
     * 
     * <p>
     * The <code>builder</code> must point to the repository content node, not
     * the index content node. Each <code>IndexHook</code> implementation will
     * have to drill down to its specific index content, and possibly deal with
     * multiple indexes of the same type.
     * </p>
     * 
     * @param type
     *            the index type
     * @param builder
     *            the node state builder of the content node that will be used
     *            for updates
     * @param root
     *            root node state
     * @return a list of index hooks of the given type
     */
    @Nonnull
    List<? extends IndexHook> getIndexHooks(
            String type, NodeBuilder builder, NodeState root);

}
