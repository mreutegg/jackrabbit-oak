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
package org.apache.jackrabbit.oak.plugins.index.p2.strategy;

import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Strategy that defines how the index content will be actually stored under the
 * index node
 * 
 */
public interface IndexStoreStrategy {

    /**
     * Removes a set of values from the index
     * 
     * @param index the index node
     * @param key the index key
     * @param values the values to be removed from the given key
     * @throws CommitFailedException
     */
    void remove(NodeBuilder index, String key, Iterable<String> values)
            throws CommitFailedException;

    /**
     * Inserts a set of values in the index
     * 
     * @param index the index node
     * @param key the index key
     * @param unique if the index is defined as unique
     * <b>Note:</b> If the uniqueness constraint is broken, the method will throw a <code>CommitFailedException</code>
     * @param values the values to be added to the given key
     * @throws CommitFailedException
     */
    void insert(NodeBuilder index, String key, boolean unique,
            Iterable<String> values) throws CommitFailedException;

    /**
     * Search for a given set of values
     * 
     * @param index index node
     * @param values values to look for
     * @return the set of paths corresponding to the given values
     */
    Set<String> find(NodeState index, Iterable<String> values);

    /**
     * Count the occurrence of a given set of values. Used in scoring.
     * 
     * @param index the index node
     * @param values values to look for
     * @return the aggregated count of occurrences for each provided value
     */
    int count(NodeState index, Iterable<String> values);

}
