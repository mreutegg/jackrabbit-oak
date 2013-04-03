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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Represents the content of a QueryIndex as well as a mechanism for keeping
 * this content up to date.
 * <p>
 * An IndexHook listens for changes to the content and updates the index data
 * accordingly.
 */
public interface IndexHook extends Editor {

    /**
     * Re-create this index.
     * 
     * @param state
     *            the parent of the node "oak:index" (the node that contains the
     *            index definition)
     * @throws CommitFailedException
     */
    void reindex(NodeBuilder state) throws CommitFailedException;

}
