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

package org.apache.jackrabbit.oak.spi.state;

import javax.annotation.Nonnull;

/**
 * A {@code RecursingNodeStateDiff} extends {@link DefaultNodeStateDiff}
 * with a factory method for diffing child nodes.
 * In contrast to {@code DefaultNodeStateDiff}, {@link #childNodeChanged(String, NodeState, NodeState)}
 * should <em>not</em> recurse into child nodes but rather only be concerned about whether to continue
 * diffing or not. The {@link #createChildDiff(String, NodeState, NodeState)} will be called instead
 * for diffing child nodes.
 */
public class RecursingNodeStateDiff extends DefaultNodeStateDiff {
    public static final RecursingNodeStateDiff EMPTY = new RecursingNodeStateDiff();

    /**
     * Create a {@code RecursingNodeStateDiff} for a child node
     * @param name  name of the child node
     * @param before  before state of the child node
     * @param after   after state of the child node
     * @return  {@code RecursingNodeStateDiff} for the child node
     */
    @Nonnull
    public RecursingNodeStateDiff createChildDiff(String name, NodeState before, NodeState after) {
        return this;
    }
}
