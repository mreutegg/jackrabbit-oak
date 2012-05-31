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
package org.apache.jackrabbit.oak.spi.commit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.util.List;

/**
 * This {@code CommitHook} aggregates a list of commit hooks into
 * a single commit hook.
 */
public class CompositeCommitHook implements CommitHook {
    private final List<CommitHook> hooks;

    public CompositeCommitHook(List<CommitHook> hooks) {
        this.hooks = hooks;
    }

    @Override
    public NodeState beforeCommit(NodeStore store, NodeState before, NodeState after)
            throws CommitFailedException {

        NodeState oldState = before;
        NodeState newState = after;
        for (CommitHook hook : hooks) {
            NodeState newOldState = newState;
            newState = hook.beforeCommit(store, oldState, newState);
            oldState = newOldState;
        }

        return newState;
    }

    @Override
    public void afterCommit(NodeStore store, NodeState before, NodeState after) {
        for (CommitHook hook : hooks) {
            hook.afterCommit(store, before, after);
        }
    }
}
