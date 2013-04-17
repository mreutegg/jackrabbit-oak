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
package org.apache.jackrabbit.oak.spi.security;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

/**
 * SecurityConfiguration... TODO
 */
public interface SecurityConfiguration {

    @Nonnull
    ConfigurationParameters getConfigurationParameters();

    @Nonnull
    WorkspaceInitializer getWorkspaceInitializer();

    @Nonnull
    List<? extends CommitHook> getCommitHooks(String workspaceName);

    @Nonnull
    List<? extends ValidatorProvider> getValidators(String workspaceName);

    @Nonnull
    List<ProtectedItemImporter> getProtectedItemImporters();

    @Nonnull
    Context getContext();

    /**
     * Default implementation that provides empty validators/parameters.
     */
    class Default implements SecurityConfiguration {

        @Nonnull
        @Override
        public ConfigurationParameters getConfigurationParameters() {
            return ConfigurationParameters.EMPTY;
        }

        @Nonnull
        @Override
        public WorkspaceInitializer getWorkspaceInitializer() {
            return new WorkspaceInitializer() {
                @Nonnull
                @Override
                public NodeState initialize(NodeState workspaceRoot, String workspaceName, QueryIndexProvider indexProvider, CommitHook commitHook) {
                    return workspaceRoot;
                }
            };
        }

        @Nonnull
        @Override
        public List<? extends CommitHook> getCommitHooks(String workspaceName) {
            return Collections.emptyList();
        }

        @Nonnull
        @Override
        public List<? extends ValidatorProvider> getValidators(String workspaceName) {
            return Collections.emptyList();
        }

        @Nonnull
        @Override
        public List<ProtectedItemImporter> getProtectedItemImporters() {
            return Collections.emptyList();
        }

        @Override
        public Context getContext() {
            return new Context.Default();
        }
    }
}
