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
package org.apache.jackrabbit.oak.spi.security;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeWorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.util.TreeLocation;

/**
 * Abstract base implementation for {@link SecurityConfiguration}s that can
 * combine different implementations.
 */
public abstract class CompositeConfiguration<T extends SecurityConfiguration> implements SecurityConfiguration {

    private final List<T> configurations = new ArrayList<T>();

    private final String name;

    public CompositeConfiguration(String name) {
        this.name = name;
    }

    public void addConfiguration(@Nonnull T configuration, @Nonnull SecurityProvider sp) {
        configurations.add(configuration);
        if (configuration instanceof ConfigurationBase) {
            ((ConfigurationBase) configuration).setSecurityProvider(sp);
        }
    }

    public void removeConfiguration(@Nonnull T configuration) {
        configurations.remove(configuration);
    }

    public List<T> getConfigurations() {
        return ImmutableList.copyOf(configurations);
    }

    //----------------------------------------------< SecurityConfiguration >---
    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public ConfigurationParameters getParameters() {
        ConfigurationParameters[] params = new ConfigurationParameters[configurations.size()];
        for (int i = 0; i < configurations.size(); i++) {
            params[i] = configurations.get(i).getParameters();
        }
        return ConfigurationParameters.of(params);
    }

    @Nonnull
    @Override
    public WorkspaceInitializer getWorkspaceInitializer() {
        return new CompositeWorkspaceInitializer(Lists.transform(configurations, new Function<T, WorkspaceInitializer>() {
            @Override
            public WorkspaceInitializer apply(T securityConfiguration) {
                return securityConfiguration.getWorkspaceInitializer();
            }
        }));
    }

    @Nonnull
    @Override
    public RepositoryInitializer getRepositoryInitializer() {
        return new CompositeInitializer(Lists.transform(configurations, new Function<T, RepositoryInitializer>() {
            @Override
            public RepositoryInitializer apply(T securityConfiguration) {
                return securityConfiguration.getRepositoryInitializer();
            }
        }));
    }

    @Nonnull
    @Override
    public List<? extends CommitHook> getCommitHooks(final String workspaceName) {
        return ImmutableList.copyOf(Iterables.concat(Lists.transform(configurations, new Function<T, List<? extends CommitHook>>() {
            @Override
            public List<? extends CommitHook> apply(T securityConfiguration) {
                return securityConfiguration.getCommitHooks(workspaceName);
            }
        })));
    }

    @Nonnull
    @Override
    public List<? extends ValidatorProvider> getValidators(final String workspaceName, final CommitInfo commitInfo) {
        return ImmutableList.copyOf(Iterables.concat(Lists.transform(configurations, new Function<T, List<? extends ValidatorProvider>>() {
            @Override
            public List<? extends ValidatorProvider> apply(T securityConfiguration) {
                return securityConfiguration.getValidators(workspaceName, commitInfo);
            }
        })));
    }

    @Nonnull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return ImmutableList.copyOf(Iterables.concat(Lists.transform(configurations, new Function<T, List<? extends ProtectedItemImporter>>() {
            @Override
            public List<? extends ProtectedItemImporter> apply(T securityConfiguration) {
                return securityConfiguration.getProtectedItemImporters();
            }
        })));
    }

    @Override
    public Context getContext() {
        return new Context() {

            @Override
            public boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property) {
                for (SecurityConfiguration sc : configurations) {
                    if (sc.getContext().definesProperty(parent, property)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean definesContextRoot(@Nonnull Tree tree) {
                for (SecurityConfiguration sc : configurations) {
                    if (sc.getContext().definesContextRoot(tree)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean definesTree(@Nonnull Tree tree) {
                for (SecurityConfiguration sc : configurations) {
                    if (sc.getContext().definesTree(tree)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean definesLocation(@Nonnull TreeLocation location) {
                for (SecurityConfiguration sc : configurations) {
                    if (sc.getContext().definesLocation(location)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
