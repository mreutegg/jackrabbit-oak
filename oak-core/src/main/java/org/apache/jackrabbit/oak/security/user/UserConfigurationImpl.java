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
package org.apache.jackrabbit.oak.security.user;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitHookProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

/**
 * UserConfigurationImpl... TODO
 */
public class UserConfigurationImpl extends SecurityConfiguration.Default implements UserConfiguration {

    private final ConfigurationParameters config;
    private final SecurityProvider securityProvider;

    public UserConfigurationImpl(SecurityProvider securityProvider) {
        this.config = securityProvider.getConfiguration(PARAM_USER_OPTIONS);
        this.securityProvider = securityProvider;
    }

    //----------------------------------------------< SecurityConfiguration >---
    @Nonnull
    @Override
    public ConfigurationParameters getConfigurationParameters() {
        return config;
    }

    @Nonnull
    @Override
    public WorkspaceInitializer getWorkspaceInitializer() {
        return new UserInitializer(securityProvider);
    }

    @Nonnull
    @Override
    public CommitHookProvider getValidators() {
        return new CommitHookProvider() {
            @Nonnull
            @Override
            public CommitHook getCommitHook(@Nonnull String workspaceName) {
                return new ValidatingHook(new UserValidatorProvider(getConfigurationParameters()));
            }
        };
    }

    @Nonnull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return Collections.<ProtectedItemImporter>singletonList(new UserImporter(config));
    }

    @Nonnull
    @Override
    public Context getContext() {
        return UserContext.getInstance();
    }

    //--------------------------------------------------< UserConfiguration >---
    @Nonnull
    @Override
    public AuthorizableActionProvider getAuthorizableActionProvider() {
        // TODO OAK-521: add proper implementation
        AuthorizableActionProvider defProvider = new DefaultAuthorizableActionProvider(securityProvider, config);
        return config.getConfigValue(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, defProvider);
    }

    @Nonnull
    @Override
    public UserManager getUserManager(Root root, NamePathMapper namePathMapper) {
        return new UserManagerImpl(root, namePathMapper, securityProvider);
    }
}
