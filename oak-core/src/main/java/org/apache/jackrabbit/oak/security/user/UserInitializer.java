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

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.core.RootImpl;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexHookManager;
import org.apache.jackrabbit.oak.plugins.index.IndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates initial set of users to be present in a given workspace. This
 * implementation uses the {@code UserManager} such as defined by the
 * user configuration.
 * <p/>
 * Currently the following users are created:
 * <p/>
 * <ul>
 * <li>An administrator user using {@link UserConstants#PARAM_ADMIN_ID}
 * or {@link UserConstants#DEFAULT_ADMIN_ID} if the config option is missing.</li>
 * <li>An administrator user using {@link UserConstants#PARAM_ANONYMOUS_ID}
 * or {@link UserConstants#DEFAULT_ANONYMOUS_ID} if the config option is
 * missing.</li>
 * </ul>
 * <p/>
 * In addition this initializer sets up index definitions for the following
 * user related properties:
 * <p/>
 * <ul>
 * <li>{@link UserConstants#REP_AUTHORIZABLE_ID}</li>
 * <li>{@link UserConstants#REP_PRINCIPAL_NAME}</li>
 * <li>{@link UserConstants#REP_MEMBERS}</li>
 * </ul>
 */
public class UserInitializer implements WorkspaceInitializer, UserConstants {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(UserInitializer.class);

    private final SecurityProvider securityProvider;

    UserInitializer(SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
    }

    //-----------------------------------------------< WorkspaceInitializer >---
    @Nonnull
    @Override
    public NodeState initialize(NodeState workspaceRoot, String workspaceName,
                                IndexHookProvider indexHook, QueryIndexProvider indexProvider,
                                CommitHook commitHook) {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeStoreBranch branch = store.branch();
        branch.setRoot(workspaceRoot);
        try {
            branch.merge(IndexHookManager.of(indexHook));
        } catch (CommitFailedException e) {
            throw new RuntimeException(e);
        }
        Root root = new RootImpl(store, commitHook, workspaceName, SystemSubject.INSTANCE, securityProvider, indexProvider);

        UserConfiguration userConfiguration = securityProvider.getUserConfiguration();
        UserManager userManager = userConfiguration.getUserManager(root, NamePathMapper.DEFAULT);

        String errorMsg = "Failed to initialize user content.";
        try {
            NodeUtil rootTree = checkNotNull(new NodeUtil(root.getTree("/")));
            NodeUtil index = rootTree.getOrAddChild(IndexConstants.INDEX_DEFINITIONS_NAME, JcrConstants.NT_UNSTRUCTURED);
            IndexUtils.createIndexDefinition(index, "authorizableId", true, new String[]{REP_AUTHORIZABLE_ID}, null);
            IndexUtils.createIndexDefinition(index, "principalName", true,
                    new String[]{REP_PRINCIPAL_NAME},
                    new String[]{NT_REP_GROUP, NT_REP_USER});
            IndexUtils.createIndexDefinition(index, "members", false, new String[]{UserConstants.REP_MEMBERS}, null);

            String adminId = userConfiguration.getConfigurationParameters().getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
            if (userManager.getAuthorizable(adminId) == null) {
                // TODO: init admin with null password and force application to set it.
                userManager.createUser(adminId, adminId);
            }
            String anonymousId = userConfiguration.getConfigurationParameters().getConfigValue(PARAM_ANONYMOUS_ID, DEFAULT_ANONYMOUS_ID);
            if (userManager.getAuthorizable(anonymousId) == null) {
                userManager.createUser(anonymousId, null);
            }
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } catch (RepositoryException e) {
            log.error(errorMsg, e);
            throw new RuntimeException(e);
        } catch (CommitFailedException e) {
            log.error(errorMsg, e);
            throw new RuntimeException(e);
        }
        return store.getRoot();
    }
}
