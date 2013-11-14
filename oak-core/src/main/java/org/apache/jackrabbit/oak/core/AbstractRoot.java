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
package org.apache.jackrabbit.oak.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.diffindex.UUIDDiffIndexProviderWrapper;
import org.apache.jackrabbit.oak.query.ExecutionContext;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.MoveInfo;
import org.apache.jackrabbit.oak.spi.commit.PostValidationHook;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.util.LazyValue;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAncestor;

public abstract class AbstractRoot implements Root {

    /**
     * The underlying store to which this root belongs
     */
    private final NodeStore store;

    private final CommitHook hook;

    private final String workspaceName;

    private final Subject subject;

    private final SecurityProvider securityProvider;

    private final QueryIndexProvider indexProvider;

    /**
     * Current root {@code Tree}
     */
    private final MutableTree rootTree;

    /**
     * Unsecured builder for the root tree
     */
    private final NodeBuilder builder;

    /**
     * Secured builder for the root tree
     */
    private final SecureNodeBuilder secureBuilder;

    /**
     * Base state of the root tree
     */
    private NodeState base;

    /**
     * Sentinel for the next move operation to take place on the this root
     */
    private Move lastMove = new Move();

    /**
     * Simple info object used to collect all move operations (source + dest)
     * for further processing in those commit hooks that wish to distinguish
     * between simple add/remove and move operations.
     * Please note that this information will only allow to perform best-effort
     * matching as depending on the sequence of modifications some operations
     * may no longer be detected as changes in the commit hook due to way the
     * diff is compiled.
     */
    private MoveInfo moveInfo = new MoveInfo();

    /**
     * Number of {@link #updated} occurred.
     */
    private long modCount;

    private final LazyValue<PermissionProvider> permissionProvider = new LazyValue<PermissionProvider>() {
        @Override
        protected PermissionProvider createValue() {
            return getAcConfig().getPermissionProvider(AbstractRoot.this, subject.getPrincipals());
        }
    };

    /**
     * New instance bases on a given {@link NodeStore} and a workspace
     *
     * @param store            node store
     * @param hook             the commit hook
     * @param workspaceName    name of the workspace
     * @param subject          the subject.
     * @param securityProvider the security configuration.
     * @param indexProvider    the query index provider.
     */
    protected AbstractRoot(NodeStore store,
            CommitHook hook,
            String workspaceName,
            Subject subject,
            SecurityProvider securityProvider,
            QueryIndexProvider indexProvider) {
        this.store = checkNotNull(store);
        this.hook = checkNotNull(hook);
        this.workspaceName = checkNotNull(workspaceName);
        this.subject = checkNotNull(subject);
        this.securityProvider = checkNotNull(securityProvider);
        this.indexProvider = indexProvider;

        base = store.getRoot();
        builder = base.builder();
        secureBuilder = new SecureNodeBuilder(builder, permissionProvider, getAcContext());
        rootTree = new MutableTree(this, secureBuilder, lastMove);
    }
 
    /**
     * Called whenever a method on this instance or on any {@code Tree} instance
     * obtained from this {@code Root} is called. This default implementation
     * does nothing. Sub classes may override this method and throw an exception
     * indicating that this {@code Root} instance is not live anymore (e.g. because
     * the session has been logged out already).
     */
    protected void checkLive() {
    }

    //---------------------------------------------------------------< Root >---

    @Override
    public boolean move(String sourcePath, String destPath) {
        if (isAncestor(checkNotNull(sourcePath), checkNotNull(destPath))) {
            return false;
        } else if (sourcePath.equals(destPath)) {
            return true;
        }

        checkLive();
        MutableTree source = rootTree.getTree(sourcePath);
        if (!source.exists()) {
            return false;
        }

        String newName = getName(destPath);
        MutableTree newParent = rootTree.getTree(getParentPath(destPath));
        if (!newParent.exists() || newParent.hasChild(newName)) {
            return false;
        }

        boolean success = source.moveTo(newParent, newName);
        if (success) {
            getTree(getParentPath(sourcePath)).updateChildOrder();
            getTree(getParentPath(destPath)).updateChildOrder();
            lastMove = lastMove.setMove(sourcePath, newParent, newName);
            updated();
            // remember all move operations for further processing in the commit hooks.
            moveInfo.addMove(sourcePath, destPath);
        }
        return success;
    }

    @Override
    public boolean copy(String sourcePath, String destPath) {
        checkLive();
        MutableTree source = rootTree.getTree(sourcePath);
        if (!source.exists()) {
            return false;
        }

        String newName = getName(destPath);
        MutableTree newParent = rootTree.getTree(getParentPath(destPath));
        if (!newParent.exists() || newParent.hasChild(newName)) {
            return false;
        }

        boolean success = source.copyTo(newParent, newName);
        if (success) {
            getTree(getParentPath(destPath)).updateChildOrder();
            updated();
        }
        return success;
    }

    @Override
    public MutableTree getTree(@Nonnull String path) {
        checkLive();
        return rootTree.getTree(path);
    }

    @Override
    public void rebase() {
        checkLive();
        if (!store.getRoot().equals(getBaseState())) {
            store.rebase(builder);
            secureBuilder.baseChanged();
            if (permissionProvider.hasValue()) {
                permissionProvider.get().refresh();
            }
        }
    }

    @Override
    public final void refresh() {
        checkLive();
        base = store.reset(builder);
        secureBuilder.baseChanged();
        modCount = 0;
        if (permissionProvider.hasValue()) {
            permissionProvider.get().refresh();
        }
    }

    @Override
    public void commit() throws CommitFailedException {
        commit(null, null);
    }

    @Override
    public void commit(@Nullable String message, @Nullable CommitHook hook)
            throws CommitFailedException {
        checkLive();
        ContentSession session = getContentSession();
        CommitInfo info = new CommitInfo(
                session.toString(),
                getCommitSubject(session),
                moveInfo, message);
        base = store.merge(builder, getCommitHook(hook, info), info);
        secureBuilder.baseChanged();
        modCount = 0;
        if (permissionProvider.hasValue()) {
            permissionProvider.get().refresh();
        }
        moveInfo.clear();
    }

    /**
     * Combine the passed {@code hook}, the globally defined commit hook(s)
     * and the hooks and validators defined by the various security related
     * configurations.
     *
     * @param extraHook extra hook to be used for just this commit, or {@code null}
     * @return A commit hook combining repository global commit hook(s) with the pluggable hooks
     *         defined with the security modules and the padded {@code hooks}.
     */
    private CommitHook getCommitHook(@Nullable CommitHook extraHook, @Nonnull CommitInfo commitInfo) {
        List<CommitHook> hooks = newArrayList();

        if (extraHook != null) {
            hooks.add(extraHook);
        }

        hooks.add(hook);

        List<CommitHook> postValidationHooks = new ArrayList<CommitHook>();
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            for (CommitHook ch : sc.getCommitHooks(workspaceName)) {
                if (ch instanceof PostValidationHook) {
                    postValidationHooks.add(ch);
                } else if (ch != EmptyHook.INSTANCE) {
                    hooks.add(ch);
                }
            }

            List<? extends ValidatorProvider> validators = sc.getValidators(workspaceName, commitInfo);
            if (!validators.isEmpty()) {
                hooks.add(new EditorHook(CompositeEditorProvider.compose(validators)));
            }
        }
        hooks.addAll(postValidationHooks);

        return CompositeHook.compose(hooks);
    }

    /**
     * Build a read only subject for the {@link #commit(String, CommitHook)} call that makes the
     * principals, auth info and the permission provider available to the commit hooks.
     *
     * @return a new read only subject.
     */
    private Subject getCommitSubject(ContentSession session) {
        Set<Object> publicCreds = ImmutableSet.of(
                permissionProvider.get(),
                session.getAuthInfo()
        );
        return new Subject(true, subject.getPrincipals(), publicCreds, Collections.<Object>emptySet());
    }

    @Override
    public boolean hasPendingChanges() {
        checkLive();
        return modCount > 0;
    }

    @Override
    public QueryEngine getQueryEngine() {
        checkLive();
        return new QueryEngineImpl() {
            @Override
            protected ExecutionContext getExecutionContext() {
                QueryIndexProvider provider = indexProvider;
                if (hasPendingChanges()) {
                    provider = new UUIDDiffIndexProviderWrapper(
                            provider, getBaseState(), getRootState());
                }
                return new ExecutionContext(getBaseState(), rootTree, provider);
            }
        };
    }

    @Override @Nonnull
    public Blob createBlob(@Nonnull InputStream inputStream) throws IOException {
        checkLive();
        return store.createBlob(checkNotNull(inputStream));
    }

    //-----------------------------------------------------------< internal >---

    /**
     * Returns the node state from the time this root was created, that
     * is this root's base state.
     *
     * @return base node state
     */
    @Nonnull
    NodeState getBaseState() {
        return base;
    }

    /**
     * Returns the secure view of this root's base state.
     *
     * @return secure base node state
     */
    NodeState getSecureBase() {
        return new SecureNodeState(base, permissionProvider.get(), getAcContext());
    }

    void updated() {
        modCount++;
    }

    //------------------------------------------------------------< private >---

    /**
     * Root node state of the tree including all transient changes at the time of
     * this call.
     *
     * @return root node state
     */
    @Nonnull
    private NodeState getRootState() {
        return builder.getNodeState();
    }

    @Nonnull
    private Context getAcContext() {
        return getAcConfig().getContext();
    }

    @Nonnull
    private AuthorizationConfiguration getAcConfig() {
        return securityProvider.getConfiguration(AuthorizationConfiguration.class);
    }

    //---------------------------------------------------------< MoveRecord >---

    /**
     * Instances of this class record move operations which took place on this root.
     * They form a singly linked list where each move instance points to the next one.
     * The last entry in the list is always an empty slot to be filled in by calling
     * {@code setMove()}. This fills the slot with the source and destination of the move
     * and links this move to the next one which will be the new empty slot.
     * <p/>
     * Moves can be applied to {@code MutableTree} instances by calling {@code apply()},
     * which will execute all moves in the list on the passed tree instance
     */
    class Move {

        /**
         * source path
         */
        private String source;

        /**
         * Parent tree of the destination
         */
        private MutableTree destParent;

        /**
         * Name at the destination
         */
        private String destName;

        /**
         * Pointer to the next move. {@code null} if this is the last, empty slot
         */
        private Move next;

        /**
         * Set this move to the given source and destination. Creates a new empty slot,
         * sets this as the next move and returns it.
         */
        Move setMove(String source, MutableTree destParent, String destName) {
            this.source = source;
            this.destParent = destParent;
            this.destName = destName;
            return next = new Move();
        }

        /**
         * Apply this and all subsequent moves to the passed tree instance.
         */
        Move apply(MutableTree tree) {
            Move move = this;
            while (move.next != null) {
                if (move.source.equals(tree.getPathInternal())) {
                    tree.setParentAndName(move.destParent, move.destName);
                }
                move = move.next;
            }
            return move;
        }

        @Override
        public String toString() {
            return source == null
                    ? "NIL"
                    : '>' + source + ':' + PathUtils.concat(destParent.getPathInternal(), destName);
        }
    }
}
