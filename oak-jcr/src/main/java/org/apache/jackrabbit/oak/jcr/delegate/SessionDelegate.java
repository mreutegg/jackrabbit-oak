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
package org.apache.jackrabbit.oak.jcr.delegate;

import java.io.IOException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.ItemExistsException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.apache.jackrabbit.oak.jcr.SessionContextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * TODO document
 *
 *
 * Responsibilities of JCR Impl classes:
 * * name/path mapping for both method arguments and return values
 * ** NamePathMapper should be in SessionImpl instead of SessionDelegate
 * * tracking and instantiation of other JCR Impl objects
 * ** Delegate classes should refer to neither the JCR API nor the Impl classes
 * ** Values should be returned as PropertyState instances that are mapped to JCR Values by an Impl class
 *
 * Delegate classes
 * * access to the Oak API
 * * the checkStatus() and perform() logic
 * ** Something like:
 * *** all the "business logic" associated with complex operations
 * *** the complex SessionObject classes from Impl classes should be pushed down to Delegates
 * *** dlg.perform(dlg.getSomeOperation(oakName, ...))
 *
 */
public class SessionDelegate {
    static final Logger log = LoggerFactory.getLogger(SessionDelegate.class);

    private final ContentSession contentSession;
    private final Root root;
    private final IdentifierManager idManager;

    private boolean isAlive = true;
    private int sessionOpCount;
    private int revision;

    private abstract class SessionReadOperation<T> extends SessionOperation<T> {
        @Override
        protected void checkPreconditions() throws RepositoryException {
            checkAlive();
        }
    }

    private abstract class SessionWriteOperation<T> extends SessionReadOperation<T> {
        @Override
        protected void checkPreconditions() throws RepositoryException {
            super.checkPreconditions();
            if (isReadOnly()) {
                throw new RepositoryException("This session is read only");
            }
        }
    }

    public SessionDelegate(@Nonnull ContentSession contentSession) {
        this.contentSession = checkNotNull(contentSession);
        this.root = contentSession.getLatestRoot();
        this.idManager = new IdentifierManager(root);
    }

    protected void refresh() {
    }

    /**
     * Performs the passed {@code SessionOperation} in a safe execution context. This
     * context ensures that the session is refreshed if necessary and that refreshing
     * occurs before the session operation is performed and the refreshing is done only
     * once.
     *
     * @param sessionOperation  the {@code SessionOperation} to perform
     * @param <T>  return type of {@code sessionOperation}
     * @return  the result of {@code sessionOperation.perform()}
     * @throws RepositoryException
     */
    public synchronized <T> T perform(SessionOperation<T> sessionOperation) throws RepositoryException {
        // Synchronize to avoid conflicting refreshes from concurrent JCR API calls
        try {
            if (sessionOpCount == 0) {
                // Refresh only for non re-entrant session operations
                refresh();
            }
            sessionOpCount++;
            sessionOperation.checkPreconditions();
            return sessionOperation.perform();
        } finally {
            sessionOpCount--;
        }
    }

    public <T> T safePerform(SessionOperation<T> sessionOperation) {
        try {
            return perform(sessionOperation);
        } catch (RepositoryException e) {
            String msg = sessionOperation + "threw an unexpected exception";
            log.error(msg, e);
            throw new IllegalArgumentException(msg, e);
        }
    }

    @Nonnull
    public Root getRoot() {
        return root;
    }

    @Nonnull
    public ContentSession getContentSession() {
        return contentSession;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public void checkAlive() throws RepositoryException {
        if (!isAlive()) {
            throw new RepositoryException("This session has been closed.");
        }
    }

    public boolean isReadOnly() {
        return false;
    }

    @Nonnull
    public AuthInfo getAuthInfo() {
        return contentSession.getAuthInfo();
    }

    public void logout() {
        if (!isAlive) {
            // ignore
            return;
        }

        isAlive = false;
        // TODO

        try {
            contentSession.close();
        } catch (IOException e) {
            log.warn("Error while closing connection", e);
        }
    }

    @Nonnull
    public IdentifierManager getIdManager() {
        return idManager;
    }

    @Nonnull
    public TreeLocation getLocation(String path) {
        return root.getLocation(path);
    }

    @CheckForNull
    public NodeDelegate getRootNode() {
        return getNode("/");
    }

    /**
     * {@code NodeDelegate} at the given path
     * @param path Oak path
     * @return  The {@code NodeDelegate} at {@code path} or {@code null} if
     * none exists or not accessible.
     */
    @CheckForNull
    public NodeDelegate getNode(String path) {
        return NodeDelegate.create(this, getLocation(path));
    }

    @CheckForNull
    public NodeDelegate getNodeByIdentifier(String id) {
        Tree tree = idManager.getTree(id);
        return (tree == null) ? null : new NodeDelegate(this, tree);
    }

    /**
     * {@code PropertyDelegate} at the given path
     * @param path Oak path
     * @return  The {@code PropertyDelegate} at {@code path} or {@code null} if
     * none exists or not accessible.
     */
    @CheckForNull
    public PropertyDelegate getProperty(String path) {
        TreeLocation location = root.getLocation(path);
        return location.getProperty() == null
            ? null
            : new PropertyDelegate(this, location);
    }

    public boolean hasPendingChanges() {
        return root.hasPendingChanges();
    }

    public void save() throws RepositoryException {
        try {
            root.commit();
            revision++;
        } catch (CommitFailedException e) {
            e.throwRepositoryException();
        }
    }

    public void refresh(boolean keepChanges) {
        if (keepChanges) {
            root.rebase();
        } else {
            root.refresh();
        }
        revision++;
    }

    //----------------------------------------------------------< Workspace >---

    @Nonnull
    public String getWorkspaceName() {
        return contentSession.getWorkspaceName();
    }

    /**
     * Copy a node
     * @param srcPath  oak path to the source node to copy
     * @param destPath  oak path to the destination
     * @throws RepositoryException
     */
    public void copy(String srcPath, String destPath) throws RepositoryException {
        // check destination
        Tree dest = getTree(destPath);
        if (dest != null) {
            throw new ItemExistsException(destPath);
        }

        // check parent of destination
        String destParentPath = PathUtils.getParentPath(destPath);
        Tree destParent = getTree(destParentPath);
        if (destParent == null) {
            throw new PathNotFoundException(PathUtils.getParentPath(destPath));
        }

        // check source exists
        Tree src = getTree(srcPath);
        if (src == null) {
            throw new PathNotFoundException(srcPath);
        }

        try {
            Root currentRoot = contentSession.getLatestRoot();
            currentRoot.copy(srcPath, destPath);
            currentRoot.commit();
        }
        catch (CommitFailedException e) {
            e.throwRepositoryException();
        }
    }

    /**
     * Move a node
     * @param srcPath  oak path to the source node to copy
     * @param destPath  oak path to the destination
     * @param transientOp  whether or not to perform the move in transient space
     * @throws RepositoryException
     */
    public void move(String srcPath, String destPath, boolean transientOp)
            throws RepositoryException {

        Root moveRoot = transientOp ? root : contentSession.getLatestRoot();

        // check destination
        Tree dest = moveRoot.getTree(destPath);
        if (dest != null) {
            throw new ItemExistsException(destPath);
        }

        // check parent of destination
        String destParentPath = PathUtils.getParentPath(destPath);
        Tree destParent = moveRoot.getTree(destParentPath);
        if (destParent == null) {
            throw new PathNotFoundException(PathUtils.getParentPath(destPath));
        }

        // check source exists
        Tree src = moveRoot.getTree(srcPath);
        if (src == null) {
            throw new PathNotFoundException(srcPath);
        }

        try {
            moveRoot.move(srcPath, destPath);
            if (!transientOp) {
                moveRoot.commit();
            }
        } catch (CommitFailedException e) {
            e.throwRepositoryException();
        }
    }

    @Nonnull
    public QueryEngine getQueryEngine() {
        return root.getQueryEngine();
    }

    //-----------------------------------------------------------< internal >---

    /**
     * Revision of this session. The revision is incremented each time a session is refreshed or saved.
     * This allows items to determine whether they need to re-resolve their underlying state when the
     * revision on which an item is based does not match the revision of the session any more.
     * @return  the current revision of this session
     */
    int getRevision() {
        return revision;
    }

    /**
     * Get the {@code Tree} with the given path
     * @param path  oak path
     * @return  tree at the given path or {@code null} if no such tree exists or
     * if the tree at {@code path} is not accessible.
     */
    @CheckForNull
    private Tree getTree(String path) {
        return root.getTree(path);
    }

    public void checkProtectedNodes(String... absJcrPaths) throws RepositoryException {
        Session session = SessionContextProvider.getSession(this);
        for (String absPath : absJcrPaths) {
            NodeImpl<?> node = (NodeImpl<?>) session.getNode(absPath);
            node.checkProtected();
        }
    }
}
