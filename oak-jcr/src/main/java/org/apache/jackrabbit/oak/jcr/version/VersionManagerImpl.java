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
package org.apache.jackrabbit.oak.jcr.version;

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.lock.LockException;
import javax.jcr.lock.LockManager;
import javax.jcr.version.Version;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.commons.iterator.NodeIteratorAdapter;
import org.apache.jackrabbit.oak.jcr.SessionContext;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionOperation;
import org.apache.jackrabbit.oak.jcr.delegate.VersionManagerDelegate;
import org.apache.jackrabbit.oak.util.TODO;

public class VersionManagerImpl implements VersionManager {

    private final SessionContext sessionContext;
    private final VersionManagerDelegate versionManagerDelegate;

    public VersionManagerImpl(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.versionManagerDelegate = VersionManagerDelegate.create(sessionContext.getSessionDelegate());
    }

    @Override
    public Node setActivity(Node activity) throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public void restoreByLabel(
            String absPath, String versionLabel, boolean removeExisting)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public void restore(
            String absPath, Version version, boolean removeExisting)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public void restore(
            String absPath, String versionName, boolean removeExisting)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public void restore(Version version, boolean removeExisting)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public void restore(Version[] versions, boolean removeExisting)
            throws ItemExistsException,
            UnsupportedRepositoryOperationException, VersionException,
            LockException, InvalidItemStateException, RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public void removeActivity(Node activityNode)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public NodeIterator merge(
            String absPath, String srcWorkspace,
            boolean bestEffort, boolean isShallow)
            throws RepositoryException {
        return TODO.unimplemented().returnValue(NodeIteratorAdapter.EMPTY);
    }

    @Override
    public NodeIterator merge(
            String absPath, String srcWorkspace, boolean bestEffort)
            throws RepositoryException {
        return TODO.unimplemented().returnValue(NodeIteratorAdapter.EMPTY);
    }

    @Override
    public NodeIterator merge(Node activityNode) throws RepositoryException {
        return TODO.unimplemented().returnValue(NodeIteratorAdapter.EMPTY);
    }

    private String getOakPathOrThrowNotFound(String absPath) throws PathNotFoundException {
        return sessionContext.getOakPathOrThrowNotFound(absPath);
    }

    @Override
    public boolean isCheckedOut(final String absPath) throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                return versionManagerDelegate.isCheckedOut(nodeDelegate);
            }
        });
    }

    @Override
    public VersionHistory getVersionHistory(final String absPath)
            throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        return sessionDelegate.perform(new SessionOperation<VersionHistory>() {
            @Override
            public VersionHistory perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                return new VersionHistoryImpl(
                        versionManagerDelegate.getVersionHistory(nodeDelegate), sessionContext);
            }
        });
    }

    @Override
    public Version getBaseVersion(final String absPath) throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        return sessionDelegate.perform(new SessionOperation<Version>() {
            @Override
            public Version perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                return new VersionImpl(
                        versionManagerDelegate.getBaseVersion(nodeDelegate), sessionContext);
            }
        });
    }

    @Override
    public Node getActivity() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public void doneMerge(String absPath, Version version)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public Node createConfiguration(String absPath) throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public Node createActivity(String title) throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public Version checkpoint(String absPath) throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Nonnull
    private LockManager getLockManager() {
        return sessionContext.getLockManager();
    }

    @Override
    public void checkout(final String absPath) throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                if (getLockManager().isLocked(absPath)) {
                    throw new LockException("Node at " + absPath + " is locked");
                }
                versionManagerDelegate.checkout(nodeDelegate);
                return null;
            }
        });
    }

    @Override
    public Version checkin(final String absPath) throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        return sessionDelegate.perform(new SessionOperation<Version>() {
            @Override
            public Version perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                if (getLockManager().isLocked(absPath)) {
                    throw new LockException("Node at " + absPath + " is locked");
                }
                return new VersionImpl(versionManagerDelegate.checkin(nodeDelegate), sessionContext);
            }
        });
    }

    @Override
    public void cancelMerge(String absPath, Version version)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

}
