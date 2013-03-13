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
package org.apache.jackrabbit.oak.jcr;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.Credentials;
import javax.jcr.Item;
import javax.jcr.ItemNotFoundException;
import javax.jcr.NamespaceException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.Workspace;
import javax.jcr.lock.LockManager;
import javax.jcr.retention.RetentionManager;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.AbstractSession;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.PropertyDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionOperation;
import org.apache.jackrabbit.oak.jcr.xml.ImportHandler;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.util.TODO;
import org.apache.jackrabbit.util.Text;
import org.apache.jackrabbit.util.XMLChar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

/**
 * TODO document
 */
public class SessionImpl extends AbstractSession implements JackrabbitSession {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(SessionImpl.class);

    private final SessionDelegate dlg;

    /**
     * Local namespace remappings. Prefixes as keys and namespace URIs as values.
     * <p/>
     * This map is only accessed from synchronized methods (see
     * <a href="https://issues.apache.org/jira/browse/JCR-1793">JCR-1793</a>).
     */
    private final Map<String, String> namespaces;

    SessionImpl(SessionDelegate dlg, SessionContext sessionContext, Map<String, String> namespaces) {
        this.dlg = dlg;
        this.namespaces = namespaces;
    }

    public void checkProtectedNodes(String... absJcrPaths) throws RepositoryException {
        for (String absPath : absJcrPaths) {
            NodeImpl<?> node = (NodeImpl<?>) getNode(absPath);
            node.checkProtected();
        }
    }

    //------------------------------------------------------------< Session >---

    @Override
    @Nonnull
    public Repository getRepository() {
        return SessionContextProvider.getRepository(dlg);
    }

    @Override
    public String getUserID() {
        return dlg.getAuthInfo().getUserID();
    }

    @Override
    public String[] getAttributeNames() {
        return dlg.getAuthInfo().getAttributeNames();
    }

    @Override
    public Object getAttribute(String name) {
        return dlg.getAuthInfo().getAttribute(name);
    }

    @Override
    @Nonnull
    public Workspace getWorkspace() {
        return SessionContextProvider.getWorkspace(dlg);
    }

    @Override
    @Nonnull
    public Session impersonate(Credentials credentials) throws RepositoryException {
        ensureIsAlive();

        ImpersonationCredentials impCreds = new ImpersonationCredentials(credentials, dlg.getAuthInfo());
        return getRepository().login(impCreds, dlg.getWorkspaceName());
    }

    @Override
    @Nonnull
    public ValueFactory getValueFactory() throws RepositoryException {
        ensureIsAlive();
        return SessionContextProvider.getValueFactory(dlg);
    }

    @Override
    @Nonnull
    public Node getRootNode() throws RepositoryException {
        return dlg.perform(new SessionOperation<NodeImpl<?>>() {
            @Override
            protected void checkPreconditions() throws RepositoryException {
                ensureIsAlive();
            }

            @Override
            public NodeImpl<?> perform() throws AccessDeniedException {
                NodeDelegate nd = dlg.getRootNode();
                if (nd == null) {
                    throw new AccessDeniedException("Root node is not accessible.");
                } else {
                    return new NodeImpl<NodeDelegate>(nd);
                }
            }
        });
    }

    @Override
    @Nonnull
    public Node getNodeByUUID(String uuid) throws RepositoryException {
        return getNodeByIdentifier(uuid);
    }

    @Override
    @Nonnull
    public Node getNodeByIdentifier(final String id) throws RepositoryException {
        return dlg.perform(new SessionOperation<NodeImpl<?>>() {
            @Override
            protected void checkPreconditions() throws RepositoryException {
                ensureIsAlive();
            }

            @Override
            public NodeImpl<?> perform() throws RepositoryException {
                NodeDelegate d = dlg.getNodeByIdentifier(id);
                if (d == null) {
                    throw new ItemNotFoundException("Node with id " + id + " does not exist.");
                }
                return new NodeImpl<NodeDelegate>(d);
            }
        });
    }

    @Override
    public Item getItem(String absPath) throws RepositoryException {
        if (nodeExists(absPath)) {
            return getNode(absPath);
        } else {
            return getProperty(absPath);
        }
    }

    @Override
    public boolean itemExists(String absPath) throws RepositoryException {
        return nodeExists(absPath) || propertyExists(absPath);
    }

    private String getOakPath(String absPath) throws RepositoryException {
        return SessionContextProvider.getOakPath(dlg, absPath);
    }

    @Override
    public Node getNode(final String absPath) throws RepositoryException {
        return dlg.perform(new SessionOperation<NodeImpl<?>>() {
            @Override
            protected void checkPreconditions() throws RepositoryException {
                ensureIsAlive();
            }

            @Override
            public NodeImpl<?> perform() throws RepositoryException {
                String oakPath = getOakPath(absPath);
                NodeDelegate d = dlg.getNode(oakPath);
                if (d == null) {
                    throw new PathNotFoundException("Node with path " + absPath + " does not exist.");
                }
                return new NodeImpl<NodeDelegate>(d);
            }
        });
    }

    @Override
    public boolean nodeExists(final String absPath) throws RepositoryException {
        return dlg.perform(new SessionOperation<Boolean>() {
            @Override
            protected void checkPreconditions() throws RepositoryException {
                ensureIsAlive();
            }

            @Override
            public Boolean perform() throws RepositoryException {
                String oakPath = getOakPath(absPath);
                return dlg.getNode(oakPath) != null;
            }
        });
    }

    @Override
    public Property getProperty(final String absPath) throws RepositoryException {
        if (absPath.equals("/")) {
            throw new RepositoryException("The root node is not a property");
        } else {
            return dlg.perform(new SessionOperation<PropertyImpl>() {
                @Override
                public PropertyImpl perform() throws RepositoryException {
                    String oakPath = getOakPathOrThrowNotFound(absPath);
                    TreeLocation loc = dlg.getLocation(oakPath);
                    if (loc.getProperty() == null) {
                        throw new PathNotFoundException(absPath);
                    } else {
                        return new PropertyImpl(new PropertyDelegate(dlg, loc));
                    }
                }
            });
        }
    }

    private String getOakPathOrThrowNotFound(String absPath) throws PathNotFoundException {
        return SessionContextProvider.getOakPathOrThrowNotFound(dlg, absPath);
    }

    @Override
    public boolean propertyExists(final String absPath) throws RepositoryException {
        if (absPath.equals("/")) {
            throw new RepositoryException("The root node is not a property");
        } else {
            return dlg.perform(new SessionOperation<Boolean>() {
                @Override
                public Boolean perform() throws RepositoryException {
                    String oakPath = getOakPathOrThrowNotFound(absPath);
                    TreeLocation loc = dlg.getLocation(oakPath);
                    return loc.getProperty() != null;
                }
            });
        }
    }

    @Override
    public void move(final String srcAbsPath, final String destAbsPath) throws RepositoryException {
        dlg.perform(new SessionOperation<Void>() {
            @Override
            protected void checkPreconditions() throws RepositoryException {
                ensureIsAlive();
                checkProtectedNodes(Text.getRelativeParent(srcAbsPath, 1), Text.getRelativeParent(destAbsPath, 1));
            }

            @Override
            public Void perform() throws RepositoryException {
                String oakPath = SessionContextProvider.getOakPathKeepIndexOrThrowNotFound(dlg, destAbsPath);
                String oakName = PathUtils.getName(oakPath);
                // handle index
                if (oakName.contains("[")) {
                    throw new RepositoryException("Cannot create a new node using a name including an index");
                }

                dlg.move(
                        getOakPathOrThrowNotFound(srcAbsPath),
                        getOakPathOrThrowNotFound(oakPath),
                        true);

                return null;
            }
        });
    }

    @Override
    public void save() throws RepositoryException {
        ensureIsAlive();
        dlg.save();
    }

    @Override
    public void refresh(boolean keepChanges) throws RepositoryException {
        ensureIsAlive();
        dlg.refresh(keepChanges);
    }

    @Override
    public boolean hasPendingChanges() throws RepositoryException {
        ensureIsAlive();
        return dlg.hasPendingChanges();
    }

    @Override
    public boolean isLive() {
        return dlg.isAlive();
    }


    @Override
    public void logout() {
        SessionContextProvider.remove(dlg);
        dlg.logout();
        synchronized (namespaces) {
            namespaces.clear();
        }
    }

    @Override
    @Nonnull
    public ContentHandler getImportContentHandler(
            String parentAbsPath, int uuidBehavior) throws RepositoryException {
        UserConfiguration userConfiguration = SessionContextProvider.getUserConfiguration(dlg);
        AccessControlConfiguration accessControlConfiguration = SessionContextProvider.getAccessControlConfiguration(dlg);
        return new ImportHandler(getNode(parentAbsPath), dlg.getRoot(), this,
                dlg, userConfiguration, accessControlConfiguration, uuidBehavior);
    }

    @Nonnull
    private LockManager getLockManager() {
        return SessionContextProvider.getLockManager(dlg);
    }

    /**
     * @see javax.jcr.Session#addLockToken(String)
     */
    @Override
    public void addLockToken(String lt) {
        try {
            getLockManager().addLockToken(lt);
        } catch (RepositoryException e) {
            log.warn("Unable to add lock token '{}' to this session: {}", lt, e.getMessage());
        }
    }

    /**
     * @see javax.jcr.Session#getLockTokens()
     */
    @Override
    @Nonnull
    public String[] getLockTokens() {
        try {
            return getLockManager().getLockTokens();
        } catch (RepositoryException e) {
            log.warn("Unable to retrieve lock tokens for this session: {}", e.getMessage());
            return new String[0];
        }
    }

    /**
     * @see javax.jcr.Session#removeLockToken(String)
     */
    @Override
    public void removeLockToken(String lt) {
        try {
            getLockManager().addLockToken(lt);
        } catch (RepositoryException e) {
            log.warn("Unable to add lock token '{}' to this session: {}", lt, e.getMessage());
        }
    }

    @Override
    public boolean hasPermission(String absPath, String actions) throws RepositoryException {
        ensureIsAlive();

        NamePathMapper namePathMapper = SessionContextProvider.getNamePathMapper(dlg);
        String oakPath = namePathMapper.getOakPathKeepIndex(absPath);
        if (oakPath == null) {
            throw new RepositoryException("Invalid JCR path: " + absPath);
        }

        PermissionProvider permissionProvider = SessionContextProvider.getPermissionProvider(dlg);
        return permissionProvider.hasPermission(absPath, actions);
    }

    @Override
    public void checkPermission(String absPath, String actions) throws RepositoryException {
        if (!hasPermission(absPath, actions)) {
            throw new AccessControlException("Access control violation: path = " + absPath + ", actions = " + actions);
        }
    }

    @Override
    public boolean hasCapability(String methodName, Object target, Object[] arguments) throws RepositoryException {
        ensureIsAlive();

        // TODO
        return TODO.unimplemented().returnValue(false);
    }

    @Override
    @Nonnull
    public AccessControlManager getAccessControlManager() throws RepositoryException {
        return SessionContextProvider.getAccessControlManager(dlg);
    }

    /**
     * @see javax.jcr.Session#getRetentionManager()
     */
    @Override
    @Nonnull
    public RetentionManager getRetentionManager() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Retention Management is not supported.");
    }

    //---------------------------------------------------------< Namespaces >---
    // The code below was initially copied from JCR Commons AbstractSession, but
    // provides information the "hasRemappings" information

    @Override
    public void setNamespacePrefix(String prefix, String uri) throws RepositoryException {
        if (prefix == null) {
            throw new IllegalArgumentException("Prefix must not be null");
        } else if (uri == null) {
            throw new IllegalArgumentException("Namespace must not be null");
        } else if (prefix.isEmpty()) {
            throw new NamespaceException(
                    "Empty prefix is reserved and can not be remapped");
        } else if (uri.isEmpty()) {
            throw new NamespaceException(
                    "Default namespace is reserved and can not be remapped");
        } else if (prefix.toLowerCase(Locale.ENGLISH).startsWith("xml")) {
            throw new NamespaceException(
                    "XML prefixes are reserved: " + prefix);
        } else if (!XMLChar.isValidNCName(prefix)) {
            throw new NamespaceException(
                    "Prefix is not a valid XML NCName: " + prefix);
        }

        synchronized (namespaces) {
            // Remove existing mapping for the given prefix
            namespaces.remove(prefix);

            // Remove existing mapping(s) for the given URI
            Set<String> prefixes = new HashSet<String>();
            for (Map.Entry<String, String> entry : namespaces.entrySet()) {
                if (entry.getValue().equals(uri)) {
                    prefixes.add(entry.getKey());
                }
            }
            namespaces.keySet().removeAll(prefixes);

            // Add the new mapping
            namespaces.put(prefix, uri);
        }
    }

    @Override
    public String[] getNamespacePrefixes() throws RepositoryException {
        Set<String> uris = new HashSet<String>();
        uris.addAll(Arrays.asList(getWorkspace().getNamespaceRegistry().getURIs()));
        synchronized (namespaces) {
            // Add namespace uris only visible to session
            uris.addAll(namespaces.values());
        }
        Set<String> prefixes = new HashSet<String>();
        for (String uri : uris) {
            prefixes.add(getNamespacePrefix(uri));
        }
        return prefixes.toArray(new String[prefixes.size()]);
    }

    @Override
    public String getNamespaceURI(String prefix) throws RepositoryException {
        synchronized (namespaces) {
            String uri = namespaces.get(prefix);

            if (uri == null) {
                // Not in local mappings, try the global ones
                uri = getWorkspace().getNamespaceRegistry().getURI(prefix);
                if (namespaces.containsValue(uri)) {
                    // The global URI is locally mapped to some other prefix,
                    // so there are no mappings for this prefix
                    throw new NamespaceException("Namespace not found: " + prefix);
                }
            }

            return uri;
        }
    }

    @Override
    public String getNamespacePrefix(String uri) throws RepositoryException {
        synchronized (namespaces) {
            for (Map.Entry<String, String> entry : namespaces.entrySet()) {
                if (entry.getValue().equals(uri)) {
                    return entry.getKey();
                }
            }

            // The following throws an exception if the URI is not found, that's OK
            String prefix = getWorkspace().getNamespaceRegistry().getPrefix(uri);

            // Generate a new prefix if the global mapping is already taken
            String base = prefix;
            for (int i = 2; namespaces.containsKey(prefix); i++) {
                prefix = base + i;
            }

            if (!base.equals(prefix)) {
                namespaces.put(prefix, uri);
            }
            return prefix;
        }
    }

    //--------------------------------------------------< JackrabbitSession >---

    @Override
    @Nonnull
    public PrincipalManager getPrincipalManager() throws RepositoryException {
        return SessionContextProvider.getPrincipalManager(dlg);
    }

    @Override
    @Nonnull
    public UserManager getUserManager() throws RepositoryException {
        return SessionContextProvider.getUserManager(dlg);
    }

    //------------------------------------------------------------< private >---

    /**
     * Ensure that this session is alive and throw an exception otherwise.
     *
     * @throws RepositoryException if this session has been rendered invalid
     *                             for some reason (e.g. if this session has been closed explicitly by logout)
     */
    private void ensureIsAlive() throws RepositoryException {
        // check session status
        if (!dlg.isAlive()) {
            throw new RepositoryException("This session has been closed.");
        }
    }
}