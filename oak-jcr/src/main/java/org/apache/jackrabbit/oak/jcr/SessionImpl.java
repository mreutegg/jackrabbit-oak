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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.Credentials;
import javax.jcr.InvalidSerializedDataException;
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
import org.apache.jackrabbit.commons.xml.DocumentViewExporter;
import org.apache.jackrabbit.commons.xml.Exporter;
import org.apache.jackrabbit.commons.xml.ParsingContentHandler;
import org.apache.jackrabbit.commons.xml.SystemViewExporter;
import org.apache.jackrabbit.commons.xml.ToXmlContentHandler;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.PropertyDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionOperation;
import org.apache.jackrabbit.oak.jcr.xml.ImportHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.util.TODO;
import org.apache.jackrabbit.util.Text;
import org.apache.jackrabbit.util.XMLChar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * TODO document
 */
public class SessionImpl implements JackrabbitSession {
    private static final Logger log = LoggerFactory.getLogger(SessionImpl.class);

    private final SessionContext sessionContext;
    private final SessionDelegate sd;

    /**
     * Local namespace remappings. Prefixes as keys and namespace URIs as values.
     * <p/>
     * This map is only accessed from synchronized methods (see
     * <a href="https://issues.apache.org/jira/browse/JCR-1793">JCR-1793</a>).
     */
    private final Map<String, String> namespaces;

    SessionImpl(SessionContext sessionContext, Map<String, String> namespaces) {
        this.sessionContext = sessionContext;
        this.sd = sessionContext.getSessionDelegate();
        this.namespaces = namespaces;
    }

    public static void checkProtectedNodes(Session session, String... absJcrPaths) throws RepositoryException {
        for (String absPath : absJcrPaths) {
            NodeImpl<?> node = (NodeImpl<?>) session.getNode(absPath);
            node.checkProtected();
        }
    }

    private abstract class CheckedSessionOperation<T> extends SessionOperation<T> {
        @Override
        protected void checkPreconditions() throws RepositoryException {
            sd.checkAlive();
        }
    }

    @CheckForNull
    private <T> T perform(@Nonnull CheckedSessionOperation<T> op) throws RepositoryException {
        return sd.perform(op);
    }

    @Nonnull
    private String getOakPathOrThrow(String absPath) throws RepositoryException {
        return sessionContext.getOakPathOrThrow(absPath);
    }

    @Nonnull
    private String getOakPathOrThrowNotFound(String absPath) throws PathNotFoundException {
        return sessionContext.getOakPathOrThrowNotFound(absPath);
    }

    private NodeImpl<?> createNodeOrNull(NodeDelegate nd) {
        return nd == null ? null : new NodeImpl<NodeDelegate>(nd, sessionContext);
    }

    private PropertyImpl createPropertyOrNull(PropertyDelegate pd) {
        return pd == null ? null : new PropertyImpl(pd, sessionContext);
    }

    @CheckForNull
    private ItemImpl<?> getItemInternal(@Nonnull String oakPath) {
        NodeDelegate nd = sd.getNode(oakPath);
        if (nd != null) {
            return createNodeOrNull(nd);
        }
        PropertyDelegate pd = sd.getProperty(oakPath);
        if (pd != null) {
            return createPropertyOrNull(pd);
        }
        return null;
    }

    /**
     * Returns the node at the specified absolute path in the workspace or
     * {@code null} if no such node exists.
     *
     * @param absPath An absolute path.
     * @return the specified {@code Node} or {@code null}.
     * @throws RepositoryException   If another error occurs.
     */
    @CheckForNull
    public Node getNodeOrNull(final String absPath) throws RepositoryException {
        return perform(new CheckedSessionOperation<Node>() {
            @Override
            public Node perform() throws RepositoryException {
                return createNodeOrNull(sd.getNode(getOakPathOrThrow(absPath)));
            }
        });
    }

    /**
     * Returns the property at the specified absolute path in the workspace or
     * {@code null} if no such node exists.
     *
     * @param absPath An absolute path.
     * @return the specified {@code Property} or {@code null}.
     * @throws RepositoryException   if another error occurs.
     */
    @CheckForNull
    public Property getPropertyOrNull(final String absPath) throws RepositoryException {
        if (absPath.equals("/")) {
            return null;
        } else {
            return perform(new CheckedSessionOperation<Property>() {
                @Override
                public Property perform() throws RepositoryException {
                    return createPropertyOrNull(sd.getProperty(getOakPathOrThrow(absPath)));
                }
            });
        }
    }

    /**
     * Returns the node at the specified absolute path in the workspace. If no
     * such node exists, then it returns the property at the specified path.
     * If no such property exists, then it return {@code null}.
     *
     * @param absPath An absolute path.
     * @return the specified {@code Item} or {@code null}.
     * @throws RepositoryException   if another error occurs.
     */
    @CheckForNull
    public Item getItemOrNull(final String absPath) throws RepositoryException {
        return perform(new CheckedSessionOperation<Item>() {
            @Override
            public Item perform() throws RepositoryException {
                return getItemInternal(getOakPathOrThrow(absPath));
            }
        });
    }

    //------------------------------------------------------------< Session >---

    @Override
    @Nonnull
    public Repository getRepository() {
        return sessionContext.getRepository();
    }

    @Override
    public String getUserID() {
        return sd.getAuthInfo().getUserID();
    }

    @Override
    public String[] getAttributeNames() {
        return sd.getAuthInfo().getAttributeNames();
    }

    @Override
    public Object getAttribute(String name) {
        return sd.getAuthInfo().getAttribute(name);
    }

    @Override
    @Nonnull
    public Workspace getWorkspace() {
        return sessionContext.getWorkspace();
    }

    @Override
    @Nonnull
    public Session impersonate(Credentials credentials) throws RepositoryException {
        sd.checkAlive();

        ImpersonationCredentials impCreds = new ImpersonationCredentials(credentials, sd.getAuthInfo());
        return getRepository().login(impCreds, sd.getWorkspaceName());
    }

    @Override
    @Nonnull
    public ValueFactory getValueFactory() throws RepositoryException {
        sd.checkAlive();
        return sessionContext.getValueFactory();
    }

    @Override
    @Nonnull
    public Node getRootNode() throws RepositoryException {
        return perform(new CheckedSessionOperation<Node>() {
            @Override
            protected Node perform() throws AccessDeniedException {
                NodeDelegate nd = sd.getRootNode();
                if (nd == null) {
                    throw new AccessDeniedException("Root node is not accessible.");
                }
                return createNodeOrNull(nd);
            }
        });
    }

    @Override
    public Node getNode(String absPath) throws RepositoryException {
        Node node = getNodeOrNull(absPath);
        if (node == null) {
            throw new PathNotFoundException("Node with path " + absPath + " does not exist.");
        }
        return node;
    }

    @Override
    public boolean nodeExists(String absPath) throws RepositoryException {
        return getNodeOrNull(absPath) != null;
    }

    @Nonnull
    private Node getNodeById(final String id) throws RepositoryException {
        return perform(new CheckedSessionOperation<Node>() {
            @Override
            public Node perform() throws ItemNotFoundException {
                NodeDelegate nd = sd.getNodeByIdentifier(id);
                if (nd == null) {
                    throw new ItemNotFoundException("Node with id " + id + " does not exist.");
                }
                return createNodeOrNull(nd);
            }
        });
    }

    @Override
    @Nonnull
    public Node getNodeByUUID(String uuid) throws RepositoryException {
        return getNodeById(uuid);
    }

    @Override
    @Nonnull
    public Node getNodeByIdentifier(String id) throws RepositoryException {
        return getNodeById(id);
    }

    @Override
    public Property getProperty(String absPath) throws RepositoryException {
        Property property = getPropertyOrNull(absPath);
        if (property == null) {
            throw new PathNotFoundException(absPath);
        }
        return property;
    }

    @Override
    public boolean propertyExists(String absPath) throws RepositoryException {
        return getPropertyOrNull(absPath) != null;
    }

    @Override
    public Item getItem(String absPath) throws RepositoryException {
        Item item = getItemOrNull(absPath);
        if (item == null) {
            throw new PathNotFoundException(absPath);
        }
        return item;
    }

    @Override
    public boolean itemExists(String absPath) throws RepositoryException {
        return getItemOrNull(absPath) != null;
    }

    @Override
    public void move(final String srcAbsPath, final String destAbsPath) throws RepositoryException {
        sd.perform(new CheckedSessionOperation<Void>() {
            @Override
            protected void checkPreconditions() throws RepositoryException {
                super.checkPreconditions();
                checkProtectedNodes(SessionImpl.this,
                        Text.getRelativeParent(srcAbsPath, 1), Text.getRelativeParent(destAbsPath, 1));
            }

            @Override
            public Void perform() throws RepositoryException {
                String oakDestPath = sessionContext.getOakPathKeepIndexOrThrowNotFound(destAbsPath);
                // handle index
                if (PathUtils.getName(oakDestPath).contains("[")) {
                    throw new RepositoryException("Cannot create a new node using a name including an index");
                }

                sd.move(getOakPathOrThrowNotFound(srcAbsPath), oakDestPath, true);
                return null;
            }
        });
    }

    @Override
    public void removeItem(final String absPath) throws RepositoryException {
        perform(new CheckedSessionOperation<Void>() {
            @Override
            protected Void perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                ItemImpl<?> item = getItemInternal(oakPath);
                if (item == null) {
                    throw new PathNotFoundException(absPath);
                }

                item.checkProtected();
                item.remove();
                return null;
            }
        });
    }

    @Override
    public void save() throws RepositoryException {
        sd.checkAlive();
        sd.save();
        sessionContext.refresh();
    }

    @Override
    public void refresh(boolean keepChanges) throws RepositoryException {
        sd.checkAlive();
        sd.refresh(keepChanges);
        sessionContext.refresh();
    }

    @Override
    public boolean hasPendingChanges() throws RepositoryException {
        sd.checkAlive();
        return sd.hasPendingChanges();
    }

    @Override
    public boolean isLive() {
        return sd.isAlive();
    }


    @Override
    public void logout() {
        if (sd.isAlive()) {
            sessionContext.dispose();
            sd.logout();
            synchronized (namespaces) {
                namespaces.clear();
            }
        }
    }

    @Override
    @Nonnull
    public ContentHandler getImportContentHandler(String parentAbsPath, int uuidBehavior)
            throws RepositoryException {
        return new ImportHandler(getNode(parentAbsPath), sessionContext, uuidBehavior);
    }

    @Override
    public void importXML(String parentAbsPath, InputStream in, int uuidBehavior)
            throws IOException, RepositoryException {
        try {
            ContentHandler handler = getImportContentHandler(parentAbsPath, uuidBehavior);
            new ParsingContentHandler(handler).parse(in);
        } catch (SAXException e) {
            Throwable exception = e.getException();
            if (exception instanceof RepositoryException) {
                throw (RepositoryException) exception;
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
            } else {
                throw new InvalidSerializedDataException("XML parse error", e);
            }
        } finally {
            // JCR-2903
            if (in != null) {
                try { in.close(); } catch (IOException ignore) {}
            }
        }
    }

    /**
     * Exports content at the given path using the given exporter.
     *
     * @param path of the node to be exported
     * @param exporter document or system view exporter
     * @throws SAXException if the SAX event handler failed
     * @throws RepositoryException if another error occurs
     */
    private synchronized void export(String path, Exporter exporter)
            throws SAXException, RepositoryException {
        Item item = getItem(path);
        if (item.isNode()) {
            exporter.export((Node) item);
        } else {
            throw new PathNotFoundException("XML export is not defined for properties: " + path);
        }
    }

    @Override
    public void exportSystemView(String absPath, ContentHandler contentHandler, boolean skipBinary, boolean noRecurse)
            throws SAXException, RepositoryException {
        export(absPath, new SystemViewExporter(this, contentHandler, !noRecurse, !skipBinary));
    }

    @Override
    public void exportSystemView(String absPath, OutputStream out, boolean skipBinary, boolean noRecurse)
            throws IOException, RepositoryException {
        try {
            ContentHandler handler = new ToXmlContentHandler(out);
            export(absPath, new SystemViewExporter(this, handler, !noRecurse, !skipBinary));
        } catch (SAXException e) {
            Exception exception = e.getException();
            if (exception instanceof RepositoryException) {
                throw (RepositoryException) exception;
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
            } else {
                throw new RepositoryException("Error serializing system view XML", e);
            }
        }
    }

    @Override
    public void exportDocumentView(String absPath, ContentHandler contentHandler, boolean skipBinary,
            boolean noRecurse) throws SAXException, RepositoryException {
        export(absPath, new DocumentViewExporter(this, contentHandler, !noRecurse, !skipBinary));
    }

    @Override
    public void exportDocumentView(String absPath, OutputStream out, boolean skipBinary, boolean noRecurse)
            throws IOException, RepositoryException {
        try {
            ContentHandler handler = new ToXmlContentHandler(out);
            export(absPath, new DocumentViewExporter(this, handler, !noRecurse, !skipBinary));
        } catch (SAXException e) {
            Exception exception = e.getException();
            if (exception instanceof RepositoryException) {
                throw (RepositoryException) exception;
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
            } else {
                throw new RepositoryException("Error serializing document view XML", e);
            }
        }
    }

    @Nonnull
    private LockManager getLockManager() {
        return sessionContext.getLockManager();
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
    public boolean hasPermission(final String absPath, final String actions) throws RepositoryException {
        return perform(new CheckedSessionOperation<Boolean>() {
            @Override
            protected Boolean perform() throws RepositoryException {
                String oakPath = getOakPathOrThrow(absPath);
                return sessionContext.getPermissionProvider().isGranted(oakPath, actions);
            }
        });
    }

    @Override
    public void checkPermission(String absPath, String actions) throws RepositoryException {
        if (!hasPermission(absPath, actions)) {
            throw new AccessControlException("Access control violation: path = " + absPath + ", actions = " + actions);
        }
    }

    @Override
    public boolean hasCapability(String methodName, Object target, Object[] arguments) throws RepositoryException {
        sd.checkAlive();

        // TODO
        return TODO.unimplemented().returnValue(false);
    }

    @Override
    @Nonnull
    public AccessControlManager getAccessControlManager() throws RepositoryException {
        return sessionContext.getAccessControlManager();
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
        synchronized (namespaces) {
            if (namespaces.isEmpty()) {
                return getWorkspace().getNamespaceRegistry().getPrefixes();
            }
        }
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
        return sessionContext.getPrincipalManager();
    }

    @Override
    @Nonnull
    public UserManager getUserManager() throws RepositoryException {
        return sessionContext.getUserManager();
    }

}