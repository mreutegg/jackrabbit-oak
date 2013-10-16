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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PermissionProviderImplTest extends AbstractSecurityTest implements AccessControlConstants {

    private static final String ADMINISTRATOR_GROUP = "administrators";
    private static final String[] READ_PATHS = new String[] {
            NamespaceConstants.NAMESPACES_PATH,
            NodeTypeConstants.NODE_TYPES_PATH,
            PrivilegeConstants.PRIVILEGES_PATH,
            "/test"
    };

    private Group adminstrators;

    @Override
    public void before() throws Exception {
        super.before();

        new NodeUtil(root.getTree("/")).addChild("test", JcrConstants.NT_UNSTRUCTURED);
        UserManager uMgr = getUserManager(root);
        adminstrators = uMgr.createGroup(ADMINISTRATOR_GROUP);
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            root.getTree("/test").remove();
            UserManager uMgr = getUserManager(root);
            if (adminstrators != null) {
                uMgr.getAuthorizable(adminstrators.getID()).remove();
            }
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(PermissionConstants.PARAM_READ_PATHS, READ_PATHS);
        map.put(PermissionConstants.PARAM_ADMINISTRATIVE_PRINCIPALS, new String[] {ADMINISTRATOR_GROUP});
        ConfigurationParameters acConfig = ConfigurationParameters.of(map);

        return ConfigurationParameters.of(ImmutableMap.of(AuthorizationConfiguration.NAME, acConfig));
    }

    @Test
    public void testReadPath() throws Exception {
        ContentSession testSession = createTestSession();
        try {
            Root r = testSession.getLatestRoot();
            PermissionProvider pp = new PermissionProviderImpl(testSession.getLatestRoot(), testSession.getAuthInfo().getPrincipals(), getSecurityProvider());

            Tree tree = r.getTree("/");
            assertFalse(tree.exists());
            assertFalse(pp.getTreePermission(tree, TreePermission.EMPTY).canRead());

            for (String path : READ_PATHS) {
                tree = r.getTree(path);
                assertTrue(tree.exists());
                assertTrue(pp.getTreePermission(tree, TreePermission.EMPTY).canRead());
            }
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testIsGrantedForReadPaths() throws Exception {
        ContentSession testSession = createTestSession();
        try {
            PermissionProvider pp = new PermissionProviderImpl(testSession.getLatestRoot(), testSession.getAuthInfo().getPrincipals(), getSecurityProvider());
            for (String path : READ_PATHS) {
                assertTrue(pp.isGranted(path, Permissions.getString(Permissions.READ)));
                assertTrue(pp.isGranted(path, Permissions.getString(Permissions.READ_NODE)));
                assertTrue(pp.isGranted(path + '/' + JcrConstants.JCR_PRIMARYTYPE, Permissions.getString(Permissions.READ_PROPERTY)));
                assertFalse(pp.isGranted(path, Permissions.getString(Permissions.READ_ACCESS_CONTROL)));
            }

            for (String path : READ_PATHS) {
                Tree tree = root.getTree(path);
                assertTrue(pp.isGranted(tree, null, Permissions.READ));
                assertTrue(pp.isGranted(tree, null, Permissions.READ_NODE));
                assertTrue(pp.isGranted(tree, tree.getProperty(JcrConstants.JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
                assertFalse(pp.isGranted(tree, null, Permissions.READ_ACCESS_CONTROL));
            }

            RepositoryPermission rp = pp.getRepositoryPermission();
            assertFalse(rp.isGranted(Permissions.READ));
            assertFalse(rp.isGranted(Permissions.READ_NODE));
            assertFalse(rp.isGranted(Permissions.READ_PROPERTY));
            assertFalse(rp.isGranted(Permissions.READ_ACCESS_CONTROL));
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testGetPrivilegesForReadPaths() throws Exception {
        ContentSession testSession = createTestSession();
        try {
            PermissionProvider pp = new PermissionProviderImpl(testSession.getLatestRoot(), testSession.getAuthInfo().getPrincipals(), getSecurityProvider());
            for (String path : READ_PATHS) {
                Tree tree = root.getTree(path);
                assertEquals(Collections.singleton(PrivilegeConstants.JCR_READ), pp.getPrivileges(tree));
            }
            assertEquals(Collections.<String>emptySet(), pp.getPrivileges(null));
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testHasPrivilegesForReadPaths() throws Exception {
        ContentSession testSession = createTestSession();
        try {
            PermissionProvider pp = new PermissionProviderImpl(testSession.getLatestRoot(), testSession.getAuthInfo().getPrincipals(), getSecurityProvider());
            for (String path : READ_PATHS) {
                Tree tree = root.getTree(path);
                assertTrue(pp.hasPrivileges(tree, PrivilegeConstants.JCR_READ));
                assertTrue(pp.hasPrivileges(tree, PrivilegeConstants.REP_READ_NODES));
                assertTrue(pp.hasPrivileges(tree, PrivilegeConstants.REP_READ_PROPERTIES));
                assertFalse(pp.hasPrivileges(tree, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
            }
            assertFalse(pp.hasPrivileges(null, PrivilegeConstants.JCR_READ));
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testAdministatorConfig() throws Exception {
        adminstrators.addMember(getTestUser());
        root.commit();

        ContentSession testSession = createTestSession();
        try {
            Root r = testSession.getLatestRoot();
            Root immutableRoot = new ImmutableRoot(r, TreeTypeProvider.EMPTY);

            PermissionProvider pp = new PermissionProviderImpl(testSession.getLatestRoot(), testSession.getAuthInfo().getPrincipals(), getSecurityProvider());

            assertTrue(r.getTree("/").exists());
            TreePermission tp = pp.getTreePermission(immutableRoot.getTree("/"), TreePermission.EMPTY);
            assertSame(TreePermission.ALL, tp);

            for (String path : READ_PATHS) {
                Tree tree = r.getTree(path);
                assertTrue(tree.exists());
                assertSame(TreePermission.ALL, pp.getTreePermission(tree, TreePermission.EMPTY));
            }
        } finally {
            testSession.close();
        }
    }
}