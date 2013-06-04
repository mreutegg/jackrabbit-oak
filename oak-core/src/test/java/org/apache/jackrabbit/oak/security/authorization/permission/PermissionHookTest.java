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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AbstractAccessControlTest;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Testing the {@code PermissionHook}
 */
public class PermissionHookTest extends AbstractAccessControlTest implements AccessControlConstants, PermissionConstants, PrivilegeConstants {

    private String testPath = "/testPath";
    private String childPath = "/testPath/childNode";

    private String testPrincipalName;
    private PrivilegeBitsProvider bitsProvider;
    private List<Principal> principals = new ArrayList<Principal>();

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        Principal testPrincipal = getTestPrincipal();
        NodeUtil rootNode = new NodeUtil(root.getTree("/"), namePathMapper);
        NodeUtil testNode = rootNode.addChild("testPath", JcrConstants.NT_UNSTRUCTURED);
        testNode.addChild("childNode", JcrConstants.NT_UNSTRUCTURED);

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(JCR_ADD_CHILD_NODES));
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_READ));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        testPrincipalName = testPrincipal.getName();
        bitsProvider = new PrivilegeBitsProvider(root);
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            root.refresh();
            Tree test = root.getTree(testPath);
            if (test.exists()) {
                test.remove();
            }

            for (Principal principal : principals) {
                getUserManager(root).getAuthorizable(principal).remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    private Tree getPrincipalRoot(String principalName) {
        return root.getTree(PERMISSIONS_STORE_PATH).getChild(adminSession.getWorkspaceName()).getChild(principalName);
    }

    private Tree getEntry(String principalName, String accessControlledPath, long index) throws Exception {
        Tree principalRoot = getPrincipalRoot(principalName);
		return traverse(principalRoot, accessControlledPath, index);
    }
	
	private Tree traverse(Tree parent, String accessControlledPath, long index) throws Exception {
		for (Tree entry : parent.getChildren()) {
		    String path = entry.getProperty(REP_ACCESS_CONTROLLED_PATH).getValue(Type.STRING);
            long entryIndex = entry.getProperty(REP_INDEX).getValue(Type.LONG);
            if (accessControlledPath.equals(path)) {
                if (index == entryIndex) {
                    return entry;
                } else if (index > entryIndex) {
                    return traverse(entry, accessControlledPath, index);
                }
            } else if (Text.isDescendant(path, accessControlledPath)) {
				return traverse(entry, accessControlledPath, index);
			}
        }
	    throw new RepositoryException("no such entry");
	}
	
	private long cntEntries(Tree parent) {
        long cnt = parent.getChildrenCount();
		for (Tree child : parent.getChildren()) {
			cnt += cntEntries(child);
		}
		return cnt;	
	}

    private void createPrincipals() throws Exception {
        if (principals.isEmpty()) {
            for (int i = 0; i < 10; i++) {
                Group gr = getUserManager(root).createGroup("testGroup"+i);
                principals.add(gr.getPrincipal());
            }
            root.commit();
        }
    }

    @Test
    public void testDuplicateAce() throws Exception {
        // add duplicate policy on OAK-API
        NodeUtil policy = new NodeUtil(root.getTree(testPath + "/rep:policy"));
        NodeUtil ace = policy.addChild("duplicateAce", NT_REP_GRANT_ACE);
        ace.setString(REP_PRINCIPAL_NAME, testPrincipalName);
        ace.setStrings(AccessControlConstants.REP_PRIVILEGES, JCR_ADD_CHILD_NODES);
        root.commit();

        Tree principalRoot = getPrincipalRoot(testPrincipalName);
        assertEquals(2, cntEntries(principalRoot));

        assertEquals(1, principalRoot.getChildrenCount());
		Tree entry1 = principalRoot.getChildren().iterator().next();
		assertEquals(bitsProvider.getBits(JCR_ADD_CHILD_NODES), PrivilegeBits.getInstance(entry1.getProperty(REP_PRIVILEGE_BITS)));
		assertEquals(testPath, entry1.getProperty(REP_ACCESS_CONTROLLED_PATH).getValue(Type.STRING));
        assertEquals(0, entry1.getProperty(REP_INDEX).getValue(Type.LONG).intValue());
		
	    assertEquals(1, entry1.getChildrenCount());
		Tree entry2 = entry1.getChildren().iterator().next();
		assertEquals(bitsProvider.getBits(JCR_ADD_CHILD_NODES), PrivilegeBits.getInstance(entry2.getProperty(REP_PRIVILEGE_BITS)));
		assertEquals(testPath, entry2.getProperty(REP_ACCESS_CONTROLLED_PATH).getValue(Type.STRING));
        assertEquals(2, entry2.getProperty(REP_INDEX).getValue(Type.LONG).intValue());

        // remove duplicate policy entry again
        root.getTree(testPath + "/rep:policy/duplicateAce").remove();
        root.commit();

        assertEquals(1, cntEntries(getPrincipalRoot(testPrincipalName)));
    }

    @Test
    public void testModifyRestrictions() throws Exception {
        Tree testAce = root.getTree(testPath + "/rep:policy").getChildren().iterator().next();
        assertEquals(testPrincipalName, testAce.getProperty(REP_PRINCIPAL_NAME).getValue(Type.STRING));

        // add a new restriction node through the OAK API instead of access control manager
        NodeUtil node = new NodeUtil(testAce);
        NodeUtil restrictions = node.addChild(REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        restrictions.setString(REP_GLOB, "*");
        String restrictionsPath = restrictions.getTree().getPath();
        root.commit();

        Tree principalRoot = getPrincipalRoot(testPrincipalName);
        assertEquals(1, cntEntries(principalRoot));
        assertEquals("*", principalRoot.getChildren().iterator().next().getProperty(REP_GLOB).getValue(Type.STRING));

        // modify the restrictions node
        Tree restrictionsNode = root.getTree(restrictionsPath);
        restrictionsNode.setProperty(REP_GLOB, "/*/jcr:content/*");
        root.commit();

        principalRoot = getPrincipalRoot(testPrincipalName);
        assertEquals(1, cntEntries(principalRoot));
        assertEquals("/*/jcr:content/*", principalRoot.getChildren().iterator().next().getProperty(REP_GLOB).getValue(Type.STRING));

        // remove the restriction again
        root.getTree(restrictionsPath).remove();
        root.commit();

        principalRoot = getPrincipalRoot(testPrincipalName);
        assertEquals(1, cntEntries(principalRoot));
        assertNull(principalRoot.getChildren().iterator().next().getProperty(REP_GLOB));
    }

    @Test
    public void testReorderAce() throws Exception {
        Tree entry = getEntry(testPrincipalName, testPath, 0);
        assertEquals(0, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());

        Tree aclTree = root.getTree(testPath + "/rep:policy");
        aclTree.getChildren().iterator().next().orderBefore(null);

        root.commit();

        entry = getEntry(testPrincipalName, testPath, 1);
        assertEquals(1, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());
    }

    @Test
    public void testReorderAndAddAce() throws Exception {
        Tree entry = getEntry(testPrincipalName, testPath, 0);
        assertEquals(0, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());

        Tree aclTree = root.getTree(testPath + "/rep:policy");
        // reorder
        aclTree.getChildren().iterator().next().orderBefore(null);

        // add a new entry
        NodeUtil ace = new NodeUtil(aclTree).addChild("denyEveryoneLockMgt", NT_REP_DENY_ACE);
        ace.setString(REP_PRINCIPAL_NAME, EveryonePrincipal.NAME);
        ace.setStrings(AccessControlConstants.REP_PRIVILEGES, JCR_LOCK_MANAGEMENT);
        root.commit();

        entry = getEntry(testPrincipalName, testPath, 1);
        assertEquals(1, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());
    }

    @Test
    public void testReorderAddAndRemoveAces() throws Exception {
        Tree entry = getEntry(testPrincipalName, testPath, 0);
        assertEquals(0, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());

        Tree aclTree = root.getTree(testPath + "/rep:policy");

        // reorder testPrincipal entry to the end
        aclTree.getChildren().iterator().next().orderBefore(null);

        Iterator<Tree> aceIt = aclTree.getChildren().iterator();
        // remove the everyone entry
        aceIt.next().remove();
        // remember the name of the testPrincipal entry.
        String name = aceIt.next().getName();

        // add a new entry
        NodeUtil ace = new NodeUtil(aclTree).addChild("denyEveryoneLockMgt", NT_REP_DENY_ACE);
        ace.setString(REP_PRINCIPAL_NAME, EveryonePrincipal.NAME);
        ace.setStrings(AccessControlConstants.REP_PRIVILEGES, JCR_LOCK_MANAGEMENT);

        // reorder the new entry before the remaining existing entry
        ace.getTree().orderBefore(name);

        root.commit();

        entry = getEntry(testPrincipalName, testPath, 1);
        assertEquals(1, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());
    }

    /**
     * ACE    :  0   1   2   3   4   5   6   7
     * Before :  tp  ev  p0  p1  p2  p3
     * After  :      ev      p2  p1  p3  p4  p5
     */
    @Test
    public void testReorderAddAndRemoveAces2() throws Exception {
        createPrincipals();

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        for (int i = 0; i < 4; i++) {
            acl.addAccessControlEntry(principals.get(i), privilegesFromNames(JCR_READ));
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        AccessControlEntry[] aces = acl.getAccessControlEntries();
        acl.removeAccessControlEntry(aces[0]);
        acl.removeAccessControlEntry(aces[2]);
        acl.orderBefore(aces[4], aces[3]);
        acl.addAccessControlEntry(principals.get(4), privilegesFromNames(JCR_READ));
        acl.addAccessControlEntry(principals.get(5), privilegesFromNames(JCR_READ));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        Tree entry = getEntry(principals.get(2).getName(), testPath, 1);
        assertEquals(1, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());

        entry = getEntry(principals.get(1).getName(), testPath, 2);
        assertEquals(2, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());
    }

    /**
     * ACE    :  0   1   2   3   4   5   6   7
     * Before :  tp  ev  p0  p1  p2  p3
     * After  :      p1      ev  p3  p2
     */
    @Test
    public void testReorderAndRemoveAces() throws Exception {
        createPrincipals();

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        for (int i = 0; i < 4; i++) {
            acl.addAccessControlEntry(principals.get(i), privilegesFromNames(JCR_READ));
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        AccessControlEntry[] aces = acl.getAccessControlEntries();
        acl.removeAccessControlEntry(aces[0]);
        acl.removeAccessControlEntry(aces[2]);
        acl.orderBefore(aces[4], null);
        acl.orderBefore(aces[3], aces[1]);
        acMgr.setPolicy(testPath, acl);
        root.commit();

        Tree entry = getEntry(EveryonePrincipal.NAME, testPath, 1);
        assertEquals(1, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());

        entry = getEntry(principals.get(2).getName(), testPath, 3);
        assertEquals(3, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());

        for (String pName : new String[] {testPrincipalName, principals.get(0).getName()}) {
            try {
                getEntry(pName, testPath, 0);
                fail();
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testImplicitAceRemoval() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addAccessControlEntry(getTestPrincipal(), privilegesFromNames(JCR_READ, REP_WRITE));
        acMgr.setPolicy(testPath, acl);

        acl = AccessControlUtils.getAccessControlList(acMgr, childPath);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_READ));
        acMgr.setPolicy(childPath, acl);
        root.commit();

        assertTrue(root.getTree(childPath+"/rep:policy").exists());

        Tree principalRoot = getPrincipalRoot(EveryonePrincipal.NAME);
        assertEquals(2, cntEntries(principalRoot));

        ContentSession testSession = createTestSession();
        Root testRoot = testSession.getLatestRoot();

        assertTrue(testRoot.getTree(childPath).exists());
        assertFalse(testRoot.getTree(childPath+"/rep:policy").exists());

        testRoot.getTree(childPath).remove();
        testRoot.commit();
        testSession.close();

        root.refresh();
        assertFalse(root.getTree(testPath).hasChild("childNode"));
        assertFalse(root.getTree(childPath+"/rep:policy").exists());
        // aces must be removed in the permission store even if the editing
        // session wasn't able to access them.
        principalRoot = getPrincipalRoot(EveryonePrincipal.NAME);
        assertEquals(1, cntEntries(principalRoot));
    }

    @Test
    public void testReorderForSinglePrincipal() throws Exception {
        Principal testPrincipal = getTestPrincipal();
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_MODIFY_ACCESS_CONTROL), false);
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_READ_ACCESS_CONTROL), true, Collections.singletonMap(REP_GLOB, getValueFactory().createValue("/*")));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        /*
        Original setup with 3 access control entries for testPrincipal @ testPath
        Expected result:
        0 - testuser - allow - JCR_ADD_CHILD_NODES       - NA
        1 - everyone - allow - READ                      - NA
        2 - testuser - deny  - JCR_MODIFY_ACCESS_CONTROL - NA
        3 - testuser - allow - JCR_READ_ACCESS_CONTROL   - /*
        */
        long rootDepths = PathUtils.getDepth(getPrincipalRoot(testPrincipalName).getPath());
        assertEntry(getEntry(testPrincipalName, testPath, 0), bitsProvider.getBits(JCR_ADD_CHILD_NODES), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 2), bitsProvider.getBits(JCR_MODIFY_ACCESS_CONTROL), false, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 3), bitsProvider.getBits(JCR_READ_ACCESS_CONTROL), true, ++rootDepths);

        /*
        Reorder entries
        Expected result:
        0 - allow - JCR_ADD_CHILD_NODES       - NA
        2 - allow - JCR_READ_ACCESS_CONTROL   - /*
        3 - deny  - JCR_MODIFY_ACCESS_CONTROL - NA
        */
        acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            if (testPrincipal.equals(ace.getPrincipal()) && Arrays.equals(privilegesFromNames(JCR_MODIFY_ACCESS_CONTROL), ace.getPrivileges())) {
                acl.orderBefore(ace, null);
            }
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        rootDepths = PathUtils.getDepth(getPrincipalRoot(testPrincipalName).getPath());
        assertEntry(getEntry(testPrincipalName, testPath, 0), bitsProvider.getBits(JCR_ADD_CHILD_NODES), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 2), bitsProvider.getBits(JCR_READ_ACCESS_CONTROL), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 3), bitsProvider.getBits(JCR_MODIFY_ACCESS_CONTROL), false, ++rootDepths);

        /*
        Remove all entries
        */
        acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            if (testPrincipal.equals(ace.getPrincipal())) {
                acl.removeAccessControlEntry(ace);
            }
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        assertEquals(0, cntEntries(getPrincipalRoot(testPrincipalName)));
    }

    @Test
    public void testReorderForSinglePrincipal2() throws Exception {
        Principal testPrincipal = getTestPrincipal();
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_MODIFY_ACCESS_CONTROL), false);
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_READ_ACCESS_CONTROL), true, Collections.singletonMap(REP_GLOB, getValueFactory().createValue("/*")));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        /*
        Original setup with 3 access control entries for testPrincipal @ testPath
        Expected result:
        0 - testuser - allow - JCR_ADD_CHILD_NODES       - NA
        1 - everyone - allow - READ                      - NA
        2 - testuser - deny  - JCR_MODIFY_ACCESS_CONTROL - NA
        3 - testuser - allow - JCR_READ_ACCESS_CONTROL   - /*
        */
        long rootDepths = PathUtils.getDepth(getPrincipalRoot(testPrincipalName).getPath());
        assertEntry(getEntry(testPrincipalName, testPath, 0), bitsProvider.getBits(JCR_ADD_CHILD_NODES), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 2), bitsProvider.getBits(JCR_MODIFY_ACCESS_CONTROL), false, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 3), bitsProvider.getBits(JCR_READ_ACCESS_CONTROL), true, ++rootDepths);

        /*
        Reorder entries
        Expected result:
        0 - allow - JCR_READ_ACCESS_CONTROL   - /*
        1 - allow - JCR_ADD_CHILD_NODES       - NA
        3 - deny  - JCR_MODIFY_ACCESS_CONTROL - NA
        */
        acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        AccessControlEntry first = null;
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            if (testPrincipal.equals(ace.getPrincipal())) {
                if (first == null) {
                    first = ace;
                }
                if (Arrays.equals(privilegesFromNames(JCR_READ_ACCESS_CONTROL), ace.getPrivileges())) {
                    acl.orderBefore(ace, first);
                }
            }
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        rootDepths = PathUtils.getDepth(getPrincipalRoot(testPrincipalName).getPath());
        assertEntry(getEntry(testPrincipalName, testPath, 0), bitsProvider.getBits(JCR_READ_ACCESS_CONTROL), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 1), bitsProvider.getBits(JCR_ADD_CHILD_NODES), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 3), bitsProvider.getBits(JCR_MODIFY_ACCESS_CONTROL), false, ++rootDepths);

        /*
        Remove all entries
        */
        acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            if (testPrincipal.equals(ace.getPrincipal())) {
                acl.removeAccessControlEntry(ace);
            }
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        assertEquals(0, cntEntries(getPrincipalRoot(testPrincipalName)));
    }

    private static void assertEntry(Tree entry, PrivilegeBits expectedBits, boolean isAllow, long expectedDepth) {
        assertEquals(expectedBits, PrivilegeBits.getInstance(entry.getProperty(REP_PRIVILEGE_BITS)));
        assertEquals(isAllow, entry.getProperty(REP_IS_ALLOW).getValue(Type.BOOLEAN));
        assertEquals(expectedDepth, PathUtils.getDepth(entry.getPath()));
    }
}