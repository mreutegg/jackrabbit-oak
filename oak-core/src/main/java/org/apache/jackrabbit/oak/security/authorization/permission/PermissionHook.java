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

import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyRoot;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.core.TreeImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;

/**
 * {@code CommitHook} implementation that processes any modification made to
 * access control content and updates persisted permission caches associated
 * with access control related data stored in the repository.
 */
public class PermissionHook implements CommitHook, AccessControlConstants, PermissionConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionHook.class);

    private final RestrictionProvider restrictionProvider;
    private final String workspaceName;

    private NodeBuilder permissionRoot;
    private ReadOnlyNodeTypeManager ntMgr;
    private PrivilegeBitsProvider bitsProvider;

    public PermissionHook(String workspaceName, RestrictionProvider restrictionProvider) {
        this.workspaceName = workspaceName;
        this.restrictionProvider = restrictionProvider;
    }

    @Nonnull
    @Override
    public NodeState processCommit(final NodeState before, NodeState after) throws CommitFailedException {
        NodeBuilder rootAfter = after.builder();

        permissionRoot = getPermissionRoot(rootAfter, workspaceName);
        ntMgr = ReadOnlyNodeTypeManager.getInstance(before);
        bitsProvider = new PrivilegeBitsProvider(new ReadOnlyRoot(before));

        after.compareAgainstBaseState(before, new Diff(new BeforeNode(before), new Node(rootAfter)));
        return rootAfter.getNodeState();
    }

    @Nonnull
    private NodeBuilder getPermissionRoot(NodeBuilder rootBuilder, String workspaceName) {
        NodeBuilder permissionStore = rootBuilder.child(NodeTypeConstants.JCR_SYSTEM).child(REP_PERMISSION_STORE);
        if (permissionStore.getProperty(JCR_PRIMARYTYPE) == null) {
            permissionStore.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
        }
        NodeBuilder permissionRoot;
        if (!permissionStore.hasChildNode(workspaceName)) {
            permissionRoot = permissionStore.child(workspaceName)
                    .setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE);
        } else {
            permissionRoot = permissionStore.child(workspaceName);
        }
        return permissionRoot;
    }

    private static Tree getTree(String name, NodeState nodeState) {
        // FIXME: this readonlytree is not properly connect to it's parent
        return new ReadOnlyTree(null, name, nodeState);
    }

    private static String getAccessControlledPath(BaseNode aclNode) {
        if (REP_REPO_POLICY.equals(aclNode.getName())) {
            return "";
        } else {
            return Text.getRelativeParent(aclNode.getPath(), 1);
        }
    }

    private static int getAceIndex(BaseNode aclNode, String aceName) {
        PropertyState ordering = checkNotNull(aclNode.getNodeState().getProperty(TreeImpl.OAK_CHILD_ORDER));
        return Lists.newArrayList(ordering.getValue(Type.STRINGS)).indexOf(aceName);
    }

    private static String generateName(NodeBuilder principalRoot, PermissionEntry entry) {
        StringBuilder name = new StringBuilder();
        name.append((entry.isAllow) ? PREFIX_ALLOW : PREFIX_DENY).append('-').append(principalRoot.getChildNodeCount());
        return name.toString();
    }

    private Set<Restriction> getRestrictions(String accessControlledPath, Tree aceTree) {
        return restrictionProvider.readRestrictions(Strings.emptyToNull(accessControlledPath), aceTree);
    }

    private class Diff implements NodeStateDiff {

        private final BeforeNode parentBefore;
        private final Node parentAfter;

        private Diff(@Nonnull BeforeNode parentBefore, @Nonnull Node parentAfter) {
            this.parentBefore = parentBefore;
            this.parentAfter = parentAfter;
        }

        @Override
        public void propertyAdded(PropertyState after) {
            // nothing to do
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (isACL(parentAfter) && TreeImpl.OAK_CHILD_ORDER.equals(before.getName())) {
                // TODO: update if order has changed without child-node modifications
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            // nothing to do
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (isACE(name, after)) {
                addEntry(name, after);
            } else {
                BeforeNode before = new BeforeNode(parentBefore.getPath(), name, MemoryNodeState.EMPTY_NODE);
                Node node = new Node(parentAfter, name);
                after.compareAgainstBaseState(before.getNodeState(), new Diff(before, node));
            }
        }

        @Override
        public void childNodeChanged(String name, final NodeState before, NodeState after) {
            if (isACE(name, before) || isACE(name, after)) {
                updateEntry(name, before, after);
            } else if (REP_RESTRICTIONS.equals(name)) {
                updateEntry(parentAfter.getName(), parentBefore.getNodeState(), parentAfter.getNodeState());
            } else {
                BeforeNode nodeBefore = new BeforeNode(parentBefore.getPath(), name, before);
                Node nodeAfter = new Node(parentAfter, name);
                after.compareAgainstBaseState(before, new Diff(nodeBefore, nodeAfter));
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            if (isACE(name, before)) {
                removeEntry(name, before);
            } else {
                BeforeNode nodeBefore = new BeforeNode(parentBefore.getPath(), name, before);
                Node after = new Node(parentAfter.getPath(), name, MemoryNodeState.EMPTY_NODE);
                after.getNodeState().compareAgainstBaseState(before, new Diff(nodeBefore, after));
            }
        }

        //--------------------------------------------------------< private >---
        private boolean isACL(Node parent) {
            return ntMgr.isNodeType(getTree(parent.getName(), parent.getNodeState()), NT_REP_POLICY);
        }

        private boolean isACE(String name, NodeState nodeState) {
            return ntMgr.isNodeType(getTree(name, nodeState), NT_REP_ACE);
        }

        private void addEntry(String name, NodeState ace) {
            PermissionEntry entry = createPermissionEntry(name, ace, parentAfter);
            entry.writeTo(permissionRoot);
        }

        private void removeEntry(String name, NodeState ace) {
            PermissionEntry entry = createPermissionEntry(name, ace, parentBefore);
            String permissionName = getPermissionNodeName(entry);
            if (permissionName != null) {
                permissionRoot.child(entry.principalName).removeNode(permissionName);
            }
        }

        private void updateEntry(String name, NodeState before, NodeState after) {
            removeEntry(name, before);
            addEntry(name, after);
        }

        @CheckForNull
        private String getPermissionNodeName(PermissionEntry permissionEntry) {
            if (permissionRoot.hasChildNode(permissionEntry.principalName)) {
                NodeBuilder principalRoot = permissionRoot.child(permissionEntry.principalName);
                for (String childName : principalRoot.getChildNodeNames()) {
                    NodeState state = principalRoot.child(childName).getNodeState();
                    if (permissionEntry.isSame(childName, state)) {
                        return childName;
                    }
                }
                log.warn("No permission entry for " + permissionEntry);
            } else {
                // inconsistency: removing an ACE that doesn't have a corresponding
                // entry in the permission store.
                log.warn("Missing permission node for principal " + permissionEntry.principalName);
            }
            return null;
        }

        @Nonnull
        private PermissionEntry createPermissionEntry(String name, NodeState ace, BaseNode acl) {
            Tree aceTree = getTree(name, ace);
            String accessControlledPath = getAccessControlledPath(acl);
            String principalName = checkNotNull(TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME));
            PrivilegeBits privilegeBits = bitsProvider.getBits(TreeUtil.getStrings(aceTree, REP_PRIVILEGES));
            boolean isAllow = NT_REP_GRANT_ACE.equals(TreeUtil.getPrimaryTypeName(aceTree));

            return new PermissionEntry(accessControlledPath, getAceIndex(acl, name), principalName,
                    privilegeBits, isAllow, getRestrictions(accessControlledPath, aceTree));
        }
    }

    private static abstract class BaseNode {

        private final String path;

        private BaseNode(String path) {
            this.path = path;
        }

        private BaseNode(String parentPath, String name) {
            this.path = PathUtils.concat(parentPath, new String[]{name});
        }

        String getName() {
            return Text.getName(path);
        }

        String getPath() {
            return path;
        }

        abstract NodeState getNodeState();
    }

    private static final class BeforeNode extends BaseNode {

        private final NodeState nodeState;

        BeforeNode(NodeState root) {
            super("/");
            this.nodeState = root;
        }


        BeforeNode(String parentPath, String name, NodeState nodeState) {
            super(parentPath, name);
            this.nodeState = nodeState;
        }

        @Override
        NodeState getNodeState() {
            return nodeState;
        }
    }

    private static final class Node extends BaseNode {

        private final NodeBuilder builder;

        private Node(NodeBuilder rootBuilder) {
            super("/");
            this.builder = rootBuilder;
        }

        private Node(String parentPath, String name, NodeState state) {
            super(parentPath, name);
            this.builder = state.builder();
        }

        private Node(Node parent, String name) {
            super(parent.getPath(), name);
            this.builder = parent.builder.child(name);
        }

        NodeState getNodeState() {
            return builder.getNodeState();
        }
    }

    private final class PermissionEntry {

        private final String accessControlledPath;
        private final int index;

        private final String principalName;
        private final PrivilegeBits privilegeBits;
        private final boolean isAllow;
        private final Set<Restriction> restrictions;

        private PermissionEntry(@Nonnull String accessControlledPath,
                      int index,
                      @Nonnull String principalName,
                      @Nonnull PrivilegeBits privilegeBits,
                      boolean isAllow, Set<Restriction> restrictions) {
            this.accessControlledPath = accessControlledPath;
            this.index = index;

            this.principalName = Text.escapeIllegalJcrChars(principalName);
            this.privilegeBits = privilegeBits;
            this.isAllow = isAllow;
            this.restrictions = restrictions;
        }

        private void writeTo(NodeBuilder permissionRoot) {
            NodeBuilder principalRoot = permissionRoot.child(principalName);
            String entryName = generateName(principalRoot, this);
            NodeBuilder entry = principalRoot.child(entryName)
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSIONS)
                    .setProperty(REP_ACCESS_CONTROLLED_PATH, accessControlledPath)
                    .setProperty(REP_INDEX, index)
                    .setProperty(privilegeBits.asPropertyState(REP_PRIVILEGE_BITS));
            for (Restriction restriction : restrictions) {
                entry.setProperty(restriction.getProperty());
            }
        }

        private boolean isSame(String name, NodeState node) {
            Tree entry = getTree(name, node);

            if (isAllow == (name.charAt(0) == PREFIX_ALLOW)) {
                return false;
            }
            if (!privilegeBits.equals(PrivilegeBits.getInstance(node.getProperty(REP_PRIVILEGES)))) {
                return false;
            }
            if (!principalName.equals(TreeUtil.getString(entry, REP_PRINCIPAL_NAME))) {
                return false;
            }
            if (index != entry.getProperty(REP_INDEX).getValue(Type.LONG)) {
                return false;
            }
            if (!accessControlledPath.equals(TreeUtil.getString(entry, REP_ACCESS_CONTROLLED_PATH))) {
                return false;
            }
            return restrictions.equals(getRestrictions(accessControlledPath, getTree(name, node)));
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("permission entry: ").append(accessControlledPath);
            sb.append(';').append(principalName);
            sb.append(';').append(isAllow ? "allow" : "deny");
            sb.append(';').append(privilegeBits);
            sb.append(';').append(restrictions);
            return sb.toString();
        }
    }
}