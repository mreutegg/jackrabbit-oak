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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.AbstractTree;
import org.apache.jackrabbit.oak.plugins.lock.LockConstants;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.util.ChildOrderDiff;
import org.apache.jackrabbit.oak.util.TreeUtil;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.CommitFailedException.ACCESS;

/**
 * Validator implementation that checks for sufficient permission for all
 * write operations executed by a given content session.
 */
class PermissionValidator extends DefaultValidator {

    private final Tree parentBefore;
    private final Tree parentAfter;
    private final TreePermission parentPermission;
    private final PermissionProvider permissionProvider;
    private final PermissionValidatorProvider provider;

    private final long permission;

    PermissionValidator(@Nonnull Tree rootTreeBefore, @Nonnull Tree rootTreeAfter,
                        @Nonnull PermissionProvider permissionProvider,
                        @Nonnull PermissionValidatorProvider provider) {
        this.parentBefore = rootTreeBefore;
        this.parentAfter = rootTreeAfter;
        this.parentPermission = permissionProvider.getTreePermission(parentBefore, TreePermission.EMPTY);

        this.permissionProvider = permissionProvider;
        this.provider = provider;

        permission = Permissions.getPermission(PermissionUtil.getPath(parentBefore, parentAfter), Permissions.NO_PERMISSION);
    }

    PermissionValidator(@Nullable Tree parentBefore, @Nullable Tree parentAfter,
                        @Nullable TreePermission parentPermission,
                        @Nonnull PermissionValidator parentValidator) {
        this.parentBefore = parentBefore;
        this.parentAfter = parentAfter;
        this.parentPermission = parentPermission;

        permissionProvider = parentValidator.permissionProvider;
        provider = parentValidator.provider;

        if (Permissions.NO_PERMISSION == parentValidator.permission) {
            this.permission = Permissions.getPermission(PermissionUtil.getPath(parentBefore, parentAfter), Permissions.NO_PERMISSION);
        } else {
            this.permission = parentValidator.permission;
        }
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        String name = after.getName();
        if (!AbstractTree.OAK_CHILD_ORDER.equals(name)) {
            checkPermissions(parentAfter, after, Permissions.ADD_PROPERTY);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        String name = after.getName();
        if (AbstractTree.OAK_CHILD_ORDER.equals(name)) {
            String childName = ChildOrderDiff.firstReordered(before, after);
            if (childName != null) {
                Tree child = parentAfter.getChild(childName);
                checkPermissions(parentAfter, false, Permissions.MODIFY_CHILD_NODE_COLLECTION);
            } // else: no re-order but only internal update
        } else if (isImmutableProperty(name)) {
            // parent node has been removed and and re-added as
            checkPermissions(parentAfter, false, Permissions.ADD_NODE|Permissions.REMOVE_NODE);
        } else {
            checkPermissions(parentAfter, after, Permissions.MODIFY_PROPERTY);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        if (!AbstractTree.OAK_CHILD_ORDER.equals(before.getName())) {
            checkPermissions(parentBefore, before, Permissions.REMOVE_PROPERTY);
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        Tree child = checkNotNull(parentAfter.getChild(name));
        if (isVersionstorageTree(child)) {
            child = getVersionHistoryTree(child);
            if (child == null) {
                throw new CommitFailedException(
                        ACCESS, 21, "New version storage node without version history: cannot verify permissions.");
            }
        }
        return checkPermissions(child, false, Permissions.ADD_NODE);
    }


    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        Tree childBefore = parentBefore.getChild(name);
        Tree childAfter = parentAfter.getChild(name);
        return nextValidator(childBefore, childAfter, permissionProvider.getTreePermission(childBefore, parentPermission));
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        Tree child = checkNotNull(parentBefore.getChild(name));
        if (isVersionstorageTree(child)) {
            throw new CommitFailedException(
                    ACCESS, 22, "Attempt to remove versionstorage node: Fail to verify delete permission.");
        }
        return checkPermissions(child, true, Permissions.REMOVE_NODE);
    }

    //-------------------------------------------------< internal / private >---
    @Nonnull
    PermissionValidator createValidator(@Nullable Tree parentBefore, @Nullable Tree parentAfter,
                                        @Nullable TreePermission parentPermission,
                                        @Nonnull PermissionValidator parentValidator) {
        return new PermissionValidator(parentBefore, parentAfter, parentPermission, parentValidator);
    }

    @CheckForNull
    Tree getParentAfter() {
        return parentAfter;
    }

    @CheckForNull
    Tree getParentBefore() {
        return parentBefore;
    }

    @Nonnull
    TreePermission getParentPermission() {
        return parentPermission;
    }

    Validator checkPermissions(@Nonnull Tree tree, boolean isBefore,
                               long defaultPermission) throws CommitFailedException {
        long toTest = getPermission(tree, defaultPermission);
        if (Permissions.isRepositoryPermission(toTest)) {
            if (!permissionProvider.getRepositoryPermission().isGranted(toTest)) {
                throw new CommitFailedException(ACCESS, 0, "Access denied");
            }
            return null; // no need for further validation down the subtree
        } else {
            TreePermission tp = permissionProvider.getTreePermission(tree, parentPermission);
            if (!tp.isGranted(toTest)) {
                throw new CommitFailedException(ACCESS, 0, "Access denied");
            }
            if (noTraverse(toTest, defaultPermission)) {
                return null;
            } else {
                return (isBefore) ?
                        nextValidator(tree, null, tp) :
                        nextValidator(null, tree, tp);
            }
        }
    }

    private void checkPermissions(@Nonnull Tree parent, @Nonnull PropertyState property,
                                  long defaultPermission) throws CommitFailedException {
        if (NodeStateUtils.isHidden(property.getName())) {
            // ignore any hidden properties (except for OAK_CHILD_ORDER which has
            // been covered in "propertyChanged"
            return;
        }
        long toTest = getPermission(parent, property, defaultPermission);
        boolean isGranted;
        if (Permissions.isRepositoryPermission(toTest)) {
            isGranted = permissionProvider.getRepositoryPermission().isGranted(toTest);
        } else {
            isGranted = parentPermission.isGranted(toTest, property);
        }

        if (!isGranted) {
            throw new CommitFailedException(ACCESS, 0, "Access denied");
        }
    }

    private Validator nextValidator(@Nullable Tree parentBefore, @Nullable Tree parentAfter, @Nonnull TreePermission treePermission) {
        Validator validator = createValidator(parentBefore, parentAfter, treePermission, this);
        return new VisibleValidator(validator, true, false);
    }

    private long getPermission(@Nonnull Tree tree, long defaultPermission) {
        if (permission != Permissions.NO_PERMISSION) {
            return permission;
        }
        long perm;
        if (provider.getAccessControlContext().definesTree(tree)) {
            perm = Permissions.MODIFY_ACCESS_CONTROL;
        } else if (provider.getUserContext().definesTree(tree)
                && !provider.requiresJr2Permissions(Permissions.USER_MANAGEMENT)) {
            perm = Permissions.USER_MANAGEMENT;
        } else {
            // FIXME: OAK-710 (identify renaming/move of nodes that only required MODIFY_CHILD_NODE_COLLECTION permission)
            perm = defaultPermission;
        }
        return perm;
    }

    private long getPermission(@Nonnull Tree parent, @Nonnull PropertyState propertyState, long defaultPermission) {
        if (permission != Permissions.NO_PERMISSION) {
            return permission;
        }
        String name = propertyState.getName();
        long perm;
        if (JcrConstants.JCR_PRIMARYTYPE.equals(name)) {
            if (defaultPermission == Permissions.MODIFY_PROPERTY) {
                perm = Permissions.NODE_TYPE_MANAGEMENT;
            } else {
                // can't determine if this was  a user supplied modification of
                // the primary type -> omit permission check.
                // Node#addNode(String, String) and related methods need to
                // perform the permission check (as it used to be in jackrabbit 2.x).
                perm = Permissions.NO_PERMISSION;
            }
        } else if (JcrConstants.JCR_MIXINTYPES.equals(name)) {
            perm = Permissions.NODE_TYPE_MANAGEMENT;
        } else if (JcrConstants.JCR_UUID.equals(name)) {
            if (isNodeType(parent, JcrConstants.MIX_REFERENCEABLE)) {
                // property added or removed: jcr:uuid is autocreated in
                // JCR, thus can't determine here if this was a user supplied
                // modification or not.
                perm = Permissions.NO_PERMISSION;
            } else {
                /* the parent is not referenceable -> check regular permissions
                   as this instance of jcr:uuid is not the mandatory/protected
                   property defined by mix:referenceable */
                perm = defaultPermission;
            }
        } else if (LockConstants.LOCK_PROPERTY_NAMES.contains(name)) {
            perm = Permissions.LOCK_MANAGEMENT;
        } else if (VersionConstants.VERSION_PROPERTY_NAMES.contains(name)) {
            perm = Permissions.VERSION_MANAGEMENT;
        } else if (provider.getAccessControlContext().definesProperty(parent, propertyState)) {
            perm = Permissions.MODIFY_ACCESS_CONTROL;
        } else if (provider.getUserContext().definesProperty(parent, propertyState)
                 && !provider.requiresJr2Permissions(Permissions.USER_MANAGEMENT)) {
            perm = Permissions.USER_MANAGEMENT;
        } else {
            perm = defaultPermission;
        }
        return perm;
    }

    private boolean noTraverse(long permission, long defaultPermission) {
        if (defaultPermission == Permissions.REMOVE_NODE && provider.requiresJr2Permissions(Permissions.REMOVE_NODE)) {
            return false;
        } else {
            return permission == Permissions.MODIFY_ACCESS_CONTROL ||
                    permission == Permissions.VERSION_MANAGEMENT ||
                    permission == Permissions.REMOVE_NODE ||
                    defaultPermission == Permissions.REMOVE_NODE;
        }
    }

    private boolean isImmutableProperty(String name) {
        // TODO: review; cant' rely on autocreated/protected definition as this doesn't reveal if a given property is expected to be never modified after creation
        if (JcrConstants.JCR_UUID.equals(name) && isNodeType(parentAfter, JcrConstants.MIX_REFERENCEABLE)) {
            return true;
        } else if (("jcr:created".equals(name) || "jcr:createdBy".equals(name)) && isNodeType(parentAfter, "mix:created")) {
            return true;
        } else {
            return false;
        }
    }

    private boolean isNodeType(Tree parent, String ntName) {
        return provider.getNodeTypeManager().isNodeType(parent, ntName);
    }

    private boolean isVersionstorageTree(Tree tree) {
        return permission == Permissions.VERSION_MANAGEMENT &&
                VersionConstants.REP_VERSIONSTORAGE.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    private Tree getVersionHistoryTree(Tree versionstorageTree) throws CommitFailedException {
        Tree versionHistory = null;
        for (Tree child : versionstorageTree.getChildren()) {
            if (VersionConstants.NT_VERSIONHISTORY.equals(TreeUtil.getPrimaryTypeName(child))) {
                versionHistory = child;
            } else if (isVersionstorageTree(child)) {
                versionHistory = getVersionHistoryTree(child);
            } else {
                throw new CommitFailedException("Misc", 0, "unexpected node");
            }
        }
        return versionHistory;
    }
}
