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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

/**
 * <h3>TreeTypeProvider</h3>
 * Allows to distinguish different types of trees based on their name, ancestry
 * or primary type. Currently the following types are supported:
 *
 * <ul>
 *     <li>{@link #TYPE_HIDDEN}: a hidden tree whose name starts with ":".
 *     Please note that the whole subtree of a hidden node is considered hidden.</li>
 *     <li>{@link #TYPE_AC}: A tree that stores access control content
 *     and requires special access {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions#READ_ACCESS_CONTROL permissions}.</li>
 *     <li>{@link #TYPE_VERSION}: if a given tree is located within
 *     any of the version related stores defined by JSR 283. Depending on the
 *     permission evaluation implementation those items require special treatment.</li>
 *     <li>{@link #TYPE_INTERNAL}: repository internal content that is not hidden (e.g. permission store)</li>
 *     <li>{@link #TYPE_DEFAULT}: the default type for trees that don't
 *     match any of the upper types.</li>
 * </ul>
 */
public final class TreeTypeProvider {

    // regular trees
    public static final int TYPE_DEFAULT = 1;
    // version store(s) content
    public static final int TYPE_VERSION = 2;
    // repository internal content such as e.g. permissions store
    public static final int TYPE_INTERNAL = 4;
    // access control content
    public static final int TYPE_AC = 8;
    // hidden trees
    public static final int TYPE_HIDDEN = 16;

    private final Context authorizationContext;

    public TreeTypeProvider(@Nonnull Context authorizationContext) {
        this.authorizationContext = authorizationContext;
    }

    public int getType(@Nonnull Tree tree) {
        if (tree.isRoot()) {
            return TYPE_DEFAULT;
        } else {
            Tree t = tree;
            while (!t.isRoot()) {
                int type = getType(t.getName(), t);
                // stop walking up the hierarchy as soon as a special type is found
                if (TYPE_DEFAULT != type) {
                    return type;
                }
                t = t.getParent();
            }
            return TYPE_DEFAULT;
        }
    }

    public int getType(@Nonnull Tree tree, int parentType) {
        if (tree.isRoot()) {
            return TYPE_DEFAULT;
        }

        int type;
        switch (parentType) {
            case TYPE_HIDDEN:
                type = TYPE_HIDDEN;
                break;
            case TYPE_VERSION:
                type = TYPE_VERSION;
                break;
            case TYPE_INTERNAL:
                type = TYPE_INTERNAL;
                break;
            case TYPE_AC:
                type = TYPE_AC;
                break;
            default:
                type = getType(tree.getName(), tree);
        }
        return type;
    }

    private int getType(@Nonnull String name, @Nonnull Tree tree) {
        int type;
        if (NodeStateUtils.isHidden(name)) {
            type = TYPE_HIDDEN;
        } else if (VersionConstants.VERSION_STORE_ROOT_NAMES.contains(name)) {
            type = (JcrConstants.JCR_SYSTEM.equals(tree.getParent().getName())) ?  TYPE_VERSION : TYPE_DEFAULT;
        } else if (PermissionConstants.REP_PERMISSION_STORE.equals(name)) {
            type = TYPE_INTERNAL;
        } else if (authorizationContext.definesContextRoot(tree)) {
            type = TYPE_AC;
        } else {
            type = TYPE_DEFAULT;
        }
        return type;
    }
}
