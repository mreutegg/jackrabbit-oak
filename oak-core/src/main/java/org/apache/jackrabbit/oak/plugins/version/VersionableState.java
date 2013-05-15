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
package org.apache.jackrabbit.oak.plugins.version;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.OnParentVersionAction;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.jcr.version.OnParentVersionAction.ABORT;
import static javax.jcr.version.OnParentVersionAction.COMPUTE;
import static javax.jcr.version.OnParentVersionAction.COPY;
import static javax.jcr.version.OnParentVersionAction.IGNORE;
import static javax.jcr.version.OnParentVersionAction.INITIALIZE;
import static javax.jcr.version.OnParentVersionAction.VERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENMIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENUUID;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PREDECESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.NT_VERSIONEDCHILD;
import static org.apache.jackrabbit.oak.plugins.version.Utils.primaryTypeOf;
import static org.apache.jackrabbit.oak.plugins.version.Utils.uuidFromNode;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.util.TODO;

/**
 * <code>VersionableState</code> provides methods to create a versionable state
 * for a version based on a versionable node.
 * <p>
 * The restore implementation of this class does not handle the removeExisting
 * flag. It is expected that this is handled on a higher level. If this is not
 * done the uniqueness constraint on the jcr:uuid will kick in and fail the
 * commit.
 * </p>
 */
class VersionableState {

    private static final String JCR_CHILDVERSIONHISTORY = "jcr:childVersionHistory";
    private static final Set<String> BASIC_PROPERTIES = new HashSet<String>();
    private static final Set<String> BASIC_FROZEN_PROPERTIES = new HashSet<String>();

    static {
        BASIC_PROPERTIES.add(JCR_PRIMARYTYPE);
        BASIC_PROPERTIES.add(JCR_UUID);
        BASIC_PROPERTIES.add(JCR_MIXINTYPES);
        BASIC_FROZEN_PROPERTIES.add(JCR_FROZENPRIMARYTYPE);
        BASIC_FROZEN_PROPERTIES.add(JCR_FROZENUUID);
        BASIC_FROZEN_PROPERTIES.add(JCR_FROZENMIXINTYPES);
    }

    private final NodeBuilder version;
    private final NodeBuilder frozenNode;
    private final NodeBuilder versionable;
    private final ReadOnlyNodeTypeManager ntMgr;

    private VersionableState(@Nonnull NodeBuilder version,
                             @Nonnull NodeBuilder versionable,
                             @Nonnull ReadOnlyNodeTypeManager ntMgr) {
        this.version = checkNotNull(version);
        this.frozenNode = version.child(JCR_FROZENNODE);
        this.versionable = checkNotNull(versionable);
        this.ntMgr = checkNotNull(ntMgr);
    }

    /**
     * Creates a frozen node under the version and initializes it with the basic
     * frozen properties (jcr:frozenPrimaryType, jcr:frozenMixinTypes and
     * jcr:frozenUuid) from the given versionable node.
     *
     * @param version the parent node of the frozen node.
     * @param versionable the versionable node.
     * @param ntMgr the node type manager.
     * @return a versionable state
     */
    @Nonnull
    static VersionableState fromVersion(@Nonnull NodeBuilder version,
                                        @Nonnull NodeBuilder versionable,
                                        @Nonnull ReadOnlyNodeTypeManager ntMgr) {
        return new VersionableState(version, versionable, ntMgr).init();
    }

    /**
     * Creates a versionable state for a restore.
     *
     * @param version the version to restore.
     * @param versionable the versionable node.
     * @param ntMgr the node type manager.
     * @return a versionable state.
     */
    static VersionableState forRestore(@Nonnull NodeBuilder version,
                                       @Nonnull NodeBuilder versionable,
                                       @Nonnull ReadOnlyNodeTypeManager ntMgr) {
        return new VersionableState(version, versionable, ntMgr);
    }

    /**
     * Creates a frozen node under the version and initializes it with the basic
     * frozen properties (jcr:frozenPrimaryType, jcr:frozenMixinTypes and
     * jcr:frozenUuid) from the given versionable node.
     *
     * @return this versionable state.
     */
    private VersionableState init() {
        // initialize jcr:frozenNode
        frozenNode.setProperty(JCR_UUID, IdentifierManager.generateUUID(), Type.STRING);
        frozenNode.setProperty(JCR_PRIMARYTYPE, NT_FROZENNODE, Type.NAME);
        Iterable<String> mixinTypes;
        if (versionable.hasProperty(JCR_MIXINTYPES)) {
            mixinTypes = versionable.getNames(JCR_MIXINTYPES);
        } else {
            mixinTypes = Collections.emptyList();
        }
        frozenNode.setProperty(JCR_FROZENMIXINTYPES, mixinTypes, Type.NAMES);
        frozenNode.setProperty(JCR_FROZENPRIMARYTYPE, primaryTypeOf(versionable), Type.NAME);
        frozenNode.setProperty(JCR_FROZENUUID, uuidFromNode(versionable), Type.STRING);
        return this;
    }

    /**
     * Creates the versionable state under the version.
     *
     * @return the frozen node.
     * @throws CommitFailedException if the operation fails. E.g. because the
     *              versionable node has a property with OPV ABORT.
     */
    NodeBuilder create() throws CommitFailedException {
        try {
            createState(versionable, frozenNode);
            return frozenNode;
        } catch (RepositoryException e) {
            throw new CommitFailedException(CommitFailedException.VERSION,
                    VersionExceptionCode.UNEXPECTED_REPOSITORY_EXCEPTION.ordinal(),
                    "Unexpected RepositoryException", e);
        }
    }

    /**
     * Restore the versionable node to the given version.
     *
     * @return the versionable node.
     * @throws CommitFailedException if the operation fails.
     */
    public NodeBuilder restore() throws CommitFailedException {
        try {
            long created = version.getProperty(JCR_CREATED).getValue(Type.DATE);
            VersionSelector selector = new DateVersionSelector(created);
            restoreFrozen(frozenNode, versionable, selector);
            restoreVersionable(versionable, version);
            return versionable;
        } catch (RepositoryException e) {
            throw new CommitFailedException(CommitFailedException.VERSION,
                    VersionExceptionCode.UNEXPECTED_REPOSITORY_EXCEPTION.ordinal(),
                    "Unexpected RepositoryException", e);
        }
    }

    //--------------------------< internal >------------------------------------

    private void restoreState(@Nonnull NodeBuilder src,
                              @Nonnull NodeBuilder dest,
                              @Nonnull VersionSelector selector)
            throws RepositoryException, CommitFailedException {
        String primaryType = primaryTypeOf(src);
        if (primaryType.equals(NT_FROZENNODE)) {
            restoreFrozen(src, dest, selector);
        } else if (primaryType.equals(NT_VERSIONEDCHILD)) {
            restoreVersionedChild(src, dest, selector);
        } else {
            restoreNode(src, dest, selector);
        }
    }

    /**
     * Restore a nt:frozenNode.
     */
    private void restoreFrozen(NodeBuilder frozen,
                               NodeBuilder dest,
                               VersionSelector selector)
            throws RepositoryException, CommitFailedException {
        // 15.7.2 Restoring Type and Identifier
        dest.setProperty(JCR_PRIMARYTYPE,
                frozen.getName(JCR_FROZENPRIMARYTYPE), Type.NAME);
        dest.setProperty(JCR_UUID,
                frozen.getProperty(JCR_FROZENUUID).getValue(Type.STRING),
                Type.STRING);
        if (frozen.hasProperty(JCR_FROZENMIXINTYPES)) {
            dest.setProperty(JCR_MIXINTYPES,
                    frozen.getNames(JCR_FROZENMIXINTYPES), Type.NAMES);
        }
        // 15.7.3 Restoring Properties
        for (PropertyState p : frozen.getProperties()) {
            if (BASIC_FROZEN_PROPERTIES.contains(p.getName())) {
                // ignore basic frozen properties we restored earlier
                continue;
            }
            int action = getOPV(dest, p);
            if (action == COPY || action == VERSION) {
                dest.setProperty(p);
            }
        }
        for (PropertyState p : dest.getProperties()) {
            if (BASIC_PROPERTIES.contains(p.getName())) {
                continue;
            }
            if (frozen.hasProperty(p.getName())) {
                continue;
            }
            int action = getOPV(dest, p);
            if (action == COPY || action == VERSION || action == ABORT) {
                dest.removeProperty(p.getName());
            } else if (action == IGNORE) {
                // no action
            } else if (action == INITIALIZE) {
                resetToDefaultValue(dest, p);
            } else if (action == COMPUTE) {
                // only COMPUTE property definitions currently are
                // jcr:primaryType and jcr:mixinTypes
                // do nothing for now
            }
        }
        restoreChildren(frozen, dest, selector);
    }

    /**
     * Restore a copied node.
     */
    private void restoreNode(NodeBuilder src,
                             NodeBuilder dest,
                             VersionSelector selector)
            throws RepositoryException, CommitFailedException {
        copyProperties(src, dest, OPVForceCopy.INSTANCE, false);
        restoreChildren(src, dest, selector);
    }

    /**
     * Restore an nt:versionedChild node.
     */
    private void restoreVersionedChild(NodeBuilder versionedChild,
                                       NodeBuilder dest,
                                       VersionSelector selector)
            throws RepositoryException {
        // 15.7.5 Chained Versions on Restore
        TODO.unimplemented().doNothing();
        // ...
        // restoreVersionable(dest, selector.select(history));
    }

    /**
     * Restores children of a <code>src</code>.
     */
    private void restoreChildren(NodeBuilder src,
                                 NodeBuilder dest,
                                 VersionSelector selector)
            throws RepositoryException, CommitFailedException {
        // 15.7.6 Restoring Child Nodes
        for (String name : src.getChildNodeNames()) {
            NodeBuilder srcChild = src.getChildNode(name);
            int action = getOPV(dest, srcChild, name);
            if (action == COPY || action == VERSION) {
                // replace on destination
                dest.removeChildNode(name);
                restoreState(srcChild, dest.child(name), selector);
            }
        }
        for (String name : dest.getChildNodeNames()) {
            if (src.hasChildNode(name)) {
                continue;
            }
            NodeBuilder destChild = dest.getChildNode(name);
            int action = getOPV(dest, destChild, name);
            if (action == COPY || action == VERSION || action == ABORT) {
                dest.removeChildNode(name);
            } else if (action == IGNORE) {
                // no action
            } else if (action == INITIALIZE) {
                TODO.unimplemented().doNothing();
            } else if (action == COMPUTE) {
                // there are currently no child node definitions
                // with OPV compute
            }
        }
    }

    /**
     * 15.7.7 Simple vs. Full Versioning after Restore
     */
    private void restoreVersionable(@Nonnull NodeBuilder versionable,
                                    @Nonnull NodeBuilder version) {
        checkNotNull(versionable).setProperty(JCR_ISCHECKEDOUT,
                false, Type.BOOLEAN);
        versionable.setProperty(JCR_BASEVERSION,
                uuidFromNode(version), Type.REFERENCE);
        versionable.setProperty(JCR_PREDECESSORS,
                Collections.<String>emptyList(), Type.REFERENCES);
    }

    private void resetToDefaultValue(NodeBuilder dest, PropertyState p)
            throws RepositoryException {
        ReadOnlyTree tree = new ReadOnlyTree(dest.getNodeState());
        PropertyDefinition def = ntMgr.getDefinition(tree, p, true);
        Value[] values = def.getDefaultValues();
        if (values != null) {
            if (p.isArray()) {
                p = PropertyStates.createProperty(p.getName(), values);
                dest.setProperty(p);
            } else if (values.length > 0) {
                p = PropertyStates.createProperty(p.getName(), values[0]);
                dest.setProperty(p);
            }
        }
    }

    private void createState(NodeBuilder src,
                             NodeBuilder dest)
            throws CommitFailedException, RepositoryException {
        copyProperties(src, dest, new OPVProvider() {
            @Override
            public int getAction(NodeBuilder src,
                                 NodeBuilder dest,
                                 PropertyState prop)
                    throws RepositoryException {
                return getOPV(src, prop);
            }
        }, true);

        // add the frozen children and histories
        for (String name : src.getChildNodeNames()) {
            NodeBuilder child = src.getChildNode(name);
            int opv = getOPV(src, child, name);

            if (opv == OnParentVersionAction.ABORT) {
                throw new CommitFailedException(CommitFailedException.VERSION,
                        VersionExceptionCode.OPV_ABORT_ITEM_PRESENT.ordinal(),
                        "Checkin aborted due to OPV abort in " + name);
            }
            if (opv == OnParentVersionAction.VERSION) {
                if (ntMgr.isNodeType(new ReadOnlyTree(child.getNodeState()), MIX_VERSIONABLE)) {
                    // create frozen versionable child
                    versionedChild(child, dest.child(name));
                } else {
                    // else copy
                    copy(child, dest.child(name));
                }
            } else if (opv == COPY) {
                copy(child, dest.child(name));
            }
        }
    }

    private void versionedChild(NodeBuilder src, NodeBuilder dest) {
        String ref = src.getProperty(JCR_VERSIONHISTORY).getValue(Type.REFERENCE);
        dest.setProperty(JCR_PRIMARYTYPE, NT_VERSIONEDCHILD, Type.NAME);
        dest.setProperty(JCR_CHILDVERSIONHISTORY, ref, Type.REFERENCE);
    }

    private void copy(NodeBuilder src,
                      NodeBuilder dest)
            throws RepositoryException, CommitFailedException {
        copyProperties(src, dest, OPVForceCopy.INSTANCE, false);
        for (String name : src.getChildNodeNames()) {
            NodeBuilder child = src.getChildNode(name);
            copy(child, dest.child(name));
        }
    }

    private void copyProperties(NodeBuilder src,
                                NodeBuilder dest,
                                OPVProvider opvProvider,
                                boolean ignoreTypeAndUUID)
            throws RepositoryException, CommitFailedException {
        // add the properties
        for (PropertyState prop : src.getProperties()) {
            int opv = opvProvider.getAction(src, dest, prop);

            String propName = prop.getName();
            if (opv == OnParentVersionAction.ABORT) {
                throw new CommitFailedException(CommitFailedException.VERSION,
                        VersionExceptionCode.OPV_ABORT_ITEM_PRESENT.ordinal(),
                        "Checkin aborted due to OPV abort in " + propName);
            }
            if (ignoreTypeAndUUID && BASIC_PROPERTIES.contains(propName)) {
                continue;
            }
            if (opv == OnParentVersionAction.VERSION
                    || opv == COPY) {
                dest.setProperty(prop);
            }
        }
    }

    private int getOPV(NodeBuilder parent, NodeBuilder child, String childName)
            throws RepositoryException {
        ReadOnlyTree parentTree = new ReadOnlyTree(parent.getNodeState());
        ReadOnlyTree childTree = new ReadOnlyTree(
                parentTree, childName, child.getNodeState());
        return ntMgr.getDefinition(parentTree, childTree).getOnParentVersion();
    }

    private int getOPV(NodeBuilder node, PropertyState property)
            throws RepositoryException {
        if (property.getName().charAt(0) == ':') {
            // FIXME: handle child order properly
            return OnParentVersionAction.COPY;
        } else {
            return ntMgr.getDefinition(new ReadOnlyTree(node.getNodeState()),
                    property, false).getOnParentVersion();
        }
    }

    private interface OPVProvider {

        public int getAction(NodeBuilder src,
                             NodeBuilder dest,
                             PropertyState prop)
                throws RepositoryException;
    }

    private static final class OPVForceCopy implements OPVProvider {

        private static final OPVProvider INSTANCE = new OPVForceCopy();

        @Override
        public int getAction(NodeBuilder src,
                             NodeBuilder dest,
                             PropertyState prop) throws RepositoryException {
            return COPY;
        }
    }
}
