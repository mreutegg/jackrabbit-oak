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
package org.apache.jackrabbit.oak.plugins.observation;

import static com.google.common.base.Objects.toStringHelper;

import java.util.Arrays;

import javax.annotation.CheckForNull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NoSuchNodeTypeException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;

/**
 * Filter for filtering observation events according to a certain criterion.
 * FIXME rename. EventFilter clashes with the same named interface in JCR 2.1
 */
public class EventFilter {
    private final ReadOnlyNodeTypeManager ntMgr;
    private final int eventTypes;
    private final String path;
    private final boolean deep;
    private final String[] uuids;
    private final String[] nodeTypeOakName;
    private final boolean includeSessionLocal;
    private final boolean includeClusterExternal;

    /**
     * Create a new instance of a filter for a certain criterion
     *
     * @param ntMgr
     * @param eventTypes  event types to include encoded as a bit mask
     * @param path        path to include
     * @param deep        {@code true} if descendants of {@code path} should be included.
     *                    {@code false} otherwise.
     * @param uuids       uuids to include
     * @param nodeTypeName              node type names to include
     * @param includeSessionLocal       include session local events if {@code true}.
     *                                  Exclude otherwise.
     * @param includeClusterExternal    include cluster external events if {@code true}.
     *                                  Exclude otherwise.
     * @throws NoSuchNodeTypeException  if any of the node types in {@code nodeTypeName} does not
     *                                  exist
     * @throws RepositoryException      if an error occurs while reading from the node type manager.
     * @see javax.jcr.observation.ObservationManager#addEventListener(javax.jcr.observation.EventListener, int, String, boolean, String[], String[], boolean) */
    public EventFilter(ReadOnlyNodeTypeManager ntMgr, int eventTypes, String path, boolean deep, String[] uuids,
            String[] nodeTypeName, boolean includeSessionLocal, boolean includeClusterExternal) {
        this.ntMgr = ntMgr;
        this.eventTypes = eventTypes;
        this.path = path;
        this.deep = deep;
        this.uuids = uuids;
        this.nodeTypeOakName = nodeTypeName;
        this.includeSessionLocal = includeSessionLocal;
        this.includeClusterExternal = includeClusterExternal;
    }

    /**
     * Match an event against this filter.
     * @param eventType  type of the event
     * @param associatedParent  associated parent node of the event
     * @return  {@code true} if the filter matches this event. {@code false} otherwise.
     */
    public boolean include(int eventType, @CheckForNull Tree associatedParent) {
        return includeByEvent(eventType)
            && associatedParent != null
            && includeByPath(associatedParent.getPath())
            && includeByType(associatedParent)
            && includeByUuid(associatedParent);
    }

    /**
     * Determine whether session local changes should be included.
     * @param isLocal  {@code true} for session local changes, {@code false} otherwise.
     * @return  {@code true} if the changes are included with this filter. {@code false} otherwise.
     */
    public boolean includeSessionLocal(boolean isLocal) {
        return includeSessionLocal || !isLocal;
    }

    /**
     * Determine whether cluster external changes should be included.
     * @param isExternal  {@code true} for cluster external changes, {@code false} otherwise.
     * @return  {@code true} if the changes are included with this filter. {@code false} otherwise.
     */
    public boolean includeClusterExternal(boolean isExternal) {
        return includeClusterExternal || !isExternal;
    }

    /**
     * Determine whether the children of a {@code path} would be matched by this filter
     * @param path  path whose children to test
     * @return  {@code true} if the children of {@code path} could be matched by this filter
     */
    public boolean includeChildren(String path) {
        return PathUtils.isAncestor(path, this.path) ||
                path.equals((this.path)) ||
                deep && PathUtils.isAncestor(this.path, path);
    }

    /**
     * @return  path of this filter
     */
    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("types", eventTypes)
                .add("path", path)
                .add("deep", deep)
                .add("uuids", Arrays.toString(uuids))
                .add("node types", Arrays.toString(nodeTypeOakName))
                .add("includeSessionLocal", includeSessionLocal)
                .add("includeClusterExternal", includeClusterExternal)
            .toString();
    }

    //-----------------------------< internal >---------------------------------

    private boolean includeByEvent(int eventType) {
        return (this.eventTypes & eventType) != 0;
    }

    private boolean includeByPath(String path) {
        boolean equalPaths = this.path.equals(path);
        if (!deep && !equalPaths) {
            return false;
        }

        if (deep && !(PathUtils.isAncestor(this.path, path) || equalPaths)) {
            return false;
        }
        return true;
    }

    /**
     * Checks whether to include an event based on the type of the associated
     * parent node and the node type filter.
     *
     * @param associatedParent the associated parent node of the event.
     * @return whether to include the event based on the type of the associated
     *         parent node.
     */
    private boolean includeByType(Tree associatedParent) {
        if (nodeTypeOakName == null) {
            return true;
        } else {
            for (String oakName : nodeTypeOakName) {
                if (ntMgr.isNodeType(associatedParent, oakName)) {
                    return true;
                }
            }
            // filter has node types set but none matched
            return false;
        }
    }

    private boolean includeByUuid(Tree associatedParent) {
        if (uuids == null) {
            return true;
        }
        if (uuids.length == 0) {
            return false;
        }

        PropertyState uuidProperty = associatedParent.getProperty(JcrConstants.JCR_UUID);
        if (uuidProperty == null) {
            return false;
        }

        String parentUuid = uuidProperty.getValue(Type.STRING);
        for (String uuid : uuids) {
            if (parentUuid.equals(uuid)) {
                return true;
            }
        }
        return false;
    }

}
