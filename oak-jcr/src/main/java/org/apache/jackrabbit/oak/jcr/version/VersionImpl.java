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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.apache.jackrabbit.oak.jcr.SessionContext;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.PropertyDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionManagerDelegate;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.util.TODO;

import static com.google.common.base.Preconditions.checkNotNull;

class VersionImpl extends NodeImpl<VersionDelegate> implements Version {

    public VersionImpl(VersionDelegate dlg, SessionContext sessionContext) {
        super(dlg, sessionContext);
    }

    @Override
    public VersionHistory getContainingHistory() throws RepositoryException {
        return new VersionHistoryImpl(
                getVersionManagerDelegate().getVersionHistory(dlg.getParent()), sessionContext);
    }

    private ValueFactoryImpl getValueFactory() {
        return sessionContext.getValueFactory();
    }

    @Override
    public Calendar getCreated() throws RepositoryException {
        PropertyDelegate dlg = getPropertyOrThrow(JcrConstants.JCR_CREATED);
        return getValueFactory().createValue(dlg.getSingleState()).getDate();
    }

    @Override
    public Version getLinearPredecessor() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public Version getLinearSuccessor() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    private List<Value> getValues(PropertyDelegate p) throws InvalidItemStateException, ValueFormatException {
        return getValueFactory().createValues(p.getMultiState());
    }

    @Override
    public Version[] getPredecessors() throws RepositoryException {
        PropertyDelegate p = getPropertyOrThrow(VersionConstants.JCR_PREDECESSORS);
        List<Version> predecessors = new ArrayList<Version>();
        VersionManagerDelegate vMgr = getVersionManagerDelegate();
        for (Value v : getValues(p)) {
            String id = v.getString();
            predecessors.add(new VersionImpl(vMgr.getVersionByIdentifier(id), sessionContext));
        }
        return predecessors.toArray(new Version[predecessors.size()]);
    }

    @Override
    public Version[] getSuccessors() throws RepositoryException {
        PropertyDelegate p = getPropertyOrThrow(VersionConstants.JCR_SUCCESSORS);
        List<Version> successors = new ArrayList<Version>();
        VersionManagerDelegate vMgr = getVersionManagerDelegate();
        for (Value v : getValues(p)) {
            String id = v.getString();
            successors.add(new VersionImpl(vMgr.getVersionByIdentifier(id), sessionContext));
        }
        return successors.toArray(new Version[successors.size()]);
    }

    @Override
    public Node getFrozenNode() throws RepositoryException {
        return new NodeImpl<NodeDelegate>(
                dlg.getChild(VersionConstants.JCR_FROZENNODE), sessionContext);
    }

    //------------------------------< internal >--------------------------------

    @Nonnull
    private VersionManagerDelegate getVersionManagerDelegate() {
        return VersionManagerDelegate.create(sessionContext.getSessionDelegate());
    }

    @Nonnull
    private PropertyDelegate getPropertyOrThrow(@Nonnull String name)
            throws RepositoryException {
        PropertyDelegate p = dlg.getProperty(checkNotNull(name));
        if (p == null) {
            throw new RepositoryException("Inconsistent version storage. " +
                    "Version does not have a " + name + " property.");
        }
        return p;
    }
}
