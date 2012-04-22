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

package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Branch;
import org.apache.jackrabbit.oak.api.TransientNodeState;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.version.VersionManager;

public interface SessionContext<T extends Session> {
    T getSession();
    GlobalContext getGlobalContext();
    String getWorkspaceName();
    ContentSession getContentSession();
    ValueFactory getValueFactory();
    LockManager getLockManager() throws RepositoryException;
    NodeTypeManager getNodeTypeManager() throws RepositoryException;
    VersionManager getVersionManager() throws RepositoryException;
    Branch getBranch();
    TransientNodeState getState(NodeImpl node);
}
