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
package org.apache.jackrabbit.oak.spi.security.authorization;

import java.security.Principal;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * {@code AccessControlContextProvider}...
 */
public interface AccessControlConfiguration extends SecurityConfiguration {

    @Nonnull
    public AccessControlManager getAccessControlManager(Root root, NamePathMapper namePathMapper);

    // TODO define how permissions eval is bound to a particular revision/branch. (passing Tree?)
    @Nonnull
    public CompiledPermissions getCompiledPermissions(NodeStore nodeStore, Set<Principal> principals);
}
