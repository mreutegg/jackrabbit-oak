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
package org.apache.jackrabbit.oak.spi.security.privilege;

import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

/**
 * PrivilegeProvider... TODO
 */
public interface PrivilegeProvider {

    /**
     * Returns all privilege definitions accessible to a given
     * {@link org.apache.jackrabbit.oak.api.ContentSession}.
     *
     * @return all privilege definitions.
     */
    @Nonnull
    PrivilegeDefinition[] getPrivilegeDefinitions();

    /**
     * Returns the privilege definition with the specified internal name.
     *
     * @param name The internal name of the privilege definition to be
     * retrieved.
     * @return The privilege definition with the given name or {@code null} if
     * no such definition exists.
     */
    @Nullable
    PrivilegeDefinition getPrivilegeDefinition(String name);

    PrivilegeDefinition registerDefinition(String privilegeName, boolean isAbstract, Set<String> declaredAggregateNames) throws RepositoryException;
}