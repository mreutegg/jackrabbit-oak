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
package org.apache.jackrabbit.oak.security.privilege;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinitionReader;
import org.apache.jackrabbit.oak.util.NodeUtil;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants.PRIVILEGES_PATH;
import static org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants.REP_AGGREGATES;
import static org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants.REP_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants.REP_PRIVILEGES;


/**
 * Reads privilege definitions from the repository content without applying
 * any validation.
 */
class PrivilegeDefinitionReaderImpl implements PrivilegeDefinitionReader {

    private final Tree privilegesTree;

    PrivilegeDefinitionReaderImpl(@Nonnull Tree privilegesTree) {
        if (privilegesTree.isRoot()) {
            TreeLocation location = privilegesTree.getLocation().getChild(JcrConstants.JCR_SYSTEM+'/'+REP_PRIVILEGES);
            this.privilegesTree = checkNotNull(location.getTree());
        } else if (PRIVILEGES_PATH.equals(privilegesTree.getPath())) {
            this.privilegesTree = privilegesTree;
        } else {
            throw new IllegalArgumentException("Illegal privilege tree " + privilegesTree);
        }
    }

    PrivilegeDefinitionReaderImpl(@Nonnull Root root) {
        this(checkNotNull(root.getTree(PRIVILEGES_PATH)));
    }

    //------------------------------------------< PrivilegeDefinitionReader >---
    @Override
    public Map<String, PrivilegeDefinition> readDefinitions() {
        Map<String, PrivilegeDefinition> definitions = new HashMap<String, PrivilegeDefinition>();
        if (privilegesTree != null) {
            for (Tree child : privilegesTree.getChildren()) {
                PrivilegeDefinition def = readDefinition(child);
                definitions.put(def.getName(), def);
            }
        }
        return definitions;
    }

    @Override
    public PrivilegeDefinition readDefinition(String privilegeName) {
        Tree definitionTree = privilegesTree.getChild(privilegeName);
        return (definitionTree == null) ? null : readDefinition(definitionTree);
    }

    //-----------------------------------------------------------< internal >---
    @Nonnull
    static PrivilegeDefinition readDefinition(Tree definitionTree) {
        NodeUtil n = new NodeUtil(definitionTree);
        String name = n.getName();
        boolean isAbstract = n.getBoolean(REP_IS_ABSTRACT);
        String[] declAggrNames = n.getStrings(REP_AGGREGATES);

        return new PrivilegeDefinitionImpl(name, isAbstract, declAggrNames);
    }
}