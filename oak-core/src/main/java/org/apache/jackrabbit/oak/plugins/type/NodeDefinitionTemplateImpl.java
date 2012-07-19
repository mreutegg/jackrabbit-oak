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
package org.apache.jackrabbit.oak.plugins.type;

import java.util.ArrayList;
import java.util.List;

import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeTemplate;

import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory.AbstractNodeDefinitionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class NodeDefinitionTemplateImpl
        extends AbstractNodeDefinitionBuilder<NodeTypeTemplate>
        implements NodeDefinitionTemplate {

    private static final Logger log =
            LoggerFactory.getLogger(NodeDefinitionTemplateImpl.class);

    private String defaultPrimaryTypeName = null;

    private String[] requiredPrimaryTypeNames = null;

    protected NodeType getNodeType(String name) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public void build() {
        // do nothing by default
    }

    @Override
    public NodeType getDeclaringNodeType() {
        return null;
    }

    @Override
    public void setDeclaringNodeType(String name) {
        // ignore
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean isAutoCreated() {
        return autocreate;
    }

    @Override
    public void setAutoCreated(boolean autocreate) {
        this.autocreate = autocreate;
    }

    @Override
    public boolean isProtected() {
        return isProtected;
    }

    @Override
    public void setProtected(boolean isProtected) {
        this.isProtected = isProtected;
    }

    @Override
    public boolean isMandatory() {
        return isMandatory;
    }

    @Override
    public void setMandatory(boolean isMandatory) {
        this.isMandatory = isMandatory;
    }

    @Override
    public int getOnParentVersion() {
        return onParent;
    }

    @Override
    public void setOnParentVersion(int onParent) {
        this.onParent = onParent;
    }

    @Override
    public boolean allowsSameNameSiblings() {
        return allowSns;
    }

    @Override
    public void setSameNameSiblings(boolean allowSns) {
        this.allowSns = allowSns;
    }

    @Override
    public void setAllowsSameNameSiblings(boolean allowSns) {
        setSameNameSiblings(allowSns);
    }

    @Override
    public NodeType getDefaultPrimaryType() {
        if (defaultPrimaryTypeName != null) {
            try {
                return getNodeType(defaultPrimaryTypeName);
            } catch (RepositoryException e) {
                log.warn("Unable to access default primary type "
                        + defaultPrimaryTypeName + " of " + name, e);
            }
        }
        return null;
    }

    @Override
    public String getDefaultPrimaryTypeName() {
        return defaultPrimaryTypeName;
    }

    @Override
    public void setDefaultPrimaryTypeName(String name) {
        this.defaultPrimaryTypeName  = name;
    }

    @Override
    public void setDefaultPrimaryType(String name) {
        setDefaultPrimaryTypeName(name);
    }

    @Override
    public NodeType[] getRequiredPrimaryTypes() {
        if (requiredPrimaryTypeNames == null) {
            return new NodeType[0];
        } else {
            List<NodeType> types =
                    new ArrayList<NodeType>(requiredPrimaryTypeNames.length);
            for (int i = 0; i < requiredPrimaryTypeNames.length; i++) {
                try {
                    types.add(getNodeType(requiredPrimaryTypeNames[i]));
                } catch (RepositoryException e) {
                    log.warn("Unable to required primary primary type "
                            + requiredPrimaryTypeNames[i] + " of " + name, e);
                }
            }
            return types.toArray(new NodeType[types.size()]);
        }
    }

    @Override
    public String[] getRequiredPrimaryTypeNames() {
        return requiredPrimaryTypeNames;
    }

    @Override
    public void setRequiredPrimaryTypeNames(String[] names) {
        this.requiredPrimaryTypeNames = names;
    }

    @Override
    public void addRequiredPrimaryType(String name) {
        if (requiredPrimaryTypeNames == null) {
            requiredPrimaryTypeNames = new String[] { name };
        } else {
            String[] names = new String[requiredPrimaryTypeNames.length + 1];
            System.arraycopy(requiredPrimaryTypeNames, 0, names, 0, requiredPrimaryTypeNames.length);
            names[requiredPrimaryTypeNames.length] = name;
            requiredPrimaryTypeNames = names;
        }

    }

}