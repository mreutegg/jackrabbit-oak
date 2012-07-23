/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr.nodetype;

import java.util.HashMap;
import java.util.Map;

import javax.jcr.nodetype.NodeTypeTemplate;

import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory;

class DefBuilderFactory extends
        DefinitionBuilderFactory<NodeTypeTemplate, Map<String, String>> {

    private Map<String, String> namespaces = new HashMap<String, String>();

    @Override
    public NodeTypeTemplateImpl newNodeTypeDefinitionBuilder() {
        return new NodeTypeTemplateImpl();
    }

    @Override
    public Map<String, String> getNamespaceMapping() {
        return namespaces;
    }

    @Override
    public void setNamespaceMapping(Map<String, String> namespaces) {
        this.namespaces = namespaces;
    }

    @Override
    public void setNamespace(String prefix, String uri) {
        namespaces.put(prefix, uri);
    }

}
