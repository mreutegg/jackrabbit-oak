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
package org.apache.jackrabbit.oak.query.ast;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.SinglePropertyState;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * The function "localname(..)".
 */
public class NodeLocalNameImpl extends DynamicOperandImpl {

    private final String selectorName;
    private SelectorImpl selector;

    public NodeLocalNameImpl(String selectorName) {
        this.selectorName = selectorName;
    }

    public String getSelectorName() {
        return selectorName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "localname(" + getSelectorName() + ')';
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public PropertyState currentProperty() {
        String name = PathUtils.getName(selector.currentPath());
        int colon = name.indexOf(':');
        // TODO LOCALNAME: evaluation of local name might not be correct
        String localName = colon < 0 ? name : name.substring(colon + 1);
        CoreValue v = query.getValueFactory().createValue(localName);
        return new SinglePropertyState("LOCALNAME", v);
    }

    @Override
    public void apply(FilterImpl f, Operator operator, CoreValue v) {
        // TODO support LOCALNAME index conditions
    }

}
