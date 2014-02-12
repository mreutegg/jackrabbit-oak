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

import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;

/**
 * The "a.x = b.y" join condition.
 */
public class EquiJoinConditionImpl extends JoinConditionImpl {

    private final String property1Name;
    private final String property2Name;
    private final String selector1Name;
    private final String selector2Name;
    private SelectorImpl selector1;
    private SelectorImpl selector2;

    public EquiJoinConditionImpl(String selector1Name, String property1Name, String selector2Name,
            String property2Name) {
        this.selector1Name = selector1Name;
        this.property1Name = property1Name;
        this.selector2Name = selector2Name;
        this.property2Name = property2Name;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return quote(selector1Name) + '.' + quote(property1Name) +
                " = " + quote(selector2Name) + '.' + quote(property2Name);
    }

    public void bindSelector(SourceImpl source) {
        selector1 = source.getExistingSelector(selector1Name);
        selector2 = source.getExistingSelector(selector2Name);
    }

    @Override
    public boolean evaluate() {
        PropertyValue p1 = selector1.currentProperty(property1Name);
        if (p1 == null) {
            return false;
        }
        PropertyValue p2 = selector2.currentProperty(property2Name);
        if (p2 == null) {
            return false;
        }
        if (!p1.isArray() && !p2.isArray()) {
            // both are single valued
            return PropertyValues.match(p1, p2);
        }
        // TODO what is the expected result of an equi join for multi-valued properties?
        if (!p1.isArray() && p2.isArray()) {
            p1 = convertValueToType(p1, p2);
            if (p1 != null && PropertyValues.match(p1, p2)) {
                return true;
            }
            return false;
        } else if (p1.isArray() && !p2.isArray()) {
            p2 = convertValueToType(p2, p1);
            if (p2 != null && PropertyValues.match(p1, p2)) {
                return true;
            }
            return false;
        }
        return PropertyValues.match(p1, p2);
    }

    @Override
    public void restrict(FilterImpl f) {
        if (f.getSelector() == selector1) {
            PropertyValue p2 = selector2.currentProperty(property2Name);
            if (p2 == null && f.isPreparing() && selector2.isPrepared()) {
                // during the prepare phase, if the selector is already
                // prepared, then we would know the value
                p2 = PropertyValues.newString(KNOWN_VALUE);
            }
            if (p2 != null) {
                if (p2.isArray()) {
                    // TODO support join on multi-valued properties
                    p2 = null;
                }
            }
            // always set the condition, even if unkown ( -> is not null)
            String p1n = normalizePropertyName(property1Name);
            f.restrictProperty(p1n, Operator.EQUAL, p2);
        }
        if (f.getSelector() == selector2) {
            PropertyValue p1 = selector1.currentProperty(property1Name);
            if (p1 == null && f.isPreparing() && selector1.isPrepared()) {
                // during the prepare phase, if the selector is already
                // prepared, then we would know the value
                p1 = PropertyValues.newString(KNOWN_VALUE);
            }
            if (p1 != null) {
                if (p1.isArray()) {
                    // TODO support join on multi-valued properties
                    p1 = null;
                }
            }
            // always set the condition, even if unkown ( -> is not null)
            String p2n = normalizePropertyName(property2Name);
            f.restrictProperty(p2n, Operator.EQUAL, p1);
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        // both properties may not be null
        if (s == selector1) {
            PropertyExistenceImpl ex = new PropertyExistenceImpl(s.getSelectorName(), property1Name);
            ex.bindSelector(s);
            s.restrictSelector(ex);
        } else if (s == selector2) {
            PropertyExistenceImpl ex = new PropertyExistenceImpl(s.getSelectorName(), property2Name);
            ex.bindSelector(s);
            s.restrictSelector(ex);
        }
    }
    
    @Override
    public boolean isParent(SourceImpl source) {
        return false;
    }
    
    @Override
    public boolean canEvaluate(Set<SourceImpl> available) {
        return available.contains(selector1) && available.contains(selector2);
    }

}
