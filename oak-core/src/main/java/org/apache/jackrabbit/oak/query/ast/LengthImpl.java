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

import javax.jcr.PropertyType;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

public class LengthImpl extends DynamicOperandImpl {

    private final PropertyValueImpl propertyValue;

    public LengthImpl(PropertyValueImpl propertyValue) {
        this.propertyValue = propertyValue;
    }

    public PropertyValueImpl getPropertyValue() {
        return propertyValue;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "LENGTH(" + getPropertyValue() + ')';
    }

    @Override
    public CoreValue currentValue() {
        CoreValue v = propertyValue.currentValue();
        if (v == null) {
            return null;
        }
        return query.getValueFactory().createValue(v.length());
    }

    @Override
    public void apply(FilterImpl f, Operator operator, CoreValue v) {
        switch (v.getType()) {
        case PropertyType.LONG:
        case PropertyType.DECIMAL:
        case PropertyType.DOUBLE:
            // ok - comparison with a number
            break;
        case PropertyType.BINARY:
        case PropertyType.STRING:
        case PropertyType.DATE:
            // ok - compare with a string literal
            break;
        default:
            throw new IllegalArgumentException(
                    "Can not compare the length with a constant of type "
                            + PropertyType.nameFromValue(v.getType()) +
                            " and value " + v.toString());
        }
        // TODO LENGTH(x) conditions: can use IS NOT NULL as a condition?
    }

}
