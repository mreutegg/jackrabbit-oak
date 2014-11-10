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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PropertyDefinition {
    private static final Logger log = LoggerFactory.getLogger(PropertyDefinition.class);
    private final String name;
    private final NodeState definition;

    private final int propertyType;
    private double fieldBoost;
    private boolean hasFieldBoost;

    public PropertyDefinition(IndexDefinition idxDefn, String name, NodeState defn) {
        this.name = name;
        this.definition = defn;

        int type = PropertyType.UNDEFINED;
        if(defn.hasProperty(LuceneIndexConstants.PROP_TYPE)){
            String typeName  = defn.getString(LuceneIndexConstants.PROP_TYPE);
            try{
                type = PropertyType.valueFromName(typeName);
            } catch (IllegalArgumentException e){
                log.warn("Invalid property type {} for property {} in Index {}", typeName, name, idxDefn);
            }
        }
        this.propertyType = type;
        this.hasFieldBoost = defn.hasProperty(LuceneIndexConstants.FIELD_BOOST) && (defn.getProperty(LuceneIndexConstants.FIELD_BOOST).getType().tag() == Type.DOUBLE.tag());
        // if !hasFieldBoost then setting default lucene field boost = 1.0
        fieldBoost = hasFieldBoost ? defn.getProperty(LuceneIndexConstants.FIELD_BOOST).getValue(Type.DOUBLE) : 1.0;
    }

    public int getPropertyType() {
        return propertyType;
    }

    public boolean hasFieldBoost() {
        return hasFieldBoost;
    }

    public double fieldBoost() {
        return fieldBoost;
    }
}
