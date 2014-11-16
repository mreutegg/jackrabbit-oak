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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.FIELD_BOOST;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_IS_REGEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.ConfigUtil.getOptionalValue;

class PropertyDefinition {
    private static final Logger log = LoggerFactory.getLogger(PropertyDefinition.class);
    /**
     * The default boost: 1.0f.
     */
    static final float DEFAULT_BOOST = 1.0f;

    private final NodeState definition;
    /**
     * Property name. By default derived from the NodeState name which has the
     * property definition. However in case property name is a pattern, relative
     * property etc then it should be defined via 'name' property in NodeState.
     * In such case NodeState name can be set to anything
     */
    final String name;

    final int propertyType;
    /**
     * The boost value for a property.
     */
    final float boost;

    final boolean isRegexp;

    final boolean index;

    final boolean stored;

    final boolean nodeScopeIndex;

    final boolean propertyIndex;

    final boolean analyzed;

    final boolean ordered;

    public PropertyDefinition(IndexingRule idxDefn, String name, NodeState defn) {
        this.isRegexp = getOptionalValue(defn, PROP_IS_REGEX, false);
        this.name = getName(defn, name);
        this.definition = defn;
        this.boost = getOptionalValue(defn, FIELD_BOOST, DEFAULT_BOOST);

        //By default if a property is defined it is indexed
        this.index = getOptionalValue(defn, LuceneIndexConstants.PROP_INDEX, true);
        this.stored = getOptionalValue(defn, LuceneIndexConstants.PROP_USE_IN_EXCERPT, idxDefn.defaultStorageEnabled);
        this.nodeScopeIndex = getOptionalValue(defn, LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, idxDefn.defaultFulltextEnabled);
        this.analyzed = getOptionalValue(defn, LuceneIndexConstants.PROP_ANALYZED, idxDefn.defaultFulltextEnabled);

        //If node is not set for full text then a property definition indicates that definition is for property index
        this.propertyIndex = getOptionalValue(defn, LuceneIndexConstants.PROP_PROPERTY_INDEX, !idxDefn.defaultFulltextEnabled);
        this.ordered = getOptionalValue(defn, LuceneIndexConstants.PROP_ORDERED, false);

        //TODO Add test case for above cases

        this.propertyType = getPropertyType(idxDefn, name, defn);
    }

    public int getPropertyType() {
        return propertyType;
    }

    public boolean skipTokenization(String propertyName) {
        //For regEx case check against a whitelist
        if (isRegexp){
            return LuceneIndexHelper.skipTokenization(propertyName);
        }
        return !analyzed;
    }

    public boolean fulltextEnabled(){
        return index && (analyzed || nodeScopeIndex);
    }

    @Override
    public String toString() {
        return "PropertyDefinition{" +
                "name='" + name + '\'' +
                ", propertyType=" + propertyType +
                ", boost=" + boost +
                ", isRegexp=" + isRegexp +
                ", index=" + index +
                ", stored=" + stored +
                ", nodeScopeIndex=" + nodeScopeIndex +
                ", propertyIndex=" + propertyIndex +
                ", analyzed=" + analyzed +
                ", ordered=" + ordered +
                '}';
    }

    //~---------------------------------------------< internal >

    private static String getName(NodeState definition, String defaultName){
        PropertyState ps = definition.getProperty(LuceneIndexConstants.PROP_NAME);
        return ps == null ? defaultName : ps.getValue(Type.STRING);
    }

    private static int getPropertyType(IndexingRule idxDefn, String name, NodeState defn) {
        int type = PropertyType.UNDEFINED;
        if (defn.hasProperty(LuceneIndexConstants.PROP_TYPE)) {
            String typeName = defn.getString(LuceneIndexConstants.PROP_TYPE);
            try {
                type = PropertyType.valueFromName(typeName);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid property type {} for property {} in Index {}", typeName, name, idxDefn);
            }
        }
        return type;
    }
}
