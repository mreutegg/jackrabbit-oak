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
package org.apache.jackrabbit.oak.plugins.index.p2;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Charsets;

/**
 * Provides a QueryIndex that does lookups against a property index
 * 
 * <p>
 * To define a property index on a subtree you have to add an <code>oak:index</code> node.
 * 
 * Next (as a child node) follows the index definition node that:
 * <ul>
 * <li>must be of type <code>oak:queryIndexDefinition</code></li>
 * <li>must have the <code>type</code> property set to <b><code>p2</code></b></li>
 * <li>contains the <code>propertyNames</code> property that indicates what property will be stored in the index</li>
 * </ul>
 * </p>
 * <p>
 * Optionally you can specify
 * <ul> 
 * <li> a uniqueness constraint on a property index by setting the <code>unique</code> flag to <code>true</code></li>
 * <li> that the property index only applies to a certain node type by setting the <code>declaringNodeTypes</code> property</li>
 * </ul>
 * </p>
 * <p>
 * Note: <code>propertyNames</code> can be a list of properties, and it is optional.in case it is missing, the node name will be used as a property name reference value
 * </p>
 * 
 * <p>
 * Note: <code>reindex</code> is a property that when set to <code>true</code>, triggers a full content reindex.
 * </p>
 * 
 * <pre>
 * <code>
 * {
 *     NodeBuilder index = root.child("oak:index");
 *     index.child("uuid")
 *         .setProperty("jcr:primaryType", "oak:queryIndexDefinition", Type.NAME)
 *         .setProperty("type", "p2")
 *         .setProperty("propertyNames", "jcr:uuid")
 *         .setProperty("declaringNodeTypes", "mix:referenceable")
 *         .setProperty("unique", true)
 *         .setProperty("reindex", true);
 * }
 * </code>
 * </pre>
 * 
 * @see QueryIndex
 * @see Property2IndexLookup
 */
class Property2Index implements QueryIndex {

    public static final String TYPE = "p2";

    // TODO the max string length should be removed, or made configurable
    private static final int MAX_STRING_LENGTH = 100; 

    static List<String> encode(PropertyValue value) {
        List<String> values = new ArrayList<String>();

        for (String v : value.getValue(Type.STRINGS)) {
            try {
                if (v.length() > MAX_STRING_LENGTH) {
                    v = v.substring(0, MAX_STRING_LENGTH);
                }
                values.add(URLEncoder.encode(v, Charsets.UTF_8.name()));
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("UTF-8 is unsupported", e);
            }
        }
        return values;
    }

    //--------------------------------------------------------< QueryIndex >--

    @Override
    public String getIndexName() {
        return "p2";
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        Property2IndexLookup lookup = new Property2IndexLookup(root);
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            // TODO support indexes on a path
            // currently, only indexes on the root node are supported
            if (lookup.isIndexed(pr.propertyName, "/", filter)) {
                if (pr.firstIncluding && pr.lastIncluding
                    && pr.first != null && pr.first.equals(pr.last)) {
                    // "[property] = $value"
                    return lookup.getCost(filter, pr.propertyName, pr.first);
                } else if (pr.first == null && pr.last == null) {
                    // "[property] is not null"
                    return lookup.getCost(filter, pr.propertyName, null);
                }
            }
        }
        // not an appropriate index
        return Double.POSITIVE_INFINITY;
    }
    
    @Override
    public Cursor query(Filter filter, NodeState root) {
        Iterable<String> paths = null;

        Property2IndexLookup lookup = new Property2IndexLookup(root);
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            // TODO support indexes on a path
            // currently, only indexes on the root node are supported
            if (lookup.isIndexed(pr.propertyName, "/", filter)) {
                // equality
                if (pr.firstIncluding && pr.lastIncluding
                    && pr.first != null && pr.first.equals(pr.last)) {
                    // "[property] = $value"
                    paths = lookup.query(filter, pr.propertyName, pr.first);
                    break;
                } else if (pr.first == null && pr.last == null) {
                    // "[property] is not null"
                    paths = lookup.query(filter, pr.propertyName, null);
                    break;
                }
            }
        }
        if (paths == null) {
            throw new IllegalStateException("Property index is used even when no index is available for filter " + filter);
        }
        return Cursors.newPathCursor(paths);
    }
    
    @Override
    public String getPlan(Filter filter, NodeState root) {
        StringBuilder buff = new StringBuilder("p2");
        Property2IndexLookup lookup = new Property2IndexLookup(root);
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            // TODO support indexes on a path
            // currently, only indexes on the root node are supported
            if (lookup.isIndexed(pr.propertyName, "/", filter)) {
                if (pr.firstIncluding && pr.lastIncluding
                    && pr.first != null && pr.first.equals(pr.last)) {
                    buff.append(' ').append(pr.propertyName).append('=').append(pr.first);
                } else if (pr.first == null && pr.last == null) {
                    buff.append(' ').append(pr.propertyName);
                }
            }
        }
        return buff.toString();
    }

}