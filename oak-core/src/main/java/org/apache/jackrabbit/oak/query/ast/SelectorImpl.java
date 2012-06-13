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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.query.Query;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.Cursor;
import org.apache.jackrabbit.oak.spi.QueryIndex;

public class SelectorImpl extends SourceImpl {

    // TODO jcr:path isn't an official feature, support it?
    private static final String PATH = "jcr:path";

    private static final String JCR_PRIMARY_TYPE = "jcr:primaryType";

    private static final String TYPE_BASE = "nt:base";

    protected QueryIndex index;

    private final String nodeTypeName, selectorName;
    private Cursor cursor;

    public SelectorImpl(String nodeTypeName, String selectorName) {
        this.nodeTypeName = nodeTypeName;
        this.selectorName = selectorName;
    }

    public String getNodeTypeName() {
        return nodeTypeName;
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
        // TODO quote nodeTypeName?
        return nodeTypeName + " AS " + getSelectorName();
    }


    @Override
    public void prepare(MicroKernel mk) {
        index = query.getBestIndex(createFilter());
    }

    @Override
    public void execute(String revisionId) {
        cursor = index.query(createFilter(), revisionId);
    }

    @Override
    public String getPlan() {
        return  nodeTypeName + " AS " + getSelectorName() + " /* " + index.getPlan(createFilter()) + " */";
    }

    private FilterImpl createFilter() {
        FilterImpl f = new FilterImpl(this);
        f.setNodeType(nodeTypeName);
        if (joinCondition != null) {
            joinCondition.apply(f);
        }
        if (!outerJoin) {
            // for outer joins, query constraints can't be applied to the
            // filter, because that would alter the result
            if (queryConstraint != null) {
                queryConstraint.apply(f);
            }
        }
        return f;
    }

    @Override
    public boolean next() {
        if (cursor == null) {
            return false;
        }
        while (true) {
            boolean result = cursor.next();
            if (!result) {
                return false;
            }
            if (nodeTypeName.equals(TYPE_BASE)) {
                return true;
            }
            Tree tree = getTree(cursor.currentPath());
            PropertyState p = tree.getProperty(JCR_PRIMARY_TYPE);
            if (p == null) {
                return true;
            }
            CoreValue v = p.getValue();
            // TODO node type matching
            if (nodeTypeName.equals(v.getString())) {
                return true;
            }
        }
    }

    @Override
    public String currentPath() {
        return cursor == null ? null : cursor.currentPath();
    }

    public CoreValue currentProperty(String propertyName) {
        if (propertyName.equals(PATH)) {
            String p = currentPath();
            if (p == null) {
                return null;
            }
            String local = getLocalPath(p);
            if (local == null) {
                // not a local path
                return null;
            }
            return query.getValueFactory().createValue(local);
        }
        String path = currentPath();
        if (path == null) {
            return null;
        }
        PropertyState p = getTree(path).getProperty(propertyName);
        if (p == null) {
            return null;
        }
        return p.getValue();
    }

    @Override
    public void init(Query query) {
        // nothing to do
    }

    @Override
    public SelectorImpl getSelector(String selectorName) {
        if (selectorName.equals(this.selectorName)) {
            return this;
        }
        return null;
    }

}
