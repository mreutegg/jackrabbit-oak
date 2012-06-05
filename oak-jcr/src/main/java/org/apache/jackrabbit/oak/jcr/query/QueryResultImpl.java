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
package org.apache.jackrabbit.oak.jcr.query;

import org.apache.jackrabbit.commons.iterator.NodeIteratorAdapter;
import org.apache.jackrabbit.commons.iterator.RowIteratorAdapter;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;

import javax.annotation.CheckForNull;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The implementation of the corresponding JCR interface.
 */
public class QueryResultImpl implements QueryResult {

    final SessionDelegate sessionDelegate;
    final Result result;
    final String pathFilter;

    public QueryResultImpl(SessionDelegate sessionDelegate, Result result) {
        this.sessionDelegate = sessionDelegate;
        this.result = result;

        // TODO the path currently contains the workspace name
        // TODO filter in oak-core once we support workspaces there
        pathFilter = "/";
    }

    @Override
    public String[] getColumnNames() throws RepositoryException {
        return result.getColumnNames();
    }

    @Override
    public String[] getSelectorNames() {
        return result.getSelectorNames();
    }

    boolean includeRow(String path) {
        if (path == null) {
            // a row without path (explain,...)
            return true;
        } else if (PathUtils.isAncestor(pathFilter, path)) {
            // a row within this workspace
            return true;
        }
        return false;
    }

    @Override
    public RowIterator getRows() throws RepositoryException {
        Iterator<RowImpl> it = new Iterator<RowImpl>() {

            private final Iterator<? extends ResultRow> it = result.getRows().iterator();
            private RowImpl current;

            {
                fetch();
            }

            private void fetch() {
                current = null;
                while(it.hasNext()) {
                    ResultRow r = it.next();
                    for (String s : getSelectorNames()) {
                        String path = r.getPath(s);
                        if (includeRow(path)) {
                            current = new RowImpl(QueryResultImpl.this, r);
                            return;
                        }
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public RowImpl next() {
                if (current == null) {
                    throw new NoSuchElementException();
                }
                RowImpl r = current;
                fetch();
                return r;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
        return new RowIteratorAdapter(it);
    }

    @CheckForNull
    NodeImpl getNode(String path) {
        NodeDelegate d = sessionDelegate.getNode(path);
        return d == null ? null : new NodeImpl(d);
    }

    String getLocalPath(String path) {
        return PathUtils.concat("/", PathUtils.relativize(pathFilter, path));
    }

    @Override
    public NodeIterator getNodes() throws RepositoryException {
        if (getSelectorNames().length > 1) {
            throw new RepositoryException("Query contains more than one selector: " +
                    Arrays.toString(getSelectorNames()));
        }
        Iterator<NodeImpl> it = new Iterator<NodeImpl>() {

            private final Iterator<? extends ResultRow> it = result.getRows().iterator();
            private NodeImpl current;

            {
                fetch();
            }

            private void fetch() {
                current = null;
                while(it.hasNext()) {
                    ResultRow r = it.next();
                    String path = r.getPath();
                    if (includeRow(path)) {
                        current = getNode(getLocalPath(path));
                        break;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public NodeImpl next() {
                if (current == null) {
                    throw new NoSuchElementException();
                }
                NodeImpl n = current;
                fetch();
                return n;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
        return new NodeIteratorAdapter(it);
    }

    Value createValue(CoreValue value) {
        return sessionDelegate.getValueFactory().createValue(value);
    }

}
