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
package org.apache.jackrabbit.oak.security.user;

import java.text.ParseException;
import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.security.user.query.QueryUtil;
import org.apache.jackrabbit.oak.security.user.query.ResultRowToAuthorizable;
import org.apache.jackrabbit.oak.security.user.query.XPathQueryBuilder;
import org.apache.jackrabbit.oak.security.user.query.XPathQueryEvaluator;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.util.ISO9075;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserQueryManager... TODO
 */
class UserQueryManager {

    private static final Logger log = LoggerFactory.getLogger(UserQueryManager.class);

    private final UserManagerImpl userManager;
    private final Root root;

    UserQueryManager(UserManagerImpl userManager, Root root) {
        this.userManager = userManager;
        this.root = root;
    }

    @Nonnull
    Iterator<Authorizable> find(Query query) throws RepositoryException {
        // TODO OAK-253: replace usage of XPATH
        XPathQueryBuilder builder = new XPathQueryBuilder();
        query.build(builder);
        return new XPathQueryEvaluator(builder, userManager, root, userManager.getNamePathMapper(), userManager.getConfig()).eval();
    }

    @Nonnull
    Iterator<Authorizable> findAuthorizables(String relativePath, String value,
                                             AuthorizableType authorizableType) throws RepositoryException {
        return findAuthorizables(relativePath, value, true, authorizableType);
    }

    /**
     * Find the authorizable trees matching the following search parameters within
     * the sub-tree defined by an authorizable tree:
     *
     * @param relPath A relative path (or a name) pointing to properties within
     * the tree defined by a given authorizable node.
     * @param value The property value to look for.
     * @param exact A boolean flag indicating if the value must match exactly or not.s
     * @param type Filter the search results to only return authorizable
     * trees of a given type. Passing {@link org.apache.jackrabbit.oak.spi.security.user.AuthorizableType#AUTHORIZABLE} indicates that
     * no filtering for a specific authorizable type is desired. However, properties
     * might still be search in the complete sub-tree of authorizables depending
     * on the other query parameters.
     * @return An iterator of authorizable trees that match the specified
     * search parameters and filters or an empty iterator if no result can be
     * found.
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    @Nonnull
    Iterator<Authorizable> findAuthorizables(String relPath, String value,
                                             boolean exact, AuthorizableType type) throws RepositoryException {
        // TODO OAK-480: fix authorizable query
        // TODO OAK-253: replace usage of XPATH
        String statement = buildXPathStatement(relPath, value, exact, type);
        QueryEngine queryEngine = root.getQueryEngine();
        try {
            Result result = queryEngine.executeQuery(statement, javax.jcr.query.Query.XPATH, Long.MAX_VALUE, 0, null, userManager.getNamePathMapper());
            Iterator<Authorizable> authorizables = Iterators.transform(result.getRows().iterator(), new ResultRowToAuthorizable(userManager));
            return Iterators.filter(authorizables, Predicates.<Object>notNull());
        } catch (ParseException e) {
            log.warn("Invalid user query: " + statement, e);
            throw new RepositoryException(e);
        }
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private String buildXPathStatement(String relPath, String value, boolean exact, AuthorizableType type) {
        StringBuilder stmt = new StringBuilder();
        String searchRoot = QueryUtil.getSearchRoot(type, userManager.getConfig());
        if (!"/".equals(searchRoot)) {
            stmt.append(searchRoot);
        }

        String propName;
        String path;
        String ntName;
        if (relPath.indexOf('/') == -1) {
            // search for properties somewhere below an authorizable node
            propName = userManager.getNamePathMapper().getOakName(relPath);
            path = null;
            ntName = null;
        } else {
            // FIXME: proper normalization of the relative path
            String oakPath = userManager.getNamePathMapper().getOakPath(relPath);
            propName = Text.getName(oakPath);
            path = Text.getRelativeParent(oakPath, 1);
            ntName = QueryUtil.getNodeTypeName(type);
        }

        stmt.append("//");
        if (path != null && !path.isEmpty()) {
            stmt.append(path);
        } else if (ntName != null) {
            stmt.append("element(*,").append(ntName).append(')');
        } else {
            stmt.append("element(*)");
        }

        if (value == null) {
            // property must exist
            stmt.append("[@").append(propName).append(']');
        } else {
            stmt.append('[');
            stmt.append((exact) ? "@" : "jcr:like(@");
            stmt.append(ISO9075.encode(propName));
            if (exact) {
                stmt.append("='");
                stmt.append(value.replaceAll("'", "''"));
                stmt.append('\'');
            } else {
                stmt.append(",'%");
                stmt.append(escapeForQuery(value));
                stmt.append("%')");
            }
            stmt.append(']');
        }
        return stmt.toString();
    }

    @Nonnull
    private static String escapeForQuery(String value) {
        StringBuilder ret = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\') {
                ret.append("\\\\");
            } else if (c == '\'') {
                ret.append("''");
            } else {
                ret.append(c);
            }
        }
        return ret.toString();
    }
}