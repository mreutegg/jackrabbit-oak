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
package org.apache.jackrabbit.mongomk.impl.action;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * An action for fetching nodes.
 */
public class FetchNodesAction extends BaseAction<List<MongoNode>> {

    public static final int LIMITLESS_DEPTH = -1;
    private static final Logger LOG = LoggerFactory.getLogger(FetchNodesAction.class);

    private final Set<String> paths;
    private final long revisionId;

    private String branchId;
    private int depth = LIMITLESS_DEPTH;
    private boolean fetchDescendants;

    /**
     * Constructs a new {@code FetchNodesAction} to fetch a node and optionally
     * its descendants under the specified path.
     *
     * @param nodeStore Node store.
     * @param path The path.
     * @param fetchDescendants Determines whether the descendants of the path
     * will be fetched as well.
     * @param revisionId The revision id.
     */
    public FetchNodesAction(MongoNodeStore nodeStore, String path,
            boolean fetchDescendants, long revisionId) {
        super(nodeStore);
        paths = new HashSet<String>();
        paths.add(path);
        this.fetchDescendants = fetchDescendants;
        this.revisionId = revisionId;
    }

    /**
     * Constructs a new {@code FetchNodesAction} to fetch nodes with the exact
     * specified paths.
     *
     * @param nodeStore Node store.
     * @param path The exact paths to fetch nodes for.
     * @param revisionId The revision id.
     */
    public FetchNodesAction(MongoNodeStore nodeStore,  Set<String> paths,
            long revisionId) {
        super(nodeStore);
        this.paths = paths;
        this.revisionId = revisionId;
    }

    /**
     * Sets the branchId for the query.
     *
     * @param branchId Branch id.
     */
    public void setBranchId(String branchId) {
        this.branchId = branchId;
    }

    /**
     * Sets the depth for the command. Only used when fetchDescendants is enabled.
     *
     * @param depth The depth for the command or -1 for limitless depth.
     */
    public void setDepth(int depth) {
        this.depth = depth;
    }

    @Override
    public List<MongoNode> execute() {
        if (paths.isEmpty()) {
            return Collections.emptyList();
        }
        DBCursor dbCursor = performQuery();
        List<MongoCommit> validCommits = new FetchCommitsAction(nodeStore, revisionId).execute();
        return getMostRecentValidNodes(dbCursor, validCommits);
    }

    private DBCursor performQuery() {
        QueryBuilder queryBuilder = QueryBuilder.start(MongoNode.KEY_PATH);
        if (paths.size() > 1) {
            queryBuilder = queryBuilder.in(paths);
        } else {
            String path = paths.toArray(new String[0])[0];
            if (fetchDescendants) {
                Pattern pattern = createPrefixRegExp(path);
                queryBuilder = queryBuilder.regex(pattern);
            } else {
                queryBuilder = queryBuilder.is(path);
            }
        }

        if (revisionId > 0) {
            queryBuilder = queryBuilder.and(MongoNode.KEY_REVISION_ID).lessThanEquals(revisionId);
        }

        if (branchId == null) {
            DBObject query = new BasicDBObject(MongoNode.KEY_BRANCH_ID, new BasicDBObject("$exists", false));
            queryBuilder = queryBuilder.and(query);
        } else {
            // Not only return nodes in the branch but also nodes in the trunk
            // before the branch was created.
            FetchBranchBaseRevisionIdAction action = new FetchBranchBaseRevisionIdAction(nodeStore, branchId);
            long headBranchRevisionId = action.execute();

            DBObject branchQuery = QueryBuilder.start().or(
                    QueryBuilder.start(MongoNode.KEY_BRANCH_ID).is(branchId).get(),
                    QueryBuilder.start(MongoNode.KEY_REVISION_ID).lessThanEquals(headBranchRevisionId).get()
            ).get();
            queryBuilder = queryBuilder.and(branchQuery);
        }

        DBObject query = queryBuilder.get();
        LOG.debug(String.format("Executing query: %s", query));

        return nodeStore.getNodeCollection().find(query);
    }

    private Pattern createPrefixRegExp(String path) {
        StringBuilder sb = new StringBuilder();

        if (depth < 0) {
            sb.append("^");
            sb.append(path);
        } else if (depth == 0) {
            sb.append("^");
            sb.append(path);
            sb.append("$");
        } else if (depth > 0) {
            sb.append("^");
            if (!"/".equals(path)) {
                sb.append(path);
            }
            sb.append("(/[^/]*)");
            sb.append("{0,");
            sb.append(depth);
            sb.append("}$");
        }

        return Pattern.compile(sb.toString());
    }

    private List<MongoNode> getMostRecentValidNodes(DBCursor dbCursor,
            List<MongoCommit> validCommits) {
        List<Long> validRevisions = extractRevisionIds(validCommits);
        Map<String, MongoNode> nodeMongos = new HashMap<String, MongoNode>();

        while (dbCursor.hasNext()) {
            MongoNode nodeMongo = (MongoNode) dbCursor.next();

            String path = nodeMongo.getPath();
            long revisionId = nodeMongo.getRevisionId();

            LOG.debug(String.format("Converting node %s (%d)", path, revisionId));

            if (!validRevisions.contains(revisionId)) {
                LOG.debug(String.format("Node will not be converted as it is not a valid commit %s (%d)",
                        path, revisionId));
                continue;
            }

            MongoNode existingNodeMongo = nodeMongos.get(path);
            if (existingNodeMongo != null) {
                long existingRevId = existingNodeMongo.getRevisionId();

                if (revisionId > existingRevId) {
                    nodeMongos.put(path, nodeMongo);
                    LOG.debug(String.format("Converted nodes was put into map and replaced %s (%d)", path, revisionId));
                } else {
                    LOG.debug(String.format("Converted nodes was not put into map because a newer version"
                            + " is available %s (%d)", path, revisionId));
                }
            } else {
                nodeMongos.put(path, nodeMongo);
                LOG.debug("Converted node was put into map");
            }
        }

        return new ArrayList<MongoNode>(nodeMongos.values());
    }

    private List<Long> extractRevisionIds(List<MongoCommit> validCommits) {
        List<Long> validRevisions = new ArrayList<Long>(validCommits.size());
        for (MongoCommit commitMongo : validCommits) {
            validRevisions.add(commitMongo.getRevisionId());
        }
        return validRevisions;
    }
}