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
import java.util.List;

import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * An action for fetching valid commits.
 */
public class FetchCommitsAction extends BaseAction<List<MongoCommit>> {

    private static final int LIMITLESS = -1;
    private static final Logger LOG = LoggerFactory.getLogger(FetchCommitsAction.class);

    private long fromRevisionId = -1;
    private long toRevisionId = -1;
    private int maxEntries = LIMITLESS;
    private boolean includeBranchCommits = true;

    /**
     * Constructs a new {@link FetchCommitsAction}
     *
     * @param nodeStore Node store.
     */
    public FetchCommitsAction(MongoNodeStore nodeStore) {
        this(nodeStore, -1L, -1L);
    }

    /**
     * Constructs a new {@link FetchCommitsAction}
     *
     * @param nodeStore Node store.
     * @param toRevisionId To revision id.
     */
    public FetchCommitsAction(MongoNodeStore nodeStore, long toRevisionId) {
        this(nodeStore, -1L, toRevisionId);
    }

    /**
     * Constructs a new {@link FetchCommitsAction}
     *
     * @param nodeStore Node store.
     * @param fromRevisionId From revision id.
     * @param toRevisionId To revision id.
     */
    public FetchCommitsAction(MongoNodeStore nodeStore, long fromRevisionId,
            long toRevisionId) {
        super(nodeStore);
        this.fromRevisionId = fromRevisionId;
        this.toRevisionId = toRevisionId;
    }

    /**
     * Sets the max number of entries that should be fetched.
     *
     * @param maxEntries The max number of entries.
     * @return this action.
     */
    public FetchCommitsAction setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
        return this;
    }

    /**
     * Sets whether the branch commits are included in the query.
     *
     * @param includeBranchCommits Whether the branch commits are included.
     * @return this action.
     */
    public FetchCommitsAction includeBranchCommits(boolean includeBranchCommits) {
        this.includeBranchCommits = includeBranchCommits;
        return this;
    }

    @Override
    public List<MongoCommit> execute() {
        if (maxEntries == 0) {
            return Collections.emptyList();
        }
        DBCursor dbCursor = fetchListOfValidCommits();
        return convertToCommits(dbCursor);
    }

    private DBCursor fetchListOfValidCommits() {
        DBCollection commitCollection = nodeStore.getCommitCollection();
        QueryBuilder queryBuilder = QueryBuilder.start(MongoCommit.KEY_FAILED).notEquals(Boolean.TRUE);
        if (fromRevisionId > -1) {
            queryBuilder = queryBuilder.and(MongoCommit.KEY_REVISION_ID).greaterThanEquals(fromRevisionId);
        }
        if (toRevisionId > -1) {
            queryBuilder = queryBuilder.and(MongoCommit.KEY_REVISION_ID).lessThanEquals(toRevisionId);
        }

        if (!includeBranchCommits) {
            queryBuilder = queryBuilder.and(new BasicDBObject(MongoNode.KEY_BRANCH_ID,
                    new BasicDBObject("$exists", false)));
        }

        DBObject query = queryBuilder.get();
        DBObject orderBy = new BasicDBObject(MongoCommit.KEY_REVISION_ID, -1);

        LOG.debug("Executing query: {}", query);

        return maxEntries > 0? commitCollection.find(query).limit(maxEntries).sort(orderBy)
                : commitCollection.find(query).sort(orderBy);
    }

    private List<MongoCommit> convertToCommits(DBCursor dbCursor) {
        try {
            List<MongoCommit> commits = new ArrayList<MongoCommit>();
            while (dbCursor.hasNext()) {
                commits.add((MongoCommit) dbCursor.next());
            }
            LOG.debug("Found list of valid revisions for max revision {}: {}",
                    toRevisionId, commits);

            return commits;
        } finally {
            dbCursor.close();
        }
    }
}
