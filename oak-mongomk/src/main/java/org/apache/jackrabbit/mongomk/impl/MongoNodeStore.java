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
package org.apache.jackrabbit.mongomk.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.jackrabbit.mk.util.SimpleLRUCache;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.api.command.Command;
import org.apache.jackrabbit.mongomk.api.command.CommandExecutor;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitAction;
import org.apache.jackrabbit.mongomk.impl.command.CommitCommandNew;
import org.apache.jackrabbit.mongomk.impl.command.DefaultCommandExecutor;
import org.apache.jackrabbit.mongomk.impl.command.DiffCommand;
import org.apache.jackrabbit.mongomk.impl.command.GetHeadRevisionCommand;
import org.apache.jackrabbit.mongomk.impl.command.GetJournalCommand;
import org.apache.jackrabbit.mongomk.impl.command.GetNodesCommandNew;
import org.apache.jackrabbit.mongomk.impl.command.GetRevisionHistoryCommand;
import org.apache.jackrabbit.mongomk.impl.command.MergeCommand;
import org.apache.jackrabbit.mongomk.impl.command.NodeExistsCommand;
import org.apache.jackrabbit.mongomk.impl.command.OneLevelDiffCommand;
import org.apache.jackrabbit.mongomk.impl.command.WaitForCommitCommand;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.apache.jackrabbit.mongomk.impl.model.MongoSync;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Implementation of {@link NodeStore} for the {@code MongoDB}.
 */
public class MongoNodeStore implements NodeStore {

    public static final String INITIAL_COMMIT_MESSAGE = "This is an autogenerated initial commit";
    public static final String INITIAL_COMMIT_PATH = "";
    public static final String INITIAL_COMMIT_DIFF = "+\"/\" : {}";

    public static final String COLLECTION_COMMITS = "commits";
    public static final String COLLECTION_NODES = "nodes";
    public static final String COLLECTION_SYNC = "sync";

    private static final Logger LOG = LoggerFactory.getLogger(MongoNodeStore.class);

    private final CommandExecutor commandExecutor;
    private final DB db;

    private Map<Long, MongoCommit> commitCache = Collections.synchronizedMap(SimpleLRUCache.<Long, MongoCommit> newInstance(1000));
    private Map<String, MongoNode> nodeCache = Collections.synchronizedMap(SimpleLRUCache.<String, MongoNode> newInstance(10000));

    /**
     * Constructs a new {@code NodeStoreMongo}.
     *
     * @param db Mongo DB.
     */
    public MongoNodeStore(DB db) {
        commandExecutor = new DefaultCommandExecutor();
        this.db = db;
        init();
    }

    @Override
    public String commit(Commit commit) throws Exception {
        Command<Long> command = new CommitCommandNew(this, commit);
        Long revisionId = commandExecutor.execute(command);
        return MongoUtil.fromMongoRepresentation(revisionId);
    }

    @Override
    public String diff(String fromRevision, String toRevision, String path, int depth)
            throws Exception {
        Command<String> command;
        if (depth == 0) {
            command = new OneLevelDiffCommand(this, fromRevision, toRevision, path);
        } else {
            command = new DiffCommand(this, fromRevision, toRevision, path, depth);
        }
        return commandExecutor.execute(command);
    }

    @Override
    public String getHeadRevision() throws Exception {
        GetHeadRevisionCommand command = new GetHeadRevisionCommand(this);
        long revisionId = commandExecutor.execute(command);
        return MongoUtil.fromMongoRepresentation(revisionId);
    }

    @Override
    public Node getNodes(String path, String revisionId, int depth, long offset,
            int maxChildNodes, String filter) throws Exception {
        GetNodesCommandNew command = new GetNodesCommandNew(this, path,
                MongoUtil.toMongoRepresentation(revisionId));
        command.setBranchId(getBranchId(revisionId));
        command.setDepth(depth);
        return commandExecutor.execute(command);
    }

    @Override
    public String merge(String branchRevisionId, String message) throws Exception {
        MergeCommand command = new MergeCommand(this, branchRevisionId, message);
        return commandExecutor.execute(command);
    }

    @Override
    public boolean nodeExists(String path, String revisionId) throws Exception {
        NodeExistsCommand command = new NodeExistsCommand(this, path,
                MongoUtil.toMongoRepresentation(revisionId));
        String branchId = getBranchId(revisionId);
        command.setBranchId(branchId);
        return commandExecutor.execute(command);
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId, String path)
            throws Exception {
        GetJournalCommand command = new GetJournalCommand(this, fromRevisionId, toRevisionId, path);
        return commandExecutor.execute(command);
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path)
            throws Exception {
        GetRevisionHistoryCommand command = new GetRevisionHistoryCommand(this,
                since, maxEntries, path);
        return commandExecutor.execute(command);
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout) throws Exception {
        WaitForCommitCommand command = new WaitForCommitCommand(this,
                oldHeadRevisionId, timeout);
        long revisionId = commandExecutor.execute(command);
        return MongoUtil.fromMongoRepresentation(revisionId);
    }

    /**
     * Returns the commit {@link DBCollection}.
     *
     * @return The commit {@link DBCollection}.
     */
    public DBCollection getCommitCollection() {
        DBCollection commitCollection = db.getCollection(COLLECTION_COMMITS);
        commitCollection.setObjectClass(MongoCommit.class);
        return commitCollection;
    }

    /**
     * Returns the sync {@link DBCollection}.
     *
     * @return The sync {@link DBCollection}.
     */
    public DBCollection getSyncCollection() {
        DBCollection syncCollection = db.getCollection(COLLECTION_SYNC);
        syncCollection.setObjectClass(MongoSync.class);
        return syncCollection;
    }

    /**
     * Returns the node {@link DBCollection}.
     *
     * @return The node {@link DBCollection}.
     */
    public DBCollection getNodeCollection() {
        DBCollection nodeCollection = db.getCollection(COLLECTION_NODES);
        nodeCollection.setObjectClass(MongoNode.class);
        return nodeCollection;
    }

    /**
     * Caches the commit.
     *
     * @param commit Commit to cache.
     */
    public void cache(Commit commit) {
        LOG.debug("Adding commit {} to cache", commit.getRevisionId());
        commitCache.put(commit.getRevisionId(), (MongoCommit)commit);
    }

    /**
     * Evicts the commit from the {@link #commitCache}.
     *
     * @param commit the commit.
     */
    public void evict(MongoCommit commit) {
        if (commitCache.remove(commit.getRevisionId()) != null) {
            LOG.debug("Removed commit {} from cache", commit.getRevisionId());
        }
    }

    /**
     * Returns the commit from the cache or null if the commit is not in the cache.
     *
     * @param revisionId Commit revision id.
     * @return Commit from cache or null if commit is not in the cache.
     */
    public MongoCommit getFromCache(long revisionId) {
        MongoCommit commit = commitCache.get(revisionId);
        if (commit != null) {
            LOG.debug("Returning commit {} from cache", revisionId);
        }
        return commit;
    }

    /**
     * Caches the node.
     *
     * @param node Node to cache.
     */
    public void cache(MongoNode node) {
        long revisionId = node.getRevisionId();
        String path = node.getPath();
        String branchId = node.getBranchId();
        String key = path + "*" + branchId + "*" + revisionId;
        if (!nodeCache.containsKey(key)) {
            LOG.debug("Adding node to cache: {}", key);
            nodeCache.put(key, node.copy());
        }
    }

    /**
     * Returns the node from the cache or null if the node is not in the cache.
     *
     * @param path Path
     * @param branchId Branch id
     * @param revisionId Revision id
     * @return
     */
    public MongoNode getFromCache(String path, String branchId, long revisionId) {
        String key = path + "*" + branchId + "*" + revisionId;
        MongoNode node = nodeCache.get(key);
        if (node == null) {
            return null;
        }
        LOG.debug("Returning node from cache: {}", key);
        return node;
    }

    private void init() {
        initCommitCollection();
        initNodeCollection();
        initSyncCollection();
    }

    private void initCommitCollection() {
        if (db.collectionExists(COLLECTION_COMMITS)){
            return;
        }
        DBCollection commitCollection = getCommitCollection();
        DBObject index = new BasicDBObject();
        index.put(MongoCommit.KEY_REVISION_ID, 1L);
        index.put(MongoCommit.KEY_BRANCH_ID, 1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.TRUE);
        commitCollection.ensureIndex(index, options);
        MongoCommit commit = new MongoCommit();
        commit.setAffectedPaths(Arrays.asList(new String[] { "/" }));
        commit.setBaseRevisionId(0L);
        commit.setDiff(INITIAL_COMMIT_DIFF);
        commit.setMessage(INITIAL_COMMIT_MESSAGE);
        commit.setRevisionId(0L);
        commit.setPath(INITIAL_COMMIT_PATH);
        commitCollection.insert(commit);
    }

    private void initNodeCollection() {
        if (db.collectionExists(COLLECTION_NODES)){
            return;
        }
        DBCollection nodeCollection = getNodeCollection();

        DBObject index = new BasicDBObject();
        index.put(MongoNode.KEY_PATH, 1L);
        index.put(MongoNode.KEY_REVISION_ID, -1L);
        index.put(MongoNode.KEY_BRANCH_ID, 1L);

        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.TRUE);

        nodeCollection.ensureIndex(index, options);

        MongoNode root = new MongoNode();
        root.setRevisionId(0L);
        root.setPath("/");
        nodeCollection.insert(root);
    }

    private void initSyncCollection() {
        if (db.collectionExists(COLLECTION_SYNC)){
            return;
        }
        DBCollection headCollection = getSyncCollection();
        MongoSync headMongo = new MongoSync();
        headMongo.setHeadRevisionId(0L);
        headMongo.setNextRevisionId(1L);
        headCollection.insert(headMongo);
    }

    private String getBranchId(String revisionId) throws Exception {
        if (revisionId == null) {
            return null;
        }

        MongoCommit baseCommit = new FetchCommitAction(this,
                MongoUtil.toMongoRepresentation(revisionId)).execute();
        return baseCommit.getBranchId();
    }
}