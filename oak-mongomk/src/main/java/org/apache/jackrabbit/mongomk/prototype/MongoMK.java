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
package org.apache.jackrabbit.mongomk.prototype;

import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.mongomk.impl.blob.MongoBlobStore;
import org.apache.jackrabbit.mongomk.prototype.Node.Children;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;

/**
 * A MicroKernel implementation that stores the data in a MongoDB.
 */
public class MongoMK implements MicroKernel {

    /**
     * The number of documents to cache.
     */
    static final int CACHE_DOCUMENTS = Integer.getInteger("oak.mongoMK.cacheDocs", 20 * 1024);

    private static final Logger LOG = LoggerFactory.getLogger(MongoMK.class);

    /**
     * The number of child node list entries to cache.
     */
    private static final int CACHE_CHILDREN = Integer.getInteger("oak.mongoMK.cacheChildren", 1024);
    
    /**
     * The number of nodes to cache.
     */
    private static final int CACHE_NODES = Integer.getInteger("oak.mongoMK.cacheNodes", 1024);
    
    /**
     * When trying to access revisions that are older than this many milliseconds, a warning is logged.
     */
    private static final int WARN_REVISION_AGE = Integer.getInteger("oak.mongoMK.revisionAge", 10000);
    
    /**
     * The delay for asynchronous operations (delayed commit propagation and
     * cache update).
     */
    // TODO test observation with multiple Oak instances
    protected static final long ASYNC_DELAY = 1000;

    /**
     * Whether this instance is disposed.
     */
    private final AtomicBoolean isDisposed = new AtomicBoolean();

    /**
     * The MongoDB store (might be used by multiple MongoMKs).
     */
    private final DocumentStore store;

    /**
     * The MongoDB blob store.
     */
    private final BlobStore blobStore;

    /**
     * The unique cluster id, similar to the unique machine id in MongoDB.
     */
    private final int clusterId;

    /**
     * The node cache.
     *
     * Key: path@rev
     * Value: node
     */
    private final Cache<String, Node> nodeCache;

    /**
     * Child node cache.
     */
    private final Cache<String, Node.Children> nodeChildrenCache;

    /**
     * The unsaved last revisions.
     * Key: path, value: revision.
     */
    private final Map<String, Revision> unsavedLastRevisions = 
            new HashMap<String, Revision>();
    
    /**
     * The last known head revision. This is the last-known revision.
     */
    private Revision headRevision;
    
    private Thread backgroundThread;
    
    private int simpleRevisionCounter;

    /**
     * Maps branch commit revision to revision it is based on
     */
    private final Map<Revision, Revision> branchCommits
            = new ConcurrentHashMap<Revision, Revision>();

    /**
     * Create a new in-memory MongoMK used for testing.
     */
    public MongoMK() {
        this(new MemoryDocumentStore(), new MemoryBlobStore(), 0);
    }
    
    /**
     * Create a new MongoMK.
     * 
     * @param db the MongoDB connection (null for in-memory)
     * @param clusterId the cluster id (must be unique)
     */
    public MongoMK(DB db, int clusterId) {
        this(db == null ? new MemoryDocumentStore() : new MongoDocumentStore(db),
                db == null ? new MemoryBlobStore() : new MongoBlobStore(db), 
                clusterId);
    }

    /**
     * Create a new MongoMK.
     *
     * @param store the store (might be shared)
     * @param blobStore the blob store to use
     * @param clusterId the cluster id (must be unique)
     */
    public MongoMK(DocumentStore store, BlobStore blobStore, int clusterId) {
        this.store = store;
        this.blobStore = blobStore;
        this.clusterId = clusterId;

        //TODO Use size based weigher
        nodeCache = CacheBuilder.newBuilder()
                        .maximumSize(CACHE_NODES)
                        .build();

        nodeChildrenCache =  CacheBuilder.newBuilder()
                        .maximumSize(CACHE_CHILDREN)
                        .build();

        backgroundThread = new Thread(
                new BackgroundOperation(this, isDisposed),
                "MongoMK background thread");
        backgroundThread.setDaemon(true);
        backgroundThread.start();
            
        init();
    }
    
    void init() {
        headRevision = newRevision();
        Node n = readNode("/", headRevision);
        if (n == null) {
            // root node is missing: repository is not initialized
            Commit commit = new Commit(this, null, headRevision);
            n = new Node("/", headRevision);
            commit.addNode(n);
            commit.applyToDocumentStore();
        } else {
            // initialize branchCommits
            Map<String, Object> nodeMap = store.find(
                    DocumentStore.Collection.NODES, Utils.getIdFromPath("/"));
            @SuppressWarnings("unchecked")
            Map<String, String> valueMap = (Map<String, String>) nodeMap.get(UpdateOp.REVISIONS);
            if (valueMap != null) {
                for (Map.Entry<String, String> commit : valueMap.entrySet()) {
                    if (!commit.getValue().equals("true")) {
                        Revision r = Revision.fromString(commit.getKey());
                        if (r.getClusterId() == clusterId) {
                            Revision b = Revision.fromString(commit.getValue());
                            branchCommits.put(r, b);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Enable using simple revisions (just a counter). This feature is useful
     * for testing.
     */
    void useSimpleRevisions() {
        this.simpleRevisionCounter = 1;
        init();
    }
    
    /**
     * Create a new revision.
     * 
     * @return the revision
     */
    Revision newRevision() {
        if (simpleRevisionCounter > 0) {
            return new Revision(simpleRevisionCounter++, 0, clusterId);
        }
        return Revision.newRevision(clusterId);
    }
    
    void runBackgroundOperations() {
        // to be implemented
    }
    
    public void dispose() {
        if (!isDisposed.getAndSet(true)) {
            synchronized (isDisposed) {
                isDisposed.notifyAll();
            }
            try {
                backgroundThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
            store.dispose();
        }
    }

    /**
     * Get the node for the given path and revision. The returned object might
     * not be modified directly.
     *
     * @param path
     * @param rev
     * @return the node
     */
    Node getNode(String path, Revision rev) {
        checkRevisionAge(rev, path);
        String key = path + "@" + rev;
        Node node = nodeCache.getIfPresent(key);
        if (node == null) {
            node = readNode(path, rev);
            if (node != null) {
                nodeCache.put(key, node);
            }
        }
        return node;
    }
    
    private void checkRevisionAge(Revision r, String path) {
        if (headRevision.getTimestamp() - r.getTimestamp() > WARN_REVISION_AGE) {
            LOG.warn("Requesting an old revision for path " + path + ", " + 
                    ((headRevision.getTimestamp() - r.getTimestamp()) / 1000) + " seconds old");
            if (LOG.isDebugEnabled()) {
                LOG.warn("Requesting an old revision", new Exception());
            }
        }
    }
    
    private boolean includeRevision(Revision x, Revision requestRevision) {
        if (branchCommits.containsKey(x)) {
            // only include if requested revision is also a branch revision
            // with a history including x
            Revision rev = requestRevision;
            for (;;) {
                if (rev != null) {
                    if (rev.equals(x)) {
                        return true;
                    }
                } else {
                    // not part of branch identified by requestedRevision
                    return false;
                }
                rev = branchCommits.get(rev);
            }
        }
        // assert: x is not a branch commit
        while (branchCommits.containsKey(requestRevision)) {
            // reset requestRevision to branch base revision to make
            // sure we don't include revisions committed after branch
            // was created
            requestRevision = branchCommits.get(requestRevision);
        }
        if (x.getClusterId() == this.clusterId && 
                requestRevision.getClusterId() == this.clusterId) {
            // both revisions were created by this cluster node: 
            // compare timestamps only
            return requestRevision.compareRevisionTime(x) >= 0;
        }
        // TODO currently we only compare the timestamps
        return requestRevision.compareRevisionTime(x) >= 0;
    }
    
    boolean isRevisionNewer(@Nonnull Revision x, @Nonnull Revision previous) {
        // TODO currently we only compare the timestamps
        return x.compareRevisionTime(previous) > 0;
    }

    /**
     * Checks if the revision is valid for the given node map. A revision is
     * considered valid if the given node map is the root of the commit, or the
     * commit root has the revision set. This method may read further nodes to
     * perform this check.
     * This method also takes pending branches into consideration.
     * The <code>readRevision</code> identifies the read revision used by the
     * client, which may be a branch revision logged in {@link #branchCommits}.
     * The revision <code>rev</code> is valid if it is part of the branch
     * history of <code>readRevision</code>.
     *
     * @param rev     revision to check.
     * @param readRevision the read revision of the client.
     * @param nodeMap the node to check.
     * @param validRevisions set of revisions already checked against
     *                       <code>readRevision</code> and considered valid.
     * @return <code>true</code> if the revision is valid; <code>false</code>
     *         otherwise.
     */
    boolean isValidRevision(@Nonnull Revision rev,
                            @Nonnull Revision readRevision,
                            @Nonnull Map<String, Object> nodeMap,
                            @Nonnull Set<Revision> validRevisions) {
        if (validRevisions.contains(rev)) {
            return true;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> revisions = (Map<String, String>) nodeMap.get(UpdateOp.REVISIONS);
        if (isCommitted(rev, readRevision, revisions)) {
            validRevisions.add(rev);
            return true;
        }
        // check commit root
        @SuppressWarnings("unchecked")
        Map<String, Integer> commitRoot = (Map<String, Integer>) nodeMap.get(UpdateOp.COMMIT_ROOT);
        String commitRootPath = null;
        if (commitRoot != null) {
            Integer depth = commitRoot.get(rev.toString());
            if (depth != null) {
                String p = Utils.getPathFromId((String) nodeMap.get(UpdateOp.ID));
                commitRootPath = PathUtils.getAncestorPath(p, PathUtils.getDepth(p) - depth);
            }
        }
        if (commitRootPath == null) {
            // shouldn't happen, either node is commit root for a revision
            // or has a reference to the commit root
            LOG.warn("Node {} does not have commit root reference for revision {}",
                    nodeMap.get(UpdateOp.ID), rev);
            LOG.warn(nodeMap.toString());
            return false;
        }
        // get root of commit
        nodeMap = store.find(DocumentStore.Collection.NODES, Utils.getIdFromPath(commitRootPath));
        if (nodeMap == null) {
            return false;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> rootRevisions = (Map<String, String>) nodeMap.get(UpdateOp.REVISIONS);
        if (isCommitted(rev, readRevision, rootRevisions)) {
            validRevisions.add(rev);
            return true;
        }
        return false;
    }

    /**
     * Returns <code>true</code> if the given revision is set to committed in
     * the revisions map. That is, the revision exists in the map and the string
     * value is <code>"true"</code> or equals the <code>readRevision</code>.
     *
     * @param revision  the revision to check.
     * @param readRevision the read revision.
     * @param revisions the revisions map, or <code>null</code> if none is set.
     * @return <code>true</code> if the revision is committed, otherwise
     *         <code>false</code>.
     */
    private boolean isCommitted(@Nonnull Revision revision,
                                @Nonnull Revision readRevision,
                                @Nullable Map<String, String> revisions) {
        if (revision.equals(readRevision)) {
            return true;
        }
        if (revisions == null) {
            return false;
        }
        String value = revisions.get(revision.toString());
        if (value == null) {
            return false;
        }
        if (value.equals("true") && !branchCommits.containsKey(readRevision)) {
            return true;
        }
        return includeRevision(revision, readRevision);
    }

    public Children getChildren(String path, Revision rev, int limit) {
        checkRevisionAge(rev, path);
        String key = path + "@" + rev;
        Children children = nodeChildrenCache.getIfPresent(key);
        if (children == null) {
            children = readChildren(path, rev, limit);
            if (children != null) {
                nodeChildrenCache.put(key, children);
            }
        }
        return children;        
    }
    
    Node.Children readChildren(String path, Revision rev, int limit) {
        String from = PathUtils.concat(path, "a");
        from = Utils.getIdFromPath(from);
        from = from.substring(0, from.length() - 1);
        String to = PathUtils.concat(path, "z");
        to = Utils.getIdFromPath(to);
        to = to.substring(0, to.length() - 2) + "0";
        List<Map<String, Object>> list = store.query(DocumentStore.Collection.NODES, from, to, limit);
        Children c = new Children(path, rev);
        Set<Revision> validRevisions = new HashSet<Revision>();
        for (Map<String, Object> e : list) {
            // filter out deleted children
            if (getLiveRevision(e, rev, validRevisions) == null) {
                continue;
            }
            // TODO put the whole node in the cache
            String id = e.get(UpdateOp.ID).toString();
            String p = Utils.getPathFromId(id);
            c.children.add(p);
        }
        return c;
    }

    private Node readNode(String path, Revision rev) {
        String id = Utils.getIdFromPath(path);
        Map<String, Object> map = store.find(DocumentStore.Collection.NODES, id);
        if (map == null) {
            return null;
        }
        Revision min = getLiveRevision(map, rev);
        if (min == null) {
            // deleted
            return null;
        }
        Node n = new Node(path, rev);
        Revision lastRevision = null;
        Revision revision =  unsavedLastRevisions.get(path);
        if (revision != null) {
            if (isRevisionNewer(revision, rev)) {
                // at most the read revision
                revision = rev;
            }
            lastRevision = revision;
        }
        for (String key : map.keySet()) {
            if (key.equals(UpdateOp.LAST_REV)) {
                Object v = map.get(key);
                @SuppressWarnings("unchecked")
                Map<String, String> valueMap = (Map<String, String>) v;
                for (String r : valueMap.keySet()) {
                    revision = Revision.fromString(valueMap.get(r));
                    if (isRevisionNewer(revision, rev)) {
                        // at most the read revision
                        revision = rev;
                    }
                    if (lastRevision == null || isRevisionNewer(revision, lastRevision)) {
                        lastRevision = revision;
                    }
                }
            }
            if (!Utils.isPropertyName(key)) {
                continue;
            }
            Object v = map.get(key);
            @SuppressWarnings("unchecked")
            Map<String, String> valueMap = (Map<String, String>) v;
            if (valueMap != null) {
                String value = getLatestValue(valueMap, min, rev);
                String propertyName = Utils.unescapePropertyName(key);
                n.setProperty(propertyName, value);
            }
        }
        n.setLastRevision(lastRevision);
        return n;
    }
    
    /**
     * Get the latest property value that is larger or equal the min revision,
     * and smaller or equal the max revision.
     * 
     * @param valueMap the revision-value map
     * @param min the minimum revision (null meaning unlimited)
     * @param max the maximum revision
     * @return the value, or null if not found
     */
    private String getLatestValue(Map<String, String> valueMap, Revision min, Revision max) {
        String value = null;
        Revision latestRev = null;
        for (String r : valueMap.keySet()) {
            Revision propRev = Revision.fromString(r);
            if (min != null) {
                if (isRevisionNewer(min, propRev)) {
                    continue;
                }
            }
            if (includeRevision(propRev, max)) {
                if (latestRev == null || isRevisionNewer(propRev, latestRev)) {
                    latestRev = propRev;
                    value = valueMap.get(r);
                }
            }
        }
        return value;
    }

    @Override
    public String getHeadRevision() throws MicroKernelException {
        return headRevision.toString();
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path)
            throws MicroKernelException {
        // not currently called by oak-core
        throw new MicroKernelException("Not implemented");
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout)
            throws MicroKernelException, InterruptedException {
        // not currently called by oak-core
        throw new MicroKernelException("Not implemented");
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId,
            String path) throws MicroKernelException {
        // not currently called by oak-core
        throw new MicroKernelException("Not implemented");
    }

    @Override
    public String diff(String fromRevisionId, String toRevisionId, String path,
            int depth) throws MicroKernelException {
        if (fromRevisionId.equals(toRevisionId)) {
            return "";
        }
        if (depth != 0) {
            throw new MicroKernelException("Only depth 0 is supported, depth is " + depth);
        }
        fromRevisionId = stripBranchRevMarker(fromRevisionId);
        toRevisionId = stripBranchRevMarker(toRevisionId);
        Node from = getNode(path, Revision.fromString(fromRevisionId));
        Node to = getNode(path, Revision.fromString(toRevisionId));
        if (from == null || to == null) {
            // TODO implement correct behavior if the node does't/didn't exist
            throw new MicroKernelException("Diff is only supported if the node exists in both cases");
        }
        JsopWriter w = new JsopStream();
        for (String p : from.getPropertyNames()) {
            // changed or removed properties
            String fromValue = from.getProperty(p);
            String toValue = to.getProperty(p);
            if (!fromValue.equals(toValue)) {
                w.tag('^').key(p).value(toValue).newline();
            }
        }
        for (String p : to.getPropertyNames()) {
            // added properties
            if (from.getProperty(p) == null) {
                w.tag('^').key(p).value(to.getProperty(p)).newline();
            }
        }
        Revision fromRev = Revision.fromString(fromRevisionId);
        Revision toRev = Revision.fromString(toRevisionId);
        // TODO this does not work well for large child node lists 
        // use a MongoDB index instead
        Children fromChildren = getChildren(path, fromRev, Integer.MAX_VALUE);
        Children toChildren = getChildren(path, toRev, Integer.MAX_VALUE);
        Set<String> childrenSet = new HashSet<String>(toChildren.children);
        for (String n : fromChildren.children) {
            if (!childrenSet.contains(n)) {
                w.tag('-').key(n).newline();
            } else {
                Node n1 = getNode(n, fromRev);
                Node n2 = getNode(n, toRev);
                // this is not fully correct:
                // a change is detected if the node changed recently,
                // even if the revisions are well in the past
                // if this is a problem it would need to be changed
                if (!n1.getId().equals(n2.getId())) {
                    w.tag('^').key(n).object().endObject().newline();
                }
            }
        }
        childrenSet = new HashSet<String>(fromChildren.children);
        for (String n : toChildren.children) {
            if (!childrenSet.contains(n)) {
                w.tag('+').key(n).object().endObject().newline();
            }
        }
        return w.toString();
    }

    @Override
    public synchronized boolean nodeExists(String path, String revisionId)
            throws MicroKernelException {
        revisionId = revisionId != null ? revisionId : headRevision.toString();
        Revision rev = Revision.fromString(stripBranchRevMarker(revisionId));
        Node n = getNode(path, rev);
        return n != null;
    }

    @Override
    public long getChildNodeCount(String path, String revisionId)
            throws MicroKernelException {
        // not currently called by oak-core
        throw new MicroKernelException("Not implemented");
    }

    @Override
    public synchronized String getNodes(String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws MicroKernelException {
        if (depth != 0) {
            throw new MicroKernelException("Only depth 0 is supported, depth is " + depth);
        }
        revisionId = revisionId != null ? revisionId : headRevision.toString();
        if (revisionId.startsWith("b")) {
            // reading from the branch is reading from the trunk currently
            revisionId = revisionId.substring(1).replace('+', ' ').trim();
        }
        Revision rev = Revision.fromString(revisionId);
        Node n = getNode(path, rev);
        if (n == null) {
            return null;
            // throw new MicroKernelException("Node not found at path " + path);
        }
        JsopStream json = new JsopStream();
        boolean includeId = filter != null && filter.contains(":id");
        includeId |= filter != null && filter.contains(":hash");
        json.object();
        n.append(json, includeId);
        // FIXME: must not read all children!
        Children c = getChildren(path, rev, Integer.MAX_VALUE);
        for (long i = offset; i < c.children.size(); i++) {
            if (maxChildNodes-- <= 0) {
                break;
            }
            String name = PathUtils.getName(c.children.get((int) i));
            json.key(name).object().endObject();
        }
        json.key(":childNodeCount").value(c.children.size());
        json.endObject();
        String result = json.toString();
        // if (filter != null && filter.contains(":hash")) {
        //     result = result.replaceAll("\":id\"", "\":hash\"");
        // }
        return result;
    }

    @Override
    public synchronized String commit(String rootPath, String json, String baseRevId,
            String message) throws MicroKernelException {
        Revision baseRev;
        if (baseRevId == null) {
            baseRev = headRevision;
            baseRevId = baseRev.toString();
        } else {
            baseRev = Revision.fromString(stripBranchRevMarker(baseRevId));
        }
        JsopReader t = new JsopTokenizer(json);
        Revision rev = newRevision();
        Commit commit = new Commit(this, baseRev, rev);
        while (true) {
            int r = t.read();
            if (r == JsopReader.END) {
                break;
            }
            String path = PathUtils.concat(rootPath, t.readString());
            switch (r) {
            case '+':
                t.read(':');
                t.read('{');
                parseAddNode(commit, t, path);
                break;
            case '-':
                commit.removeNode(path);
                markAsDeleted(path, commit, true);
                commit.removeNodeDiff(path);
                break;
            case '^':
                t.read(':');
                String value;
                if (t.matches(JsopReader.NULL)) {
                    value = null;
                    commit.getDiff().tag('^').key(path).value(null);
                } else {
                    value = t.readRawValue().trim();
                    commit.getDiff().tag('^').key(path).value(value);
                }
                String p = PathUtils.getParentPath(path);
                String propertyName = PathUtils.getName(path);
                commit.updateProperty(p, propertyName, value);
                commit.updatePropertyDiff(p, propertyName, value);
                break;
            case '>': {
                // TODO support moving nodes that were modified within this commit
                t.read(':');
                String sourcePath = path;
                String targetPath = t.readString();
                if (!PathUtils.isAbsolute(targetPath)) {
                    targetPath = PathUtils.concat(rootPath, targetPath);
                }
                if (!nodeExists(sourcePath, baseRevId)) {
                    throw new MicroKernelException("Node not found: " + sourcePath + " in revision " + baseRevId);
                }
                if (nodeExists(targetPath, baseRevId)) {
                    throw new MicroKernelException("Node already exists: " + targetPath + " in revision " + baseRevId);
                }
                commit.moveNode(sourcePath, targetPath);
                moveNode(sourcePath, targetPath, baseRev, commit);
                break;
            }
            case '*': {
                // TODO support copying nodes that were modified within this commit
                t.read(':');
                String sourcePath = path;
                String targetPath = t.readString();
                if (!PathUtils.isAbsolute(targetPath)) {
                    targetPath = PathUtils.concat(rootPath, targetPath);
                }
                if (!nodeExists(sourcePath, baseRevId)) {
                    throw new MicroKernelException("Node not found: " + sourcePath + " in revision " + baseRevId);
                }
                commit.copyNode(sourcePath, targetPath);
                copyNode(sourcePath, targetPath, baseRev, commit);
                break;
            }
            default:
                throw new MicroKernelException("token: " + (char) t.getTokenType());
            }
        }
        if (baseRevId.startsWith("b")) {
            // remember branch commit
            branchCommits.put(rev, baseRev);
            boolean success = false;
            try {
                // prepare commit
                commit.prepare(baseRev);
                success = true;
            } finally {
                if (!success) {
                    branchCommits.remove(rev);
                }
            }

            return "b" + rev.toString();

            // String jsonBranch = branchCommits.remove(revisionId);
            // jsonBranch += commit.getDiff().toString();
            // String branchRev = revisionId + "+";
            // branchCommits.put(branchRev, jsonBranch);
            // return branchRev;
        }
        commit.apply();
        headRevision = commit.getRevision();
        return rev.toString();
    }

    private void copyNode(String sourcePath, String targetPath, Revision baseRev, Commit commit) {
        moveOrCopyNode(false, sourcePath, targetPath, baseRev, commit);
    }
    
    private void moveNode(String sourcePath, String targetPath, Revision baseRev, Commit commit) {
        moveOrCopyNode(true, sourcePath, targetPath, baseRev, commit);
    }
    
    private void moveOrCopyNode(boolean move,
                                String sourcePath,
                                String targetPath,
                                Revision baseRev,
                                Commit commit) {
        // TODO Optimize - Move logic would not work well with very move of very large subtrees
        // At minimum we can optimize by traversing breadth wise and collect node id
        // and fetch them via '$in' queries

        // TODO Transient Node - Current logic does not account for operations which are part
        // of this commit i.e. transient nodes. If its required it would need to be looked
        // into

        Node n = getNode(sourcePath, baseRev);

        // Node might be deleted already
        if (n == null) {
            return;
        }

        Node newNode = new Node(targetPath, commit.getRevision());
        n.copyTo(newNode);

        commit.addNode(newNode);
        if (move) {
            markAsDeleted(sourcePath, commit, false);
        }
        Node.Children c = getChildren(sourcePath, baseRev, Integer.MAX_VALUE);
        for (String srcChildPath : c.children) {
            String childName = PathUtils.getName(srcChildPath);
            String destChildPath = PathUtils.concat(targetPath, childName);
            moveOrCopyNode(move, srcChildPath, destChildPath, baseRev, commit);
        }
    }

    private void markAsDeleted(String path, Commit commit, boolean subTreeAlso) {
        Revision rev = commit.getRevision();
        commit.removeNode(path);

        if (subTreeAlso) {

            // recurse down the tree
            // TODO causes issue with large number of children
            Node n = getNode(path, rev);

            // remove from the cache
            nodeCache.invalidate(path + "@" + rev);
            
            if (n != null) {
                Node.Children c = getChildren(path, rev,
                        Integer.MAX_VALUE);
                for (String childPath : c.children) {
                    markAsDeleted(childPath, commit, true);
                }
                nodeChildrenCache.invalidate(n.getId());
            }
        }

        // Remove the node from the cache
        nodeCache.invalidate(path + "@" + rev);
    }

    /**
     * Get the latest revision where the node was alive at or before the
     * provided revision.
     *
     * @param nodeMap the node map
     * @param maxRev the maximum revision to return
     * @return the earliest revision, or null if the node is deleted at the
     *         given revision
     */
    private Revision getLiveRevision(Map<String, Object> nodeMap,
                                     Revision maxRev) {
        return getLiveRevision(nodeMap, maxRev, new HashSet<Revision>());
    }

    /**
    * Get the latest revision where the node was alive at or before the
    * provided revision.
    *
    * @param nodeMap the node map
    * @param maxRev the maximum revision to return
    * @param validRevisions the set of revisions already checked against
     *                      maxRev and considered valid.
    * @return the earliest revision, or null if the node is deleted at the
    *         given revision
    */
    private Revision getLiveRevision(Map<String, Object> nodeMap,
            Revision maxRev, Set<Revision> validRevisions) {
        @SuppressWarnings("unchecked")
        Map<String, String> valueMap = (Map<String, String>) nodeMap
                .get(UpdateOp.DELETED);
        Revision firstRev = null;
        String value = null;
        if (valueMap == null) {
            return null;
        }
        for (String r : valueMap.keySet()) {
            Revision propRev = Revision.fromString(r);
            if (isRevisionNewer(propRev, maxRev)
                    || !isValidRevision(propRev, maxRev, nodeMap, validRevisions)) {
                continue;
            }
            if (firstRev == null || isRevisionNewer(propRev, firstRev)) {
                firstRev = propRev;
                value = valueMap.get(r);
            }
        }
        if ("true".equals(value)) {
            return null;
        }
        return firstRev;
    }
    
    /**
     * Get the revision of the latest change made to this node.
     * 
     * @param nodeMap the document
     * @param before the returned value is guaranteed to be older than this revision
     * @param onlyCommitted whether only committed changes should be considered
     * @return the revision, or null if deleted
     */
    @Nullable Revision getNewestRevision(Map<String, Object> nodeMap,
                                         Revision before, boolean onlyCommitted) {
        if (nodeMap == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> valueMap = (Map<String, String>) nodeMap
                .get(UpdateOp.DELETED);
        if (valueMap == null) {
            return null;
        }
        Revision newestRev = null;
        String newestValue = null;
        for (String r : valueMap.keySet()) {
            Revision propRev = Revision.fromString(r);
            if (newestRev == null || isRevisionNewer(propRev, newestRev)) {
                if (isRevisionNewer(before, propRev)) {
                    if (!onlyCommitted
                            || isValidRevision(propRev, before,
                                nodeMap, new HashSet<Revision>())) {
                        newestRev = propRev;
                        newestValue = valueMap.get(r);
                    }
                }
            }
        }
        if ("true".equals(newestValue)) {
            // deleted in the newest revision
            return null;
        }
        return newestRev;
    }
    
    private static String stripBranchRevMarker(String revisionId) {
        if (revisionId.startsWith("b")) {
            return revisionId.substring(1);
        }
        return revisionId;
    }
    
    public static void parseAddNode(Commit commit, JsopReader t, String path) {
        Node n = new Node(path, commit.getRevision());
        if (!t.matches('}')) {
            do {
                String key = t.readString();
                t.read(':');
                if (t.matches('{')) {
                    String childPath = PathUtils.concat(path, key);
                    parseAddNode(commit, t, childPath);
                } else {
                    String value = t.readRawValue().trim();
                    n.setProperty(key, value);
                }
            } while (t.matches(','));
            t.read('}');
        }
        commit.addNode(n);
        commit.addNodeDiff(n);
    }

    @Override
    public String branch(@Nullable String trunkRevisionId) throws MicroKernelException {
        // nothing is written when the branch is created, the returned
        // revision simply acts as a reference to the branch base revision
        String revisionId = trunkRevisionId != null ? trunkRevisionId : headRevision.toString();
        return "b" + revisionId;
    }

    @Override
    public String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        // TODO improve implementation if needed
        if (!branchRevisionId.startsWith("b")) {
            throw new MicroKernelException("Not a branch: " + branchRevisionId);
        }

        // reading from the branch is reading from the trunk currently
        String revisionId = branchRevisionId.substring(1).replace('+', ' ').trim();
        // make branch commits visible
        List<Revision> branchRevisions = new ArrayList<Revision>();
        UpdateOp op = new UpdateOp("/", Utils.getIdFromPath("/"), false);
        Revision baseRevId = Revision.fromString(revisionId);
        while (baseRevId != null) {
            branchRevisions.add(baseRevId);
            op.set(UpdateOp.REVISIONS + "." + baseRevId, "true");
            baseRevId = branchCommits.get(baseRevId);
        }
        store.createOrUpdate(DocumentStore.Collection.NODES, op);
        // remove from branchCommits map after successful update
        for (Revision r : branchRevisions) {
            branchCommits.remove(r);
        }
        headRevision = newRevision();
        return headRevision.toString();

        // TODO improve implementation if needed
        // if (!branchRevisionId.startsWith("b")) {
        //     throw new MicroKernelException("Not a branch: " + branchRevisionId);
        // }
        //
        // String commit = branchCommits.remove(branchRevisionId);
        // return commit("", commit, null, null);
    }

    @Override
    @Nonnull
    public String rebase(String branchRevisionId, String newBaseRevisionId)
            throws MicroKernelException {
        // TODO improve implementation if needed
        return branchRevisionId;
    }

    @Override
    public long getLength(String blobId) throws MicroKernelException {
        try {
            return blobStore.getBlobLength(blobId);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length)
            throws MicroKernelException {
        try {
            return blobStore.readBlob(blobId, pos, buff, off, length);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public String write(InputStream in) throws MicroKernelException {
        try {
            return blobStore.writeBlob(in);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public DocumentStore getDocumentStore() {
        return store;
    }
    
    /**
     * A background thread.
     */
    static class BackgroundOperation implements Runnable {
        final WeakReference<MongoMK> ref;
        private final AtomicBoolean isDisposed;
        BackgroundOperation(MongoMK mk, AtomicBoolean isDisposed) {
            ref = new WeakReference<MongoMK>(mk);
            this.isDisposed = isDisposed;
        }
        public void run() {
            while (!isDisposed.get()) {
                synchronized (isDisposed) {
                    try {
                        isDisposed.wait(ASYNC_DELAY);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                MongoMK mk = ref.get();
                if (mk != null) {
                    mk.runBackgroundOperations();
                }
            }
        }
    }

    public void applyChanges(Revision rev, String path, 
            boolean isNew, boolean isDelete, boolean isWritten, 
            ArrayList<String> added, ArrayList<String> removed) {
        if (!isWritten) {
            unsavedLastRevisions.put(path, rev);
        } else {
            unsavedLastRevisions.remove(path);
        }
        Children c = nodeChildrenCache.getIfPresent(path + "@" + rev);
        if (isNew || (!isDelete && c != null)) {
            String key = path + "@" + rev;
            Children c2 = new Children(path, rev);
            TreeSet<String> set = new TreeSet<String>();
            if (c != null) {
                set.addAll(c.children);
            }
            set.removeAll(removed);
            set.addAll(added);
            c2.children.addAll(set);
            nodeChildrenCache.put(key, c2);
        }
    }
    
}
