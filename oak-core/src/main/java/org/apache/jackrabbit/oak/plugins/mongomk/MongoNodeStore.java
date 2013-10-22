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
package org.apache.jackrabbit.oak.plugins.mongomk;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.mongomk.util.LoggingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.mongomk.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a NodeStore on MongoDB.
 */
public final class MongoNodeStore implements NodeStore, RevisionContext {

    private static final Logger LOG = LoggerFactory.getLogger(MongoNodeStore.class);

    /**
     * Do not cache more than this number of children for a document.
     */
    private static final int NUM_CHILDREN_CACHE_LIMIT = Integer.getInteger("oak.mongoMK.childrenCacheLimit", 16 * 1024);

    /**
     * When trying to access revisions that are older than this many
     * milliseconds, a warning is logged. The default is one minute.
     */
    private static final int WARN_REVISION_AGE =
            Integer.getInteger("oak.mongoMK.revisionAge", 60 * 1000);

    /**
     * Enable background operations
     */
    private static final boolean ENABLE_BACKGROUND_OPS = Boolean.parseBoolean(System.getProperty("oak.mongoMK.backgroundOps", "true"));

    /**
     * How long to remember the relative order of old revision of all cluster
     * nodes, in milliseconds. The default is one hour.
     */
    private static final int REMEMBER_REVISION_ORDER_MILLIS = 60 * 60 * 1000;

    /**
     * The MongoDB store (might be used by multiple MongoMKs).
     */
    protected final DocumentStore store;

    /**
     * Whether this instance is disposed.
     */
    private final AtomicBoolean isDisposed = new AtomicBoolean();

    /**
     * The delay for asynchronous operations (delayed commit propagation and
     * cache update).
     */
    protected int asyncDelay = 1000;

    /**
     * The cluster instance info.
     */
    private final ClusterNodeInfo clusterNodeInfo;

    /**
     * The unique cluster id, similar to the unique machine id in MongoDB.
     */
    private final int clusterId;

    /**
     * The comparator for revisions.
     */
    private final Revision.RevisionComparator revisionComparator;

    /**
     * Unmerged branches of this MongoNodeStore instance.
     */
    // TODO at some point, open (unmerged) branches
    // need to be garbage collected (in-memory and on disk)
    private final UnmergedBranches branches;

    /**
     * The unsaved last revisions. This contains the parents of all changed
     * nodes, once those nodes are committed but the parent node itself wasn't
     * committed yet. The parents are not immediately persisted as this would
     * cause each commit to change all parents (including the root node), which
     * would limit write scalability.
     *
     * Key: path, value: revision.
     */
    private final UnsavedModifications unsavedLastRevisions = new UnsavedModifications();

    /**
     * Set of IDs for documents that may need to be split.
     */
    private final Map<String, String> splitCandidates = Maps.newConcurrentMap();

    /**
     * The splitting point in milliseconds. If a document is split, revisions
     * older than this number of milliseconds are moved to a different document.
     * The default is 0, meaning documents are never split. Revisions that are
     * newer than this are kept in the newest document.
     */
    private final long splitDocumentAgeMillis;

    /**
     * The last known revision for each cluster instance.
     *
     * Key: the machine id, value: revision.
     */
    private final Map<Integer, Revision> lastKnownRevision =
            new ConcurrentHashMap<Integer, Revision>();

    /**
     * The last known head revision. This is the last-known revision.
     */
    private volatile Revision headRevision;

    private Thread backgroundThread;

    /**
     * Enable using simple revisions (just a counter). This feature is useful
     * for testing.
     */
    private AtomicInteger simpleRevisionCounter;

    private boolean stopBackground;

    /**
     * The node cache.
     *
     * Key: path@rev, value: node
     */
    private final Cache<String, Node> nodeCache;
    private final CacheStats nodeCacheStats;

    /**
     * Child node cache.
     *
     * Key: path@rev, value: children
     */
    private final Cache<String, Node.Children> nodeChildrenCache;
    private final CacheStats nodeChildrenCacheStats;

    /**
     * Child doc cache.
     */
    private final Cache<String, NodeDocument.Children> docChildrenCache;
    private final CacheStats docChildrenCacheStats;

    public MongoNodeStore(MongoMK.Builder builder) {
        if (builder.isUseSimpleRevision()) {
            this.simpleRevisionCounter = new AtomicInteger(0);
        }
        DocumentStore s = builder.getDocumentStore();
        if (builder.getTiming()) {
            s = new TimingDocumentStoreWrapper(s);
        }
        if (builder.getLogging()) {
            s = new LoggingDocumentStoreWrapper(s);
        }
        this.store = s;
        int cid = builder.getClusterId();
        cid = Integer.getInteger("oak.mongoMK.clusterId", cid);
        if (cid == 0) {
            clusterNodeInfo = ClusterNodeInfo.getInstance(store);
            // TODO we should ensure revisions generated from now on
            // are never "older" than revisions already in the repository for
            // this cluster id
            cid = clusterNodeInfo.getId();
        } else {
            clusterNodeInfo = null;
        }
        this.clusterId = cid;
        this.revisionComparator = new Revision.RevisionComparator(clusterId);
        this.branches = new UnmergedBranches(getRevisionComparator());
        this.splitDocumentAgeMillis = builder.getSplitDocumentAgeMillis();
        this.asyncDelay = builder.getAsyncDelay();

        //TODO Make stats collection configurable as it add slight overhead

        nodeCache = builder.buildCache(builder.getNodeCacheSize());
        nodeCacheStats = new CacheStats(nodeCache, "MongoMk-Node",
                builder.getWeigher(), builder.getNodeCacheSize());

        nodeChildrenCache = builder.buildCache(builder.getChildrenCacheSize());
        nodeChildrenCacheStats = new CacheStats(nodeChildrenCache, "MongoMk-NodeChildren",
                builder.getWeigher(), builder.getChildrenCacheSize());

        docChildrenCache = builder.buildCache(builder.getDocChildrenCacheSize());
        docChildrenCacheStats = new CacheStats(docChildrenCache, "MongoMk-DocChildren",
                builder.getWeigher(), builder.getDocChildrenCacheSize());

        init();
        // initial reading of the revisions of other cluster nodes
        backgroundRead();
        getRevisionComparator().add(headRevision, Revision.newRevision(0));
        headRevision = newRevision();
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
            branches.init(store, this);
        }
        backgroundThread = new Thread(
                new BackgroundOperation(this, isDisposed),
                "MongoMK background thread");
        backgroundThread.setDaemon(true);
        backgroundThread.start();
    }

    void dispose() {
        // force background write (with asyncDelay > 0, the root wouldn't be written)
        // TODO make this more obvious / explicit
        // TODO tests should also work if this is not done
        asyncDelay = 0;
        runBackgroundOperations();
        if (!isDisposed.getAndSet(true)) {
            synchronized (isDisposed) {
                isDisposed.notifyAll();
            }
            try {
                backgroundThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
            if (clusterNodeInfo != null) {
                clusterNodeInfo.dispose();
            }
            store.dispose();
            LOG.info("Disposed MongoMK with clusterNodeId: {}", clusterId);
        }
    }

    public void stopBackground() {
        stopBackground = true;
    }

    @Nonnull
    Revision getHeadRevision() {
        return headRevision;
    }

    Revision setHeadRevision(Revision newHead) {
        Revision previous = headRevision;
        headRevision = newHead;
        return previous;
    }

    @Nonnull
    DocumentStore getDocumentStore() {
        return store;
    }

    /**
     * Create a new revision.
     *
     * @return the revision
     */
    @Nonnull
    Revision newRevision() {
        if (simpleRevisionCounter != null) {
            return new Revision(simpleRevisionCounter.getAndIncrement(), 0, clusterId);
        }
        return Revision.newRevision(clusterId);
    }

    public void setAsyncDelay(int delay) {
        this.asyncDelay = delay;
    }

    public int getAsyncDelay() {
        return asyncDelay;
    }

    public ClusterNodeInfo getClusterInfo() {
        return clusterNodeInfo;
    }

    public CacheStats getNodeCacheStats() {
        return nodeCacheStats;
    }

    public CacheStats getNodeChildrenCacheStats() {
        return nodeChildrenCacheStats;
    }

    public CacheStats getDocChildrenCacheStats() {
        return docChildrenCacheStats;
    }

    public int getPendingWriteCount() {
        return unsavedLastRevisions.getPaths().size();
    }

    public long getSplitDocumentAgeMillis() {
        return this.splitDocumentAgeMillis;
    }

    /**
     * Checks that revision x is newer than another revision.
     *
     * @param x the revision to check
     * @param previous the presumed earlier revision
     * @return true if x is newer
     */
    boolean isRevisionNewer(@Nonnull Revision x, @Nonnull Revision previous) {
        return getRevisionComparator().compare(x, previous) > 0;
    }

    /**
     * Enqueue the document with the given id as a split candidate.
     *
     * @param id the id of the document to check if it needs to be split.
     */
    void addSplitCandidate(String id) {
        splitCandidates.put(id, id);
    }

    void copyNode(String sourcePath, String targetPath, Commit commit) {
        moveOrCopyNode(false, sourcePath, targetPath, commit);
    }

    void moveNode(String sourcePath, String targetPath, Commit commit) {
        moveOrCopyNode(true, sourcePath, targetPath, commit);
    }

    void markAsDeleted(String path, Commit commit, boolean subTreeAlso) {
        Revision rev = commit.getBaseRevision();
        checkState(rev != null, "Base revision of commit must not be null");
        commit.removeNode(path);

        if (subTreeAlso) {
            // recurse down the tree
            // TODO causes issue with large number of children
            Node n = getNode(path, rev);

            if (n != null) {
                Node.Children c = getChildren(path, rev, Integer.MAX_VALUE);
                for (String childPath : c.children) {
                    markAsDeleted(childPath, commit, true);
                }
            }
        }
    }

    /**
     * Get the node for the given path and revision. The returned object might
     * not be modified directly.
     *
     * @param path the path of the node.
     * @param rev the read revision.
     * @return the node or <code>null</code> if the node does not exist at the
     *          given revision.
     */
    @CheckForNull
    Node getNode(final @Nonnull String path, final @Nonnull Revision rev) {
        checkRevisionAge(checkNotNull(rev), checkNotNull(path));
        try {
            String key = path + "@" + rev;
            Node node = nodeCache.get(key, new Callable<Node>() {
                @Override
                public Node call() throws Exception {
                    Node n = readNode(path, rev);
                    if (n == null) {
                        n = Node.MISSING;
                    }
                    return n;
                }
            });
            return node == Node.MISSING ? null : node;
        } catch (ExecutionException e) {
            throw new MicroKernelException(e);
        }
    }

    public Node.Children getChildren(final String path, final Revision rev, final int limit)  throws
            MicroKernelException {
        checkRevisionAge(rev, path);
        String key = path + "@" + rev;
        Node.Children children;
        try {
            children = nodeChildrenCache.get(key, new Callable<Node.Children>() {
                @Override
                public Node.Children call() throws Exception {
                    return readChildren(path, rev, limit);
                }
            });
        } catch (ExecutionException e) {
            throw new MicroKernelException("Error occurred while fetching children nodes for path "+path, e);
        }

        //In case the limit > cached children size and there are more child nodes
        //available then refresh the cache
        if (children.hasMore) {
            if (limit > children.children.size()) {
                children = readChildren(path, rev, limit);
                if (children != null) {
                    nodeChildrenCache.put(key, children);
                }
            }
        }
        return children;
    }

    Node.Children readChildren(String path, Revision rev, int limit) {
        // TODO use offset, to avoid O(n^2) and running out of memory
        // to do that, use the *name* of the last entry of the previous batch of children
        // as the starting point
        Iterable<NodeDocument> docs;
        Node.Children c = new Node.Children();
        int rawLimit = limit;
        Set<Revision> validRevisions = new HashSet<Revision>();
        do {
            c.children.clear();
            c.hasMore = true;
            docs = readChildren(path, rawLimit);
            int numReturned = 0;
            for (NodeDocument doc : docs) {
                numReturned++;
                // filter out deleted children
                if (doc.isDeleted(this, rev, validRevisions)) {
                    continue;
                }
                String p = Utils.getPathFromId(doc.getId());
                if (c.children.size() < limit) {
                    // add to children until limit is reached
                    c.children.add(p);
                }
            }
            if (numReturned < rawLimit) {
                // fewer documents returned than requested
                // -> no more documents
                c.hasMore = false;
            }
            // double rawLimit for next round
            rawLimit = (int) Math.min(((long) rawLimit) * 2, Integer.MAX_VALUE);
        } while (c.children.size() < limit && c.hasMore);
        return c;
    }

    @Nonnull
    Iterable<NodeDocument> readChildren(final String path, int limit) {
        String from = Utils.getKeyLowerLimit(path);
        String to = Utils.getKeyUpperLimit(path);
        if (limit > NUM_CHILDREN_CACHE_LIMIT) {
            // do not use cache
            return store.query(Collection.NODES, from, to, limit);
        }
        // check cache
        NodeDocument.Children c = docChildrenCache.getIfPresent(path);
        if (c == null) {
            c = new NodeDocument.Children();
            List<NodeDocument> docs = store.query(Collection.NODES, from, to, limit);
            for (NodeDocument doc : docs) {
                String p = Utils.getPathFromId(doc.getId());
                c.childNames.add(PathUtils.getName(p));
            }
            c.isComplete = docs.size() < limit;
            docChildrenCache.put(path, c);
        } else if (c.childNames.size() < limit && !c.isComplete) {
            // fetch more and update cache
            String lastName = c.childNames.get(c.childNames.size() - 1);
            String lastPath = PathUtils.concat(path, lastName);
            from = Utils.getIdFromPath(lastPath);
            int remainingLimit = limit - c.childNames.size();
            List<NodeDocument> docs = store.query(Collection.NODES,
                    from, to, remainingLimit);
            NodeDocument.Children clone = c.clone();
            for (NodeDocument doc : docs) {
                String p = Utils.getPathFromId(doc.getId());
                clone.childNames.add(PathUtils.getName(p));
            }
            clone.isComplete = docs.size() < remainingLimit;
            docChildrenCache.put(path, clone);
            c = clone;
        }
        Iterable<NodeDocument> it = Iterables.transform(c.childNames, 
                new Function<String, NodeDocument>() {
            @Override
            public NodeDocument apply(String name) {
                String p = PathUtils.concat(path, name);
                return store.find(Collection.NODES, Utils.getIdFromPath(p));
            }
        });
        if (c.childNames.size() > limit * 2) {
            it = Iterables.limit(it, limit * 2);
        }
        return it;
    }

    @CheckForNull
    private Node readNode(String path, Revision readRevision) {
        String id = Utils.getIdFromPath(path);
        Revision lastRevision = getPendingModifications().get(path);
        NodeDocument doc = store.find(Collection.NODES, id);
        if (doc == null) {
            return null;
        }
        return doc.getNodeAtRevision(this, readRevision, lastRevision);
    }

    /**
     * Apply the changes of a node to the cache.
     *
     * @param rev the revision
     * @param path the path
     * @param isNew whether this is a new node
     * @param isDelete whether the node is deleted
     * @param isWritten whether the MongoDB documented was added / updated
     * @param isBranchCommit whether this is from a branch commit
     * @param added the list of added child nodes
     * @param removed the list of removed child nodes
     *
     */
    public void applyChanges(Revision rev, String path,
                             boolean isNew, boolean isDelete, boolean isWritten,
                             boolean isBranchCommit, ArrayList<String> added,
                             ArrayList<String> removed) {
        UnsavedModifications unsaved = unsavedLastRevisions;
        if (isBranchCommit) {
            Revision branchRev = rev.asBranchRevision();
            unsaved = branches.getBranch(branchRev).getModifications(branchRev);
        }
        // track unsaved modifications of nodes that were not
        // written in the commit (implicitly modified parent)
        // or any modification if this is a branch commit
        if (!isWritten || isBranchCommit) {
            Revision prev = unsaved.put(path, rev);
            if (prev != null) {
                if (isRevisionNewer(prev, rev)) {
                    // revert
                    unsaved.put(path, prev);
                    String msg = String.format("Attempt to update " +
                            "unsavedLastRevision for %s with %s, which is " +
                            "older than current %s.",
                            path, rev, prev);
                    throw new MicroKernelException(msg);
                }
            }
        } else {
            // the document was updated:
            // we no longer need to update it in a background process
            unsaved.remove(path);
        }
        String key = path + "@" + rev;
        Node.Children c = nodeChildrenCache.getIfPresent(key);
        if (isNew || (!isDelete && c != null)) {
            Node.Children c2 = new Node.Children();
            TreeSet<String> set = new TreeSet<String>();
            if (c != null) {
                set.addAll(c.children);
            }
            set.removeAll(removed);
            for (String name : added) {
                // make sure the name string does not contain
                // unnecessary baggage
                set.add(new String(name));
            }
            c2.children.addAll(set);
            nodeChildrenCache.put(key, c2);
        }
        if (!added.isEmpty()) {
            NodeDocument.Children docChildren = docChildrenCache.getIfPresent(path);
            if (docChildren != null) {
                int currentSize = docChildren.childNames.size();
                TreeSet<String> names = new TreeSet<String>(docChildren.childNames);
                // incomplete cache entries must not be updated with
                // names at the end of the list because there might be
                // a next name in MongoDB smaller than the one added
                if (!docChildren.isComplete) {
                    for (String childPath : added) {
                        String name = PathUtils.getName(childPath);
                        if (names.higher(name) != null) {
                            // make sure the name string does not contain
                            // unnecessary baggage
                            names.add(new String(name));
                        }
                    }
                } else {
                    // add all
                    for (String childPath : added) {
                        // make sure the name string does not contain
                        // unnecessary baggage
                        names.add(new String(PathUtils.getName(childPath)));
                    }
                }
                // any changes?
                if (names.size() != currentSize) {
                    // create new cache entry with updated names
                    boolean complete = docChildren.isComplete;
                    docChildren = new NodeDocument.Children();
                    docChildren.isComplete = complete;
                    docChildren.childNames.addAll(names);
                    docChildrenCache.put(path, docChildren);
                }
            }
        }
    }

    //-------------------------< NodeStore >------------------------------------

    @Nonnull
    @Override
    public NodeState getRoot() {
        // TODO: implement
        return null;
    }

    @Nonnull
    @Override
    public NodeState merge(@Nonnull NodeBuilder builder,
                           @Nonnull CommitHook commitHook,
                           @Nonnull CommitInfo info)
            throws CommitFailedException {
        // TODO: implement
        return null;
    }

    @Nonnull
    @Override
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        // TODO: implement
        return null;
    }

    @Override
    public NodeState reset(@Nonnull NodeBuilder builder) {
        // TODO: implement
        return null;
    }

    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        // TODO: implement
        return null;
    }

    @Nonnull
    @Override
    public String checkpoint(long lifetime) {
        // TODO: implement
        return null;
    }

    @CheckForNull
    @Override
    public NodeState retrieve(@Nonnull String checkpoint) {
        // TODO: implement
        return null;
    }

    //------------------------< RevisionContext >-------------------------------

    @Override
    public UnmergedBranches getBranches() {
        return branches;
    }

    @Override
    public UnsavedModifications getPendingModifications() {
        return unsavedLastRevisions;
    }

    @Override
    public Revision.RevisionComparator getRevisionComparator() {
        return revisionComparator;
    }

    @Override
    public void publishRevision(Revision foreignRevision, Revision changeRevision) {
        Revision.RevisionComparator revisionComparator = getRevisionComparator();
        if (revisionComparator.compare(headRevision, foreignRevision) >= 0) {
            // already visible
            return;
        }
        int clusterNodeId = foreignRevision.getClusterId();
        if (clusterNodeId == this.clusterId) {
            return;
        }
        // the (old) head occurred first
        Revision headSeen = Revision.newRevision(0);
        // then we saw this new revision (from another cluster node)
        Revision otherSeen = Revision.newRevision(0);
        // and after that, the current change
        Revision changeSeen = Revision.newRevision(0);
        revisionComparator.add(foreignRevision, otherSeen);
        // TODO invalidating the whole cache is not really needed,
        // but how to ensure we invalidate the right part of the cache?
        // possibly simply wait for the background thread to pick
        // up the changes, but this depends on how often this method is called
        store.invalidateCache();
        // the latest revisions of the current cluster node
        // happened before the latest revisions of other cluster nodes
        revisionComparator.add(headRevision, headSeen);
        revisionComparator.add(changeRevision, changeSeen);
        // the head revision is after other revisions
        headRevision = Revision.newRevision(clusterId);
    }

    @Override
    public int getClusterId() {
        return clusterId;
    }

    //----------------------< background operations >---------------------------

    void runBackgroundOperations() {
        if (isDisposed.get()) {
            return;
        }
        backgroundRenewClusterIdLease();
        if (simpleRevisionCounter != null) {
            // only when using timestamp
            return;
        }
        if (!ENABLE_BACKGROUND_OPS || stopBackground) {
            return;
        }
        synchronized (this) {
            try {
                backgroundSplit();
                backgroundWrite();
                backgroundRead();
            } catch (RuntimeException e) {
                if (isDisposed.get()) {
                    return;
                }
                LOG.warn("Background operation failed: " + e.toString(), e);
            }
        }
    }

    private void backgroundRenewClusterIdLease() {
        if (clusterNodeInfo == null) {
            return;
        }
        clusterNodeInfo.renewLease(asyncDelay);
    }

    void backgroundRead() {
        String id = Utils.getIdFromPath("/");
        NodeDocument doc = store.find(Collection.NODES, id, asyncDelay);
        if (doc == null) {
            return;
        }
        Map<Integer, Revision> lastRevMap = doc.getLastRev();

        Revision.RevisionComparator revisionComparator = getRevisionComparator();
        boolean hasNewRevisions = false;
        // the (old) head occurred first
        Revision headSeen = Revision.newRevision(0);
        // then we saw this new revision (from another cluster node)
        Revision otherSeen = Revision.newRevision(0);
        for (Map.Entry<Integer, Revision> e : lastRevMap.entrySet()) {
            int machineId = e.getKey();
            if (machineId == clusterId) {
                continue;
            }
            Revision r = e.getValue();
            Revision last = lastKnownRevision.get(machineId);
            if (last == null || r.compareRevisionTime(last) > 0) {
                lastKnownRevision.put(machineId, r);
                hasNewRevisions = true;
                revisionComparator.add(r, otherSeen);
            }
        }
        if (hasNewRevisions) {
            // TODO invalidating the whole cache is not really needed,
            // instead only those children that are cached could be checked
            store.invalidateCache();
            // TODO only invalidate affected items
            docChildrenCache.invalidateAll();
            // add a new revision, so that changes are visible
            Revision r = Revision.newRevision(clusterId);
            // the latest revisions of the current cluster node
            // happened before the latest revisions of other cluster nodes
            revisionComparator.add(r, headSeen);
            // the head revision is after other revisions
            headRevision = Revision.newRevision(clusterId);
        }
        revisionComparator.purge(Revision.getCurrentTimestamp() - REMEMBER_REVISION_ORDER_MILLIS);
    }

    private void backgroundSplit() {
        for (Iterator<String> it = splitCandidates.keySet().iterator(); it.hasNext();) {
            String id = it.next();
            NodeDocument doc = store.find(Collection.NODES, id);
            if (doc == null) {
                continue;
            }
            for (UpdateOp op : doc.split(this)) {
                NodeDocument before = store.createOrUpdate(Collection.NODES, op);
                if (before != null) {
                    NodeDocument after = store.find(Collection.NODES, op.getId());
                    if (after != null) {
                        LOG.info("Split operation on {}. Size before: {}, after: {}",
                                new Object[]{id, before.getMemory(), after.getMemory()});
                    }
                }
            }
            it.remove();
        }
    }

    void backgroundWrite() {
        if (unsavedLastRevisions.getPaths().size() == 0) {
            return;
        }
        ArrayList<String> paths = new ArrayList<String>(unsavedLastRevisions.getPaths());
        // sort by depth (high depth first), then path
        Collections.sort(paths, new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                int d1 = Utils.pathDepth(o1);
                int d2 = Utils.pathDepth(o1);
                if (d1 != d2) {
                    return Integer.signum(d1 - d2);
                }
                return o1.compareTo(o2);
            }

        });

        long now = Revision.getCurrentTimestamp();
        UpdateOp updateOp = null;
        Revision lastRev = null;
        List<String> ids = new ArrayList<String>();
        for (int i = 0; i < paths.size(); i++) {
            String p = paths.get(i);
            Revision r = unsavedLastRevisions.get(p);
            if (r == null) {
                continue;
            }
            // FIXME: with below code fragment the root (and other nodes
            // 'close' to the root) will not be updated in MongoDB when there
            // are frequent changes.
            if (Revision.getTimestampDifference(now, r.getTimestamp()) < asyncDelay) {
                continue;
            }
            int size = ids.size();
            if (updateOp == null) {
                // create UpdateOp
                Commit commit = new Commit(this, null, r);
                commit.touchNode(p);
                updateOp = commit.getUpdateOperationForNode(p);
                lastRev = r;
                ids.add(Utils.getIdFromPath(p));
            } else if (r.equals(lastRev)) {
                // use multi update when possible
                ids.add(Utils.getIdFromPath(p));
            }
            // update if this is the last path or
            // revision is not equal to last revision
            if (i + 1 >= paths.size() || size == ids.size()) {
                store.update(Collection.NODES, ids, updateOp);
                for (String id : ids) {
                    unsavedLastRevisions.remove(Utils.getPathFromId(id));
                }
                ids.clear();
                updateOp = null;
                lastRev = null;
            }
        }
    }

    //-----------------------------< internal >---------------------------------

    private void moveOrCopyNode(boolean move,
                                String sourcePath,
                                String targetPath,
                                Commit commit) {
        // TODO Optimize - Move logic would not work well with very move of very large subtrees
        // At minimum we can optimize by traversing breadth wise and collect node id
        // and fetch them via '$in' queries

        // TODO Transient Node - Current logic does not account for operations which are part
        // of this commit i.e. transient nodes. If its required it would need to be looked
        // into

        Node n = getNode(sourcePath, commit.getBaseRevision());

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
        Node.Children c = getChildren(sourcePath, commit.getBaseRevision(), Integer.MAX_VALUE);
        for (String srcChildPath : c.children) {
            String childName = PathUtils.getName(srcChildPath);
            String destChildPath = PathUtils.concat(targetPath, childName);
            moveOrCopyNode(move, srcChildPath, destChildPath, commit);
        }
    }

    private void checkRevisionAge(Revision r, String path) {
        // TODO only log if there are new revisions available for the given node
        if (LOG.isDebugEnabled()) {
            if (headRevision.getTimestamp() - r.getTimestamp() > WARN_REVISION_AGE) {
                LOG.debug("Requesting an old revision for path " + path + ", " +
                        ((headRevision.getTimestamp() - r.getTimestamp()) / 1000) + " seconds old");
            }
        }
    }

    /**
     * A background thread.
     */
    static class BackgroundOperation implements Runnable {
        final WeakReference<MongoNodeStore> ref;
        private final AtomicBoolean isDisposed;
        private int delay;

        BackgroundOperation(MongoNodeStore nodeStore, AtomicBoolean isDisposed) {
            ref = new WeakReference<MongoNodeStore>(nodeStore);
            delay = nodeStore.getAsyncDelay();
            this.isDisposed = isDisposed;
        }

        @Override
        public void run() {
            while (delay != 0 && !isDisposed.get()) {
                synchronized (isDisposed) {
                    try {
                        isDisposed.wait(delay);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                MongoNodeStore nodeStore = ref.get();
                if (nodeStore != null) {
                    nodeStore.runBackgroundOperations();
                    delay = nodeStore.getAsyncDelay();
                }
            }
        }
    }
}
