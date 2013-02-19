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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mongomk.impl.blob.MongoBlobStore;
import org.apache.jackrabbit.mongomk.prototype.Node.Children;
import org.apache.jackrabbit.oak.commons.PathUtils;

import com.mongodb.DB;

/**
 * A MicroKernel implementation that stores the data in a MongoDB.
 */
public class MongoMK implements MicroKernel {

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
    // TODO: should be path@id
    private final Map<String, Node> nodeCache = new Cache<String, Node>(1024);
    
    /**
     * The unsaved write count increments.
     */
    private final Map<String, Long> writeCountIncrements = new HashMap<String, Long>();

    /**
     * For revisions that are older than this many seconds, the MongoMK will
     * assume the revision is valid. For more recent changes, the MongoMK needs
     * to verify it first (by reading the revision root). The default is
     * Integer.MAX_VALUE, meaning no revisions are trusted. Once the garbage
     * collector removes old revisions, this value is changed.
     */
    private static final int trustedRevisionAge = Integer.MAX_VALUE;

    /**
     * The delay for asynchronous operations (delayed commit propagation and
     * cache update).
     */
    protected static final long ASYNC_DELAY = 1000;

    /**
     * The set of known valid revision.
     * The key is the revision id, the value is 1 (because a cache can't be a set).
     */
    private final Map<String, Long> revCache = new Cache<String, Long>(1024);
    
    /**
     * The last known head revision. This is the last-known revision.
     */
    private Revision headRevision;
    
    AtomicBoolean isDisposed = new AtomicBoolean();
    
    private Thread backgroundThread;
    
    private final Map<String, String> branchCommits = new HashMap<String, String>();

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
        backgroundThread = new Thread(new Runnable() {
            public void run() {
                while (!isDisposed.get()) {
                    synchronized (isDisposed) {
                        try {
                            isDisposed.wait(ASYNC_DELAY);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                    runBackgroundOperations();
                }
            }
        }, "MongoMK background thread");
        backgroundThread.setDaemon(true);
        backgroundThread.start();
        headRevision = Revision.newRevision(clusterId);
        Node n = readNode("/", headRevision);
        if (n == null) {
            // root node is missing: repository is not initialized
            Commit commit = new Commit(headRevision);
            n = new Node("/", headRevision);
            commit.addNode(n);
            commit.apply(store);
        }
    }
    
    Revision newRevision() {
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
        String key = path + "@" + rev;
        Node node = nodeCache.get(key);
        if (node == null) {
            node = readNode(path, rev);
            if (node != null) {
                nodeCache.put(key, node);
            }
        }
        return node;
    }
    
    private boolean includeRevision(Revision x, Revision requestRevision) {
        if (x.getClusterId() == this.clusterId && 
                requestRevision.getClusterId() == this.clusterId) {
            // both revisions were created by this cluster node: 
            // compare timestamps only
            return requestRevision.compareRevisionTime(x) >= 0;
        }
        // TODO currently we only compare the timestamps
        return x.compareRevisionTime(requestRevision) >= 0;
    }
    
    public Node.Children readChildren(String path, Revision rev, int limit) {
        String from = PathUtils.concat(path, "a");
        from = Node.convertPathToDocumentId(from);
        from = from.substring(0, from.length() - 1);
        String to = PathUtils.concat(path, "z");
        to = Node.convertPathToDocumentId(to);
        to = to.substring(0, to.length() - 2) + "0";
        List<Map<String, Object>> list = store.query(DocumentStore.Collection.NODES, from, to, limit);
        Node.Children c = new Node.Children(path, rev);
        for (Map<String, Object> e : list) {
            // TODO put the whole node in the cache
            String id = e.get("_id").toString();
            String p = id.substring(2);
            c.children.add(p);
        }
        return c;
    }

    private Node readNode(String path, Revision rev) {
        String id = Node.convertPathToDocumentId(path);
        Map<String, Object> map = store.find(DocumentStore.Collection.NODES, id);
        if (map == null) {
            return null;
        }
        Node n = new Node(path, rev);
        Long w = writeCountIncrements.get(path);
        long writeCount = w == null ? 0 : w;
        for(String key : map.keySet()) {
            if (key.equals("_writeCount")) {
                writeCount += (Long) map.get(key);
            }
            if (key.startsWith("_")) {
                // TODO property name escaping
                continue;
            }
            Object v = map.get(key);
            @SuppressWarnings("unchecked")
            Map<String, String> valueMap = (Map<String, String>) v;
            if (valueMap != null) {
                for (String r : valueMap.keySet()) {
                    Revision propRev = Revision.fromString(r);
                    if (includeRevision(propRev, rev)) {
                        n.setProperty(key, valueMap.get(r));
                    }
                }
            }
        }
        n.setWriteCount(writeCount);
        return n;
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
        // TODO implement if needed
        return "{}";
    }

    @Override
    public boolean nodeExists(String path, String revisionId)
            throws MicroKernelException {
        Revision rev = Revision.fromString(revisionId);
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
    public String getNodes(String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws MicroKernelException {
        if (depth != 0) {
            throw new MicroKernelException("Only depth 0 is supported, depth is " + depth);
        }
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
        includeId = filter != null && filter.contains(":hash");
        json.object();
        n.append(json, includeId);
        Children c = readChildren(path, rev, maxChildNodes);
        for (String s : c.children) {
            String name = PathUtils.getName(s);
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
    public String commit(String rootPath, String json, String revisionId,
            String message) throws MicroKernelException {
        revisionId = revisionId == null ? headRevision.toString() : revisionId;
        JsopReader t = new JsopTokenizer(json);
        Revision rev = Revision.newRevision(clusterId);
        Commit commit = new Commit(rev);
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
                // TODO support remove operations
                commit.removeNode(path);
                break;
            case '^':
                t.read(':');
                String value;
                if (t.matches(JsopReader.NULL)) {
                    value = null;
                    commit.getDiff().tag('^').key(path).value(null);
                } else {
                    value = t.readRawValue().trim();
                    commit.getDiff().tag('^').key(path).value(null);
                }
                String p = PathUtils.getParentPath(path);
                String propertyName = PathUtils.getName(path);
                UpdateOp op = commit.getUpdateOperationForNode(p);
                op.addMapEntry(propertyName, rev.toString(), value);
                op.increment("_writeCount", 1);
                break;
            case '>': {
                t.read(':');
                String name = PathUtils.getName(path);
                String position, target, to;
                boolean rename;
                if (t.matches('{')) {
                    rename = false;
                    position = t.readString();
                    t.read(':');
                    target = t.readString();
                    t.read('}');
                    if (!PathUtils.isAbsolute(target)) {
                        target = PathUtils.concat(rootPath, target);
                    }
                } else {
                    rename = true;
                    position = null;
                    target = t.readString();
                    if (!PathUtils.isAbsolute(target)) {
                        target = PathUtils.concat(rootPath, target);
                    }
                }
                boolean before = false;
                if ("last".equals(position)) {
                    target = PathUtils.concat(target, name);
                    position = null;
                } else if ("first".equals(position)) {
                    target = PathUtils.concat(target, name);
                    position = null;
                    before = true;
                } else if ("before".equals(position)) {
                    position = PathUtils.getName(target);
                    target = PathUtils.getParentPath(target);
                    target = PathUtils.concat(target, name);
                    before = true;
                } else if ("after".equals(position)) {
                    position = PathUtils.getName(target);
                    target = PathUtils.getParentPath(target);
                    target = PathUtils.concat(target, name);
                } else if (position == null) {
                    // move
                } else {
                    throw new MicroKernelException("position: " + position);
                }
                to = PathUtils.relativize("/", target);
                boolean inPlaceRename = false;
                if (rename) {
                    if (PathUtils.getParentPath(path).equals(PathUtils.getParentPath(to))) {
                        inPlaceRename = true;
                        position = PathUtils.getName(path);
                    }
                }
                // TODO support move operations
                break;
            }
            case '*': {
                // TODO possibly support target position notation
                t.read(':');
                String target = t.readString();
                if (!PathUtils.isAbsolute(target)) {
                    target = PathUtils.concat(rootPath, target);
                }
                String to = PathUtils.relativize("/", target);
                // TODO support copy operations
                break;
            }
            default:
                throw new MicroKernelException("token: " + (char) t.getTokenType());
            }
        }
        if (revisionId.startsWith("b")) {
            // just commit to head currently
            applyCommit(commit);
            return "b" + rev.toString();
            
            // String jsonBranch = branchCommits.remove(revisionId);
            // jsonBranch += commit.getDiff().toString();
            // String branchRev = revisionId + "+";
            // branchCommits.put(branchRev, jsonBranch);
            // return branchRev;
        }
        applyCommit(commit);
        return rev.toString();
    }    
    
    private void applyCommit(Commit commit) {
        headRevision = commit.getRevision();
        if (commit.isEmpty()) {
            return;
        }
        commit.apply(store);
        for(String path : commit.getChangedParents()) {
            Long value = writeCountIncrements.get(path);
            value = value == null ? 1 : value + 1;
            writeCountIncrements.put(path, value);
        }
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
    }

    @Override
    public String branch(String trunkRevisionId) throws MicroKernelException {
        // TODO improve implementation if needed
        String branchId = "b" + trunkRevisionId;
        // branchCommits.put(branchId, "");
        return branchId;
    }

    @Override
    public String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        // reading from the branch is reading from the trunk currently
        String revisionId = branchRevisionId.substring(1).replace('+', ' ').trim();
        return revisionId;
        
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

    static class Cache<K, V> extends LinkedHashMap<K, V> {

        private static final long serialVersionUID = 1L;
        private int size;

        Cache(int size) {
            super(size, (float) 0.75, true);
            this.size = size;
        }

        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > size;
        }

    }

    public DocumentStore getDocumentStore() {
        return store;
    }

}
