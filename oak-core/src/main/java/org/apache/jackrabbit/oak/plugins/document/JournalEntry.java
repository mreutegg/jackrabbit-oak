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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;

/**
 * Keeps track of changes performed between two consecutive background updates.
 *
 * TODO:
 *      Query external changes in chunks.
 *      {@link #getChanges(Revision, Revision, DocumentStore)} current reads
 *      all JournalEntry documents in one go with a limit of Integer.MAX_VALUE.
 * TODO:
 *      Use external sort when changes are applied to diffCache. See usage of
 *      {@link #applyTo(DiffCache, Revision, Revision)} in
 *      {@link DocumentNodeStore#backgroundRead(boolean)}.
 *      The utility {@link StringSort} can be used for this purpose.
 * TODO:
 *      Push changes to {@link MemoryDiffCache} instead of {@link LocalDiffCache}.
 *      See {@link TieredDiffCache#newEntry(Revision, Revision)}. Maybe a new
 *      method is needed for this purpose?
 * TODO:
 *      Create JournalEntry for external changes related to _lastRev recovery.
 *      See {@link LastRevRecoveryAgent#recover(Iterator, int, boolean)}.
 * TODO:
 *      Cleanup old journal entries in the document store.
 */
public final class JournalEntry extends Document {

    /**
     * The revision format for external changes:
     * &lt;clusterId>-&lt;timestamp>-&lt;counter>. The string is prefixed with
     * "b" if it denotes a branch revision.
     */
    private static final String REVISION_FORMAT = "%d-%0" +
            Long.toHexString(Long.MAX_VALUE).length() + "x-%0" +
            Integer.toHexString(Integer.MAX_VALUE).length() + "x";

    private static final String CHANGES = "_c";

    private static final String BRANCH_COMMITS = "_bc";

    private final DocumentStore store;

    private volatile TreeNode changes = null;

    JournalEntry(DocumentStore store) {
        this.store = store;
    }

    /**
     * Gets a document with external changes within the given revision range.
     * The two revisions must have the same clusterId.
     *
     * @param from the lower bound of the revision range (exclusive).
     * @param to the upper bound of the revision range (inclusive).
     * @param store the document store to query.
     * @return a document with the external changes between {@code from} and
     *          {@code to}.
     */
    static JournalEntry getChanges(@Nonnull Revision from,
                                   @Nonnull Revision to,
                                   @Nonnull DocumentStore store) {
        checkArgument(checkNotNull(from).getClusterId() == checkNotNull(to).getClusterId());

        // to is inclusive, but DocumentStore.query() toKey is exclusive
        to = new Revision(to.getTimestamp(), to.getCounter() + 1,
                to.getClusterId(), to.isBranch());

        JournalEntry doc = JOURNAL.newDocument(store);
        for (JournalEntry d : store.query(JOURNAL, asId(from), asId(to), Integer.MAX_VALUE)) {
            doc.apply(d);
        }
        return doc;
    }

    void modified(String path) {
        TreeNode node = getChanges();
        for (String name : PathUtils.elements(path)) {
            node = node.getOrCreate(name);
        }
    }

    void modified(Iterable<String> paths) {
        for (String p : paths) {
            modified(p);
        }
    }

    void branchCommit(@Nonnull Iterable<Revision> revisions) {
        String branchCommits = (String) get(BRANCH_COMMITS);
        if (branchCommits == null) {
            branchCommits = "";
        }
        for (Revision r : revisions) {
            if (branchCommits.length() > 0) {
                branchCommits += ",";
            }
            branchCommits += asId(r.asBranchRevision());
        }
        put(BRANCH_COMMITS, branchCommits);
    }

    String getChanges(String path) {
        TreeNode node = getNode(path);
        if (node == null) {
            return "";
        }
        return getChanges(node);
    }

    UpdateOp asUpdateOp(@Nonnull Revision revision) {
        String id = asId(revision);
        UpdateOp op = new UpdateOp(id, true);
        op.set(ID, id);
        op.set(CHANGES, getChanges().serialize());
        String bc = (String) get(BRANCH_COMMITS);
        if (bc != null) {
            op.set(BRANCH_COMMITS, bc);
        }
        return op;
    }

    /**
     * Applies the changes of the {@code other} document on top of this
     * document.
     *
     * @param other the other document with external changes.
     */
    void apply(JournalEntry other) {
        TreeNode n = getChanges();
        n.apply(other.getChanges());
        for (JournalEntry e : other.getBranchCommits()) {
            n.apply(e.getChanges());
        }
    }

    /**
     * Applies the changes of this document to the diff cache.
     *
     * @param diffCache the diff cache.
     * @param from the lower bound revision of this change set.
     * @param to the upper bound revision of this change set.
     */
    void applyTo(@Nonnull DiffCache diffCache,
                 @Nonnull Revision from,
                 @Nonnull Revision to) {
        final DiffCache.Entry entry = checkNotNull(diffCache).newEntry(from, to);
        TraversingVisitor visitor = new TraversingVisitor() {
            @Override
            public void node(TreeNode node, String path) {
                entry.append(path, getChanges(node));
            }
        };
        getChanges().accept(visitor, "/");
        for (JournalEntry e : getBranchCommits()) {
            e.getChanges().accept(visitor, "/");
        }
        entry.done();
    }

    /**
     * Returns the branch commits that are related to this journal entry.
     *
     * @return the branch commits.
     */
    @Nonnull
    Iterable<JournalEntry> getBranchCommits() {
        List<JournalEntry> commits = Lists.newArrayList();
        String bc = (String) get(BRANCH_COMMITS);
        if (bc != null) {
            for (String id : bc.split(",")) {
                JournalEntry d = store.find(JOURNAL, id);
                if (d == null) {
                    throw new IllegalStateException(
                            "Missing external change for revision: " + id);
                }
                commits.add(d);
            }
        }
        return commits;
    }

    //-----------------------------< internal >---------------------------------

    private String getChanges(TreeNode node) {
        JsopBuilder builder = new JsopBuilder();
        for (String name : node.keySet()) {
            builder.tag('^');
            builder.key(name);
            builder.object().endObject();
        }
        return builder.toString();
    }

    private static String asId(@Nonnull Revision revision) {
        checkNotNull(revision);
        String s = String.format(REVISION_FORMAT, revision.getClusterId(), revision.getTimestamp(), revision.getCounter());
        if (revision.isBranch()) {
            s = "b" + s;
        }
        return s;
    }

    @CheckForNull
    private TreeNode getNode(String path) {
        TreeNode node = getChanges();
        for (String name : PathUtils.elements(path)) {
            node = node.get(name);
            if (node == null) {
                return null;
            }
        }
        return node;
    }

    @Nonnull
    private TreeNode getChanges() {
        if (changes == null) {
            TreeNode node = new TreeNode();
            String c = (String) get(CHANGES);
            if (c != null) {
                node.parse(new JsopTokenizer(c));
            }
            changes = node;
        }
        return changes;
    }

    private static final class TreeNode {

        private final Map<String, TreeNode> children = Maps.newHashMap();

        void apply(TreeNode node) {
            for (Map.Entry<String, TreeNode> entry : node.children.entrySet()) {
                getOrCreate(entry.getKey()).apply(entry.getValue());
            }
        }

        void parse(JsopReader reader) {
            reader.read('{');
            if (!reader.matches('}')) {
                do {
                    String name = Utils.unescapePropertyName(reader.readString());
                    reader.read(':');
                    getOrCreate(name).parse(reader);
                } while (reader.matches(','));
                reader.read('}');
            }
        }

        String serialize() {
            JsopBuilder builder = new JsopBuilder();
            builder.object();
            toJson(builder);
            builder.endObject();
            return builder.toString();
        }

        @Nonnull
        Set<String> keySet() {
            return children.keySet();
        }

        @CheckForNull
        TreeNode get(String name) {
            return children.get(name);
        }

        void accept(TraversingVisitor visitor, String path) {
            visitor.node(this, path);
            for (Map.Entry<String, TreeNode> entry : children.entrySet()) {
                entry.getValue().accept(visitor, concat(path, entry.getKey()));
            }
        }

        private void toJson(JsopBuilder builder) {
            for (Map.Entry<String, TreeNode> entry : children.entrySet()) {
                builder.key(Utils.escapePropertyName(entry.getKey()));
                builder.object();
                entry.getValue().toJson(builder);
                builder.endObject();
            }
        }

        private TreeNode getOrCreate(String name) {
            TreeNode c = children.get(name);
            if (c == null) {
                c = new TreeNode();
                children.put(name, c);
            }
            return c;
        }
    }

    private interface TraversingVisitor {

        void node(TreeNode node, String path);
    }
}
