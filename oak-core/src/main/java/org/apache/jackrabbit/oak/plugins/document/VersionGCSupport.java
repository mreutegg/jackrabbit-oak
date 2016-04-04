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

package org.apache.jackrabbit.oak.plugins.document;

import static com.google.common.collect.Iterables.filter;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getSelectedDocuments;

import java.util.Set;

import org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class VersionGCSupport {

    private final DocumentStore store;

    public VersionGCSupport(DocumentStore store) {
        this.store = store;
    }

    public Iterable<NodeDocument> getPossiblyDeletedDocs(final long lastModifiedTime) {
        return filter(getSelectedDocuments(store, NodeDocument.DELETED_ONCE, 1), new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument input) {
                return input.wasDeletedOnce() && !input.hasBeenModifiedSince(lastModifiedTime);
            }
        });
    }

    public void deleteSplitDocuments(Set<SplitDocType> gcTypes,
                                     RevisionVector sweepRevs,
                                     long oldestRevTimeStamp,
                                     VersionGCStats stats) {
        stats.splitDocGCCount += createCleanUp(gcTypes, sweepRevs, oldestRevTimeStamp, stats)
                .disconnect()
                .deleteSplitDocuments();
    }

    protected SplitDocumentCleanUp createCleanUp(Set<SplitDocType> gcTypes,
                                                 RevisionVector sweepRevs,
                                                 long oldestRevTimeStamp,
                                                 VersionGCStats stats) {
        return new SplitDocumentCleanUp(store, stats,
                identifyGarbage(gcTypes, sweepRevs, oldestRevTimeStamp));
    }

    protected Iterable<NodeDocument> identifyGarbage(final Set<SplitDocType> gcTypes,
                                                     final RevisionVector sweepRevs,
                                                     final long oldestRevTimeStamp) {
        return filter(getAllDocuments(store), new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument doc) {
                return gcTypes.contains(doc.getSplitDocType())
                        && doc.hasAllRevisionLessThan(oldestRevTimeStamp)
                        && !isDefaultSplitNewerThan(sweepRevs, doc);
            }
        });
    }

    protected static boolean isDefaultSplitNewerThan(RevisionVector sweepRevs,
                                           NodeDocument doc) {
        if (doc.getSplitDocType() != SplitDocType.DEFAULT) {
            return false;
        }
        Revision r = Iterables.getFirst(doc.getAllChanges(), null);
        return r != null && sweepRevs.isRevisionNewer(r);
    }
}
