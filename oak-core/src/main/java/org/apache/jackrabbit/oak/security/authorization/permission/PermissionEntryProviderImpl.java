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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;

/**
 * {@code PermissionEntryProviderImpl} ...
 */
public class PermissionEntryProviderImpl implements PermissionEntryProvider {

    private static final long MAX_SIZE = 250; // TODO define size or make configurable

    private final Set<String> principalNames;

    private final Set<String> existingNames = new HashSet<String>();

    private Map<String, Collection<PermissionEntry>> pathEntryMap;

    private final PermissionStore store;

    private final PermissionEntryCache.Local cache;

    protected PermissionEntryProviderImpl(@Nonnull PermissionStore store, @Nonnull PermissionEntryCache.Local cache,
                                          @Nonnull Set<String> principalNames) {
        this.store = store;
        this.cache = cache;
        this.principalNames = Collections.unmodifiableSet(principalNames);
        init();
    }

    private void init() {
        long cnt = 0;
        existingNames.clear();
        for (String name: principalNames) {
            if (cnt > MAX_SIZE) {
                if (cache.hasEntries(store, name)) {
                    existingNames.add(name);
                }
            } else {
                long n = cache.getNumEntries(store, name);
                cnt+= n;
                if (n > 0) {
                    existingNames.add(name);
                }
            }
        }
        if (cnt < MAX_SIZE) {
            // cache all entries of all principals
            pathEntryMap = new HashMap<String, Collection<PermissionEntry>>();
            for (String name: principalNames) {
                cache.load(store, pathEntryMap, name);
            }
        } else {
            pathEntryMap = null;
        }
    }

    public void flush() {
        cache.flush(principalNames);
        init();
    }

    public Iterator<PermissionEntry> getEntryIterator(@Nonnull EntryPredicate predicate) {
        if (existingNames.isEmpty()) {
            return Iterators.emptyIterator();
        } else {
            return new EntryIterator(predicate);
        }
    }

    public Collection<PermissionEntry> getEntries(@Nonnull Tree accessControlledTree) {
        if (existingNames.isEmpty()) {
            return Collections.emptyList();
        } else if (pathEntryMap != null) {
            Collection<PermissionEntry> entries = pathEntryMap.get(accessControlledTree.getPath());
            return (entries != null) ? entries : Collections.<PermissionEntry>emptyList();
        } else {
            return (accessControlledTree.hasChild(AccessControlConstants.REP_POLICY)) ?
                    loadEntries(accessControlledTree.getPath()) :
                    Collections.<PermissionEntry>emptyList();
        }
    }

    @Nonnull
    public Collection<PermissionEntry> getEntries(@Nonnull String path) {
        if (existingNames.isEmpty()) {
            return Collections.emptyList();
        } else if (pathEntryMap != null) {
            Collection<PermissionEntry> entries = pathEntryMap.get(path);
            return (entries != null) ? entries : Collections.<PermissionEntry>emptyList();
        } else {
            return loadEntries(path);
        }
    }

    @Nonnull
    private Collection<PermissionEntry> loadEntries(@Nonnull String path) {
        Collection<PermissionEntry> ret = new TreeSet<PermissionEntry>();
        for (String name: existingNames) {
            cache.load(store, ret, name, path);
        }
        return ret;
    }

    private final class EntryIterator extends AbstractEntryIterator {

        private final EntryPredicate predicate;

        // the ordered permission entries at a given path in the hierarchy
        private Iterator<PermissionEntry> nextEntries = Iterators.emptyIterator();

        // the next oak path for which to retrieve permission entries
        private String path;

        private EntryIterator(@Nonnull EntryPredicate predicate) {
            this.predicate = predicate;
            this.path = Strings.nullToEmpty(predicate.getPath());
        }

        @Override
        protected void seekNext() {
            for (next = null; next == null;) {
                if (nextEntries.hasNext()) {
                    PermissionEntry pe = nextEntries.next();
                    if (predicate.apply(pe)) {
                        next = pe;
                    }
                } else {
                    if (path == null) {
                        break;
                    }
                    nextEntries = getEntries(path).iterator();
                    path = PermissionUtil.getParentPathOrNull(path);
                }
            }
        }
    }
}