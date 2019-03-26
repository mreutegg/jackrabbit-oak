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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.elementsEqual;

/**
 * <code>Path</code>...
 */
public final class Path implements CacheValue, Comparable<Path> {

    private static final String NULL_PATH_STRING = "<null>";

    private static final Pattern DELIMITED = Pattern.compile("/");

    public static final Path ROOT = new Path(null, "", "".hashCode());

    public static final Path NULL = new Path(null, NULL_PATH_STRING, NULL_PATH_STRING.hashCode());

    @Nullable
    private final Path parent;

    @NotNull
    private final String name;

    private int hash;

    private Path(@Nullable Path parent,
                 @NotNull String name,
                 int hash) {
        this.parent = parent;
        this.name = name;
        this.hash = hash;
    }

    public Path(@NotNull Path parent, @NotNull String name) {
        this(checkNotNull(parent), checkNotNull(name), -1);
        checkArgument(!name.isEmpty(), "name cannot be the empty String");
        checkArgument(parent != NULL, "parent cannot be the NULL path");
    }

    @NotNull
    public String getName() {
        return name;
    }

    @NotNull
    public Iterable<String> elements() {
        List<String> elements = new ArrayList<>(getDepth());
        Path p = this;
        while (p.parent != null) {
            elements.add(p.name);
            p = p.parent;
        }
        return Lists.reverse(elements);
    }

    public boolean isRoot() {
        return name.isEmpty();
    }

    @Nullable
    public Path getParent() {
        return parent;
    }

    public int length() {
        if (isRoot()) {
            // root
            return 1;
        } else if (this == NULL) {
            return name.length();
        }
        int length = 0;
        Path p = this;
        while (p.parent != null) {
            length += p.name.length() + 1;
            p = p.parent;
        }
        return length;
    }

    /**
     * The depth of this path. The {@link #ROOT} and {@link #NULL} paths have
     * a depth of 0. The path {@code /foo/bar} has depth 2.
     *
     * @return the depth of the path.
     */
    public int getDepth() {
        int depth = 0;
        Path p = this;
        while (p.parent != null) {
            depth++;
            p = p.parent;
        }
        return depth;
    }

    /**
     * Get the nth ancestor of a path. The 1st ancestor is the parent path,
     * 2nd ancestor the grandparent path, and so on...
     * <p>
     * If {@code nth <= 0}, then this path is returned.
     *
     * @param nth  indicates the ancestor level for which the path should be
     *             calculated.
     * @return the ancestor path
     */
    @NotNull
    public Path getAncestor(int nth) {
        Path p = this;
        while (nth-- > 0 && p.parent != null) {
            p = p.parent;
        }
        return p;
    }

    /**
     * Return {@code true} if {@code this} path is an ancestor of the
     * {@code other} path, otherwise {@code false}.
     *
     * @param other the other path.
     * @return whether this path is an ancestor of the other path.
     */
    public boolean isAncestorOf(@NotNull Path other) {
        checkNotNull(other);
        if (this == NULL || other == NULL) {
            return false;
        }
        int depthDiff = other.getDepth() - getDepth();
        return depthDiff > 0
                && elementsEqual(elements(), other.getAncestor(depthDiff).elements());
    }

    @NotNull
    public static Path fromCharSequence(@NotNull CharSequence path) throws IllegalArgumentException {
        checkNotNull(path);
        if (NULL_PATH_STRING.contentEquals(path)) {
            return NULL;
        }
        if (path.length() == 0 || path.charAt(0) != '/') {
            throw new IllegalArgumentException("path must be absolute");
        }
        Path p = ROOT;
        for (String name : DELIMITED.split(path)) {
            if (!name.isEmpty()) {
                p = new Path(p, StringCache.get(name));
            }
        }
        return p;
    }

    @NotNull
    public static Path fromString(@NotNull String path) throws IllegalArgumentException {
        checkNotNull(path);
        if (path.equals(NULL_PATH_STRING)) {
            return NULL;
        }
        if (!PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("path must be absolute");
        }
        Path p = ROOT;
        for (String name : PathUtils.elements(path)) {
            p = new Path(p, StringCache.get(name));
        }
        return p;
    }

    @NotNull
    public StringBuilder toStringBuilder(@NotNull StringBuilder sb) {
        if (name.isEmpty()) {
            sb.append('/');
        } else {
            buildPath(sb);
        }
        return sb;
    }

    @Override
    public int getMemory() {
        int memory = 0;
        Path p = this;
        while (p.parent != null) {
            memory += 24; // shallow size
            memory += StringUtils.estimateMemoryUsage(name);
            p = p.parent;
        }
        return memory;
    }

    @Override
    public int compareTo(@NotNull Path other) {
        checkNotNull(other);
        // a few special cases
        if (this == NULL) {
            return other == NULL ? 0 : -1;
        } else if (other == NULL) {
            return 1;
        }
        // regular case (neither this nor other is NULL)
        int depth = getDepth();
        int otherDepth = other.getDepth();
        int minDepth = Math.min(depth, otherDepth);
        Iterable<String> elements = getAncestor(depth - minDepth).elements();
        Iterator<String> otherElements = other.getAncestor(otherDepth - minDepth).elements().iterator();
        for (String name : elements) {
            String otherName = otherElements.next();
            int c = name.compareTo(otherName);
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(depth, otherDepth);
    }

    @Override
    public String toString() {
        if (name.isEmpty()) {
            return "/";
        } else if (this == NULL) {
            return NULL_PATH_STRING;
        } else {
            return buildPath(new StringBuilder()).toString();
        }
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == -1 && parent != null) {
            h = parent.hashCode() ^ name.hashCode();
            hash = h;
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof Path) {
            Path other = (Path) obj;
            return this.name.equals(other.name)
                    && Objects.equals(this.parent, other.parent);
        }
        return false;
    }

    private StringBuilder buildPath(StringBuilder sb) {
        if (parent != null) {
            parent.buildPath(sb).append("/").append(name);
        }
        return sb;
    }
}
