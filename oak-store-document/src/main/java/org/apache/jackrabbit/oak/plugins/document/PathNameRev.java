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

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <code>PathNameRev</code>...
 */
public final class PathNameRev implements CacheValue {

    private static final Logger LOG = LoggerFactory.getLogger(PathNameRev.class);

    @NotNull
    private final Path path;

    @NotNull
    private final String name;

    @NotNull
    private final RevisionVector revision;

    public PathNameRev(@NotNull Path path,
                       @Nullable String name,
                       @NotNull RevisionVector revision) {
        this.path = checkNotNull(path);
        this.name = name != null ? name : "";
        this.revision = checkNotNull(revision);
    }

    @NotNull
    public Path getPath() {
        return path;
    }

    @Override
    public int getMemory() {
        long size = 28L // shallow size
                + path.getMemory()
                + StringUtils.estimateMemoryUsage(name)
                + revision.getMemory();
        if (size > Integer.MAX_VALUE) {
            LOG.debug("Estimated memory footprint larger than Integer.MAX_VALUE: {}.", size);
            size = Integer.MAX_VALUE;
        }
        return (int) size;
    }

    //----------------------------< Object >------------------------------------


    @Override
    public int hashCode() {
        return name.hashCode() ^ path.hashCode() ^ revision.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof PathNameRev) {
            PathNameRev other = (PathNameRev) obj;
            return revision.equals(other.revision)
                    && name.equals(other.name)
                    && path.equals(other.path);
        }
        return false;
    }

    @Override
    public String toString() {
        int dim = revision.getDimensions();
        int len = path.length() + name.length();
        StringBuilder sb = new StringBuilder(len + (Revision.REV_STRING_APPROX_SIZE + 1) * dim);
        sb.append(name);
        // TODO: optimize
        sb.append(path);
        sb.append("@");
        revision.toStringBuilder(sb);
        return sb.toString();
    }

    public String asString() {
        return toString();
    }

    public static PathNameRev fromString(String s) {
        String name = "";
        int index = s.indexOf('/');
        if (index > 0) {
            name = s.substring(0, index);
            s = s.substring(index);
        }
        index = s.lastIndexOf('@');
        return new PathNameRev(Path.fromString(s.substring(0, index)),
                name, RevisionVector.fromString(s.substring(index + 1)));
    }

    public int compareTo(PathNameRev b) {
        if (this == b) {
            return 0;
        }
        int compare = name.compareTo(b.name);
        if (compare == 0) {
            // TODO: optimize?
            compare = path.toString().compareTo(b.path.toString());
        }
        if (compare == 0) {
            compare = revision.compareTo(b.revision);
        }
        return compare;
    }
}
