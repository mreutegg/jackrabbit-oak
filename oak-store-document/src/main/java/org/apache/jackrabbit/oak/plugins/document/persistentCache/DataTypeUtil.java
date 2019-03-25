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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.StringDataType;

/**
 * <code>DataTypeUtil</code>...
 */
class DataTypeUtil {

    static void revisionVectorToBuffer(RevisionVector rv, WriteBuffer buffer) {
        buffer.putVarInt(rv.getDimensions());
        for (Revision r : rv) {
            buffer.putLong(r.getTimestamp());
            buffer.putVarInt(r.getCounter());
            buffer.putVarInt(r.getClusterId());
            byte b = 0;
            if (r.isBranch()) {
                b = 1;
            }
            buffer.put(b);
        }
    }

    static RevisionVector revisionVectorFromBuffer(ByteBuffer buffer) {
        int dim  = DataUtils.readVarInt(buffer);
        List<Revision> revisions = new ArrayList<>();
        for (int i = 0; i < dim; i++) {
            revisions.add(new Revision(
                    buffer.getLong(),
                    DataUtils.readVarInt(buffer),
                    DataUtils.readVarInt(buffer),
                    buffer.get() == 1)
            );
        }
        return new RevisionVector(revisions);
    }

    static void pathToBuffer(Path p, WriteBuffer buffer) {
        buffer.putVarInt(p.getDepth() + 1);
        // write path elements backwards
        while (p != null) {
            StringDataType.INSTANCE.write(buffer, p.getName());
            p = p.getParent();
        }
    }

    static Path pathFromBuffer(ByteBuffer buffer) {
        int numElements = DataUtils.readVarInt(buffer);
        List<String> elements = new ArrayList<>(numElements);
        for (int i = 0; i < numElements; i++) {
            elements.add(StringDataType.INSTANCE.read(buffer));
        }
        Path p;
        if (elements.size() == 1 && !elements.get(0).isEmpty()) {
            p = Path.NULL;
        } else {
            p = Path.ROOT;
            for (int i = elements.size() - 2; i >= 0; i--) {
                p = new Path(p, elements.get(i));
            }
        }
        return p;
    }

    static void pathRevToBuffer(PathRev pr, WriteBuffer buffer) {
        pathToBuffer(pr.getPath(), buffer);
        revisionVectorToBuffer(pr.getRevision(), buffer);
    }

    static PathRev pathRevFromBuffer(ByteBuffer buffer) {
        return new PathRev(
                pathFromBuffer(buffer),
                revisionVectorFromBuffer(buffer)
        );
    }
}
