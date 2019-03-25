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

import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.PathNameRev;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.h2.mvstore.WriteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CacheTypeTest {

    @Test
    public void pathRev() {
        Path p = Path.fromString("/foo/bar/quux");
        RevisionVector rv = RevisionVector.fromString("r3-4-1,br4-9-2");
        PathRev expected = new PathRev(p, rv);
        WriteBuffer buffer = new WriteBuffer(1024);
        CacheType.NODE.writeKey(buffer, expected);
        ByteBuffer readBuffer = buffer.getBuffer();
        readBuffer.rewind();
        PathRev pr = CacheType.NODE.readKey(readBuffer);
        assertEquals(expected, pr);
    }

    @Test
    public void pathNameRev() {
        Path p = Path.fromString("/foo/bar/quux");
        String name = "baz";
        RevisionVector rv = RevisionVector.fromString("r3-4-1,br4-9-2");
        PathNameRev expected = new PathNameRev(p, name, rv);
        WriteBuffer buffer = new WriteBuffer(1024);
        CacheType.CHILDREN.writeKey(buffer, expected);
        ByteBuffer readBuffer = buffer.getBuffer();
        readBuffer.rewind();
        PathNameRev pr = CacheType.CHILDREN.readKey(readBuffer);
        assertEquals(expected, pr);
    }
}
