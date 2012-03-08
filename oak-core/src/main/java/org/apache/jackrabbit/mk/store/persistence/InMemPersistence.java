/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.store.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Commit;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.Node;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.store.BinaryBinding;
import org.apache.jackrabbit.mk.store.Binding;
import org.apache.jackrabbit.mk.store.IdFactory;
import org.apache.jackrabbit.mk.store.NotFoundException;

/**
 *
 */
public class InMemPersistence implements Persistence, BlobStore {

    private final Map<Id, byte[]> nodes = Collections.synchronizedMap(new HashMap<Id, byte[]>());
    private final Map<Id, StoredCommit> commits = Collections.synchronizedMap(new HashMap<Id, StoredCommit>());
    private final Map<Id, ChildNodeEntriesMap> cneMaps = Collections.synchronizedMap(new HashMap<Id, ChildNodeEntriesMap>());
    private final BlobStore blobs = new MemoryBlobStore();

    private Id head;

    // TODO: make this configurable
    private IdFactory idFactory = IdFactory.getDigestFactory();
    
    public void initialize(File homeDir) throws Exception {
        head = null;
    }

    public void close() {
    }

    public Id readHead() throws Exception {
        return head;
    }

    public void writeHead(Id id) throws Exception {
        head = id;
    }

    public Binding readNodeBinding(Id id) throws NotFoundException, Exception {
        byte[] bytes = nodes.get(id);
        if (bytes != null) {
            return new BinaryBinding(new ByteArrayInputStream(bytes));
        } else {
            throw new NotFoundException(id.toString());
        }
    }

    public Id writeNode(Node node) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        node.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        Id id = new Id(idFactory.createContentId(bytes));

        if (!nodes.containsKey(id)) {
            nodes.put(id, bytes);
        }

        return id;
    }

    public StoredCommit readCommit(Id id) throws NotFoundException, Exception {
        StoredCommit commit = commits.get(id);
        if (commit != null) {
            return commit;
        } else {
            throw new NotFoundException(id.toString());
        }
    }

    public void writeCommit(Id id, Commit commit) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        commit.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();

        if (!commits.containsKey(id)) {
            commits.put(id, StoredCommit.deserialize(id.toString(), new BinaryBinding(new ByteArrayInputStream(bytes))));
        }
    }

    public ChildNodeEntriesMap readCNEMap(Id id) throws NotFoundException, Exception {
        ChildNodeEntriesMap map = cneMaps.get(id);
        if (map != null) {
            return map;
        } else {
            throw new NotFoundException(id.toString());
        }
    }

    public Id writeCNEMap(ChildNodeEntriesMap map) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        map.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        Id id = new Id(idFactory.createContentId(bytes));

        if (!cneMaps.containsKey(id)) {
            cneMaps.put(id, ChildNodeEntriesMap.deserialize(new BinaryBinding(new ByteArrayInputStream(bytes))));
        }

        return id;
    }
    
    //------------------------------------------------------------< BlobStore >

    public String addBlob(String tempFilePath) throws Exception {
        return blobs.addBlob(tempFilePath);
    }

    public String writeBlob(InputStream in) throws Exception {
        return blobs.writeBlob(in);
    }

    public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws NotFoundException, Exception {
        return blobs.readBlob(blobId, pos, buff, off, length);
    }

    public long getBlobLength(String blobId) throws Exception {
        return blobs.getBlobLength(blobId);
    }
}
