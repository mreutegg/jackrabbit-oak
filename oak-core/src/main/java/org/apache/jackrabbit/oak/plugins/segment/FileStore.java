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
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Charsets.UTF_8;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Maps;

public class FileStore implements SegmentStore {

    private final Map<String, Journal> journals = Maps.newHashMap();

    private final ByteBuffer tar;

    private final ConcurrentMap<UUID, Segment> segments =
            Maps.newConcurrentMap();

    public FileStore(String filename, NodeState root) {
        try {
            RandomAccessFile file = new RandomAccessFile(filename, "rw");
            try {
                tar = file.getChannel().map(READ_WRITE, 0, 1024 * 1024 * 1024);
            } finally {
                file.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        journals.put("root", new MemoryJournal(this, root));
    }

    public FileStore(NodeState root) {
        this("data.tar", root);
    }

    public FileStore(String filename) {
        this(filename, EMPTY_NODE);
    }

    public FileStore() {
        this(EMPTY_NODE);
    }

    @Override
    public synchronized Journal getJournal(final String name) {
        Journal journal = journals.get(name);
        if (journal == null) {
            journal = new MemoryJournal(this, "root");
            journals.put(name, journal);
        }
        return journal;
    }

    @Override
    public Segment readSegment(UUID id) {
        Segment segment = segments.get(id);
        if (segment != null) {
            return segment;
        } else {
            throw new IllegalArgumentException("Segment not found: " + id);
        }
    }

    @Override
    public void createSegment(
            UUID segmentId, byte[] data, int offset, int length,
            Collection<UUID> referencedSegmentIds,
            Map<String, RecordId> strings, Map<Template, RecordId> templates) {
        tar.put(segmentId.toString().getBytes(UTF_8));
        tar.putInt(referencedSegmentIds.size());
        for (UUID referencedSegmentId : referencedSegmentIds) {
            tar.putLong(referencedSegmentId.getMostSignificantBits());
            tar.putLong(referencedSegmentId.getLeastSignificantBits());
        }
        tar.putInt(length);
        int position = tar.position();
        tar.put(data, offset, length);

        ByteBuffer buffer = tar.asReadOnlyBuffer();
        buffer.position(position);
        buffer.limit(position + length);
        buffer = buffer.slice();

        Segment segment = new Segment(
                this, segmentId, buffer,
                referencedSegmentIds, strings, templates);
        if (segments.putIfAbsent(segmentId, segment) != null) {
            throw new IllegalStateException(
                    "Segment override: " + segmentId);
        }
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        if (segments.remove(segmentId) == null) {
            throw new IllegalStateException("Missing segment: " + segmentId);
        }
    }

}
