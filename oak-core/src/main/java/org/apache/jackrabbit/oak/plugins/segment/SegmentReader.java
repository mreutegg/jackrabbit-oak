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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;

import java.io.IOException;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;

public class SegmentReader {

    private final SegmentStore store;

    public SegmentReader(SegmentStore store) {
        this.store = store;
    }

    public String readString(RecordId recordId) {
        SegmentStream stream = readStream(recordId);
        try {
            if (stream.getLength() > Integer.MAX_VALUE) {
                throw new IllegalStateException(
                        "Too long value: " + stream.getLength());
            }
            byte[] data = new byte[(int) stream.getLength()];
            ByteStreams.readFully(stream, data);
            return new String(data, Charsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected IOException", e);
        } finally {
            stream.close();
        }
    }

    public SegmentStream readStream(RecordId recordId) {
        Segment segment = store.readSegment(recordId.getSegmentId());
        int offset = recordId.getOffset();
        long length = segment.readLong(offset);
        int size = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
        ListRecord list =
            new ListRecord(segment.readRecordId(offset + 8), size);
        return new SegmentStream(this, list, length);
    }

    public int readInt(RecordId recordId, int position) {
        checkNotNull(recordId);
        checkArgument(position >= 0);
        Segment segment = store.readSegment(recordId.getSegmentId());
        return segment.readInt(recordId.getOffset() + position);
    }

    public long readLong(RecordId recordId, int position) {
        checkNotNull(recordId);
        checkArgument(position >= 0);
        Segment segment = store.readSegment(recordId.getSegmentId());
        return segment.readLong(recordId.getOffset() + position);
    }

    public void readBytes(
            RecordId recordId, int position,
            byte[] buffer, int offset, int length) {
        checkNotNull(recordId);
        checkArgument(position >= 0);
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);

        Segment segment = store.readSegment(recordId.getSegmentId());
        segment.readBytes(
                recordId.getOffset() + position, buffer, offset, length);
    }

    public RecordId readRecordId(RecordId recordId, int position) {
        checkNotNull(recordId);
        checkArgument(position >= 0);

        Segment segment = store.readSegment(recordId.getSegmentId());
        return segment.readRecordId(recordId.getOffset() + position);
    }

    public ListRecord readList(RecordId recordId, int numberOfEntries) {
        checkNotNull(recordId);
        checkArgument(numberOfEntries >= 0);

        if (numberOfEntries > 0) {
            Segment segment = store.readSegment(recordId.getSegmentId());
            RecordId id = segment.readRecordId(recordId.getOffset());
            return new ListRecord(id, numberOfEntries);
        } else {
            return new ListRecord(recordId, numberOfEntries);
        }
    }

    public BlockRecord readBlock(RecordId recordId, int size) {
        checkNotNull(recordId);
        checkArgument(size > 0);
        return new BlockRecord(recordId, size);
    }

}
