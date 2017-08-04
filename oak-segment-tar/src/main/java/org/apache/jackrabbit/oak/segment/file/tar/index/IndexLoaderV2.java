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

package org.apache.jackrabbit.oak.segment.file.tar.index;

import static java.nio.ByteBuffer.wrap;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

class IndexLoaderV2 {

    static final int MAGIC = ('\n' << 24) + ('0' << 16) + ('K' << 8) + '\n';

    private final int blockSize;

    IndexLoaderV2(int blockSize) {
        this.blockSize = blockSize;
    }

    Index loadIndex(RandomAccessFile file) throws InvalidIndexException, IOException {
        long length = file.length();
        // read the index metadata just before the two final zero blocks
        ByteBuffer meta = ByteBuffer.allocate(IndexV2.FOOTER_SIZE);
        file.seek(length - 2 * blockSize - IndexV2.FOOTER_SIZE);
        file.readFully(meta.array());
        int crc32 = meta.getInt();
        int count = meta.getInt();
        int bytes = meta.getInt();
        int magic = meta.getInt();

        if (magic != MAGIC) {
            throw new InvalidIndexException("Magic number mismatch");
        }

        if (count < 1 || bytes < count * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE || bytes % blockSize != 0) {
            throw new InvalidIndexException("Invalid metadata");
        }

        // this involves seeking backwards in the file, which might not
        // perform well, but that's OK since we only do this once per file
        ByteBuffer index = ByteBuffer.allocate(count * IndexEntryV2.SIZE);
        file.seek(length - 2 * blockSize - IndexV2.FOOTER_SIZE - count * IndexEntryV2.SIZE);
        file.readFully(index.array());
        index.mark();

        CRC32 checksum = new CRC32();
        checksum.update(index.array());
        if (crc32 != (int) checksum.getValue()) {
            throw new InvalidIndexException("Invalid checksum");
        }

        long limit = length - 2 * blockSize - bytes - blockSize;
        long lastMsb = Long.MIN_VALUE;
        long lastLsb = Long.MIN_VALUE;
        byte[] entry = new byte[IndexEntryV2.SIZE];
        for (int i = 0; i < count; i++) {
            index.get(entry);

            ByteBuffer buffer = wrap(entry);
            long msb = buffer.getLong();
            long lsb = buffer.getLong();
            int offset = buffer.getInt();
            int size = buffer.getInt();

            if (lastMsb > msb || (lastMsb == msb && lastLsb > lsb)) {
                throw new InvalidIndexException("Incorrect entry ordering");
            }
            if (lastMsb == msb && lastLsb == lsb && i > 0) {
                throw new InvalidIndexException("Duplicate entry");
            }
            if (offset < 0 || offset % blockSize != 0) {
                throw new InvalidIndexException("Invalid entry offset");
            }
            if (size < 1 || offset + size > limit) {
                throw new InvalidIndexException("Invalid entry size");
            }

            lastMsb = msb;
            lastLsb = lsb;
        }

        index.reset();

        return new IndexV2(index);
    }

}
