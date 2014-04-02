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
package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentTracker;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStore implements SegmentStore {

    private static final Logger log = LoggerFactory.getLogger(FileStore.class);

    private static final int MB = 1024 * 1024;

    private static final Pattern FILE_NAME_PATTERN =
            Pattern.compile("(data|bulk)((0|[1-9][0-9]*)[0-9]{4})(a?).tar");

    private static final String FILE_NAME_FORMAT = "data%05d%s.tar";

    private static final String JOURNAL_FILE_NAME = "journal.log";

    private static final boolean MEMORY_MAPPING_DEFAULT =
            "64".equals(System.getProperty("sun.arch.data.model", "32"));

    private final SegmentTracker tracker;

    private final File directory;

    private final BlobStore blobStore;

    private final int maxFileSize;

    private final boolean memoryMapping;

    private volatile List<TarReader> readers;

    private int writeNumber;

    private File writeFile;

    private TarWriter writer;

    private final RandomAccessFile journalFile;

    /**
     * The latest head state.
     */
    private final AtomicReference<RecordId> head;

    /**
     * The persisted head of the root journal, used to determine whether the
     * latest {@link #head} value should be written to the disk.
     */
    private final AtomicReference<RecordId> persistedHead;

    /**
     * The background flush thread. Automatically flushes the TarMK state
     * once every five seconds.
     */
    private final Thread flushThread;

    /**
     * Synchronization aid used by the background flush thread to stop itself
     * as soon as the {@link #close()} method is called.
     */
    private final CountDownLatch timeToClose = new CountDownLatch(1);

    public FileStore(BlobStore blobStore, File directory, int maxFileSizeMB, boolean memoryMapping)
            throws IOException {
        this(blobStore, directory, EMPTY_NODE, maxFileSizeMB, 0, memoryMapping);
    }

    public FileStore(File directory, int maxFileSizeMB, boolean memoryMapping)
            throws IOException {
        this(null, directory, maxFileSizeMB, memoryMapping);
    }

    public FileStore(File directory, int maxFileSizeMB)
            throws IOException {
        this(null, directory, maxFileSizeMB, MEMORY_MAPPING_DEFAULT);
    }

    public FileStore(File directory, int maxFileSizeMB, int cacheSizeMB,
            boolean memoryMapping) throws IOException {
        this(null, directory, EMPTY_NODE, maxFileSizeMB, cacheSizeMB, memoryMapping);
    }

    public FileStore(
            BlobStore blobStore, final File directory, NodeState initial,
            int maxFileSizeMB, int cacheSizeMB, boolean memoryMapping)
            throws IOException {
        checkNotNull(directory).mkdirs();
        if (cacheSizeMB > 0) {
            this.tracker = new SegmentTracker(this, cacheSizeMB);
        } else {
            this.tracker = new SegmentTracker(this);
        }
        this.blobStore = blobStore;
        this.directory = directory;
        this.maxFileSize = maxFileSizeMB * MB;
        this.memoryMapping = memoryMapping;

        Map<Integer, File> map = collectFiles(directory);
        this.readers = newArrayListWithCapacity(map.size());
        Integer[] indices = map.keySet().toArray(new Integer[map.size()]);
        Arrays.sort(indices);
        for (int i = indices.length - 1; i >= 0; i--) {
            readers.add(TarReader.open(
                    singletonMap('a', map.get(indices[i])), memoryMapping));
        }

        if (indices.length > 0) {
            this.writeNumber = indices[indices.length - 1] + 1;
        } else {
            this.writeNumber = 0;
        }
        this.writeFile = new File(
                directory,
                String.format(FILE_NAME_FORMAT, writeNumber, "a"));
        this.writer = new TarWriter(writeFile);

        journalFile = new RandomAccessFile(
                new File(directory, JOURNAL_FILE_NAME), "rw");

        LinkedList<RecordId> heads = newLinkedList();
        String line = journalFile.readLine();
        while (line != null) {
            int space = line.indexOf(' ');
            if (space != -1) {
                heads.add(RecordId.fromString(tracker, line.substring(0, space)));
            }
            line = journalFile.readLine();
        }

        RecordId id = null;
        while (id == null && !heads.isEmpty()) {
            RecordId last = heads.removeLast();
            SegmentId segmentId = last.getSegmentId();
            if (containsSegment(
                    segmentId.getMostSignificantBits(),
                    segmentId.getLeastSignificantBits())) {
                id = last;
            } else {
                log.warn("Unable to access revision {}, rewinding...", last);
            }
        }

        if (id != null) {
            head = new AtomicReference<RecordId>(id);
            persistedHead = new AtomicReference<RecordId>(id);
        } else {
            NodeBuilder builder = EMPTY_NODE.builder();
            builder.setChildNode("root", initial);
            head = new AtomicReference<RecordId>(tracker.getWriter().writeNode(
                    builder.getNodeState()).getRecordId());
            persistedHead = new AtomicReference<RecordId>(null);
        }

        this.flushThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    timeToClose.await(1, SECONDS);
                    while (timeToClose.getCount() > 0) {
                        try {
                            flush();
                        } catch (IOException e) {
                            log.warn("Failed to flush the TarMK at" +
                                    directory, e);
                        }
                        timeToClose.await(5, SECONDS);
                    }
                } catch (InterruptedException e) {
                    log.warn("TarMK flush thread interrupted");
                }
            }
        });
        flushThread.setName("TarMK flush thread: " + directory);
        flushThread.setDaemon(true);
        flushThread.setPriority(Thread.MIN_PRIORITY);
        flushThread.start();
    }

    static SortedMap<Integer, File> collectFiles(File directory) throws IOException {
        SortedMap<Integer, File> dataFiles = newTreeMap();
        Map<Integer, File> bulkFiles = newHashMap();

        for (File file : directory.listFiles()) {
            Matcher matcher = FILE_NAME_PATTERN.matcher(file.getName());
            if (matcher.matches()) {
                Integer index = Integer.parseInt(matcher.group(2));
                if ("data".equals(matcher.group(1))) {
                    checkState(dataFiles.put(index, file) == null);
                } else {
                    checkState(bulkFiles.put(index, file) == null);
                }
            }
        }

        if (!bulkFiles.isEmpty()) {
            log.info("Upgrading TarMK file names in {}", directory);

            if (!dataFiles.isEmpty()) {
                // first put all the data segments at the end of the list
                Integer[] indices =
                        dataFiles.keySet().toArray(new Integer[dataFiles.size()]);
                int position = Math.max(
                        indices[indices.length - 1] + 1,
                        bulkFiles.size());
                for (Integer index : indices) {
                    File file = dataFiles.remove(index);
                    Integer newIndex = position++;
                    File newFile = new File(
                            directory, format(FILE_NAME_FORMAT, newIndex, "a"));
                    log.info("Renaming {} to {}", file, newFile);
                    file.renameTo(newFile);
                    dataFiles.put(newIndex, newFile);
                }
            }

            // then add all the bulk segments at the beginning of the list
            Integer[] indices =
                    bulkFiles.keySet().toArray(new Integer[bulkFiles.size()]);
            Arrays.sort(indices);
            int position = 0;
            for (Integer index : indices) {
                File file = bulkFiles.remove(index);
                Integer newIndex = position++;
                File newFile = new File(
                        directory, format(FILE_NAME_FORMAT, newIndex, "a"));
                log.info("Renaming {} to {}", file, newFile);
                file.renameTo(newFile);
                dataFiles.put(newIndex, newFile);
            }
        }

        return dataFiles;
    }

    public void flush() throws IOException {
        synchronized (persistedHead) {
            RecordId before = persistedHead.get();
            RecordId after = head.get();
            if (!after.equals(before)) {
                // needs to happen outside the synchronization block below to
                // avoid a deadlock with another thread flushing the writer
                tracker.getWriter().flush();

                synchronized (this) {
                    writer.flush();
                    journalFile.writeBytes(after + " root\n");
                    journalFile.getChannel().force(false);
                    persistedHead.set(after);
                }
            }
        }
    }

    public synchronized Iterable<SegmentId> getSegmentIds() {
        List<SegmentId> ids = newArrayList();
        for (UUID uuid : writer.getUUIDs()) {
            ids.add(tracker.getSegmentId(
                    uuid.getMostSignificantBits(),
                    uuid.getLeastSignificantBits()));
        }
        for (TarReader reader : readers) {
            for (UUID uuid : reader.getUUIDs()) {
                ids.add(tracker.getSegmentId(
                        uuid.getMostSignificantBits(),
                        uuid.getLeastSignificantBits()));
            }
        }
        return ids;
    }

    @Override
    public SegmentTracker getTracker() {
        return tracker;
    }

    @Override
    public SegmentNodeState getHead() {
        return new SegmentNodeState(head.get());
    }

    @Override
    public boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        RecordId id = this.head.get();
        return id.equals(base.getRecordId())
                && this.head.compareAndSet(id, head.getRecordId());
    }

    @Override
    public void close() {
        try {
            // avoid deadlocks while joining the flush thread
            timeToClose.countDown();
            try {
                flushThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while joining the TarMK flush thread", e);
            }

            synchronized (this) {
                flush();

                writer.close();
                journalFile.close();

                List<TarReader> list = readers;
                readers = newArrayList();
                for (TarReader reader : list) {
                    reader.close();
                }

                System.gc(); // for any memory-mappings that are no longer used
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to close the TarMK at " + directory, e);
        }
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        if (id.getTracker() == tracker) {
            return true;
        }

        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();
        return containsSegment(msb, lsb);
    }

    private boolean containsSegment(long msb, long lsb) {
        for (TarReader reader : readers) {
            if (reader.containsEntry(msb, lsb)) {
                return true;
            }
        }
        synchronized (this) {
            return writer.containsEntry(msb, lsb);
        }
    }

    @Override
    public Segment readSegment(SegmentId id) {
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();

        for (TarReader reader : readers) {
            try {
                ByteBuffer buffer = reader.readEntry(msb, lsb);
                if (buffer != null) {
                    return new Segment(tracker, id, buffer);
                }
            } catch (IOException e) {
                log.warn("Failed to read from tar file " + reader, e);
            }
        }

        synchronized (this) {
            ByteBuffer buffer = writer.readEntry(msb, lsb);
            if (buffer != null) {
                return new Segment(tracker, id, buffer);
            }
        }

        for (TarReader reader : readers) {
            try {
                ByteBuffer buffer = reader.readEntry(msb, lsb);
                if (buffer != null) {
                    return new Segment(tracker, id, buffer);
                }
            } catch (IOException e) {
                log.warn("Failed to read from tar file " + reader, e);
            }
        }

        throw new IllegalStateException("Segment " + id + " not found");
    }

    @Override
    public synchronized void writeSegment(
            SegmentId id, byte[] data, int offset, int length) {
        try {
            long size = writer.writeEntry(
                    id.getMostSignificantBits(),
                    id.getLeastSignificantBits(),
                    data, offset, length);
            if (size >= maxFileSize) {
                writer.close();

                List<TarReader> list =
                        newArrayListWithCapacity(1 + readers.size());
                list.add(new TarReader(writeFile, memoryMapping));
                list.addAll(readers);
                readers = list;

                writeNumber++;
                writeFile = new File(
                        directory,
                        String.format(FILE_NAME_FORMAT, writeNumber, "a"));
                writer = new TarWriter(writeFile);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Blob readBlob(String blobId) {
        if (blobStore != null) {
            return new BlobStoreBlob(blobStore, blobId);
        }
        throw new IllegalStateException("Attempt to read external blob with blobId [" + blobId + "] " +
                "without specifying BlobStore");
    }

    @Override
    public BlobStore getBlobStore() {
        return blobStore;
    }

    @Override
    public void gc() {
        System.gc();
        Set<SegmentId> ids = tracker.getReferencedSegmentIds();
        // TODO reclaim unreferenced segments
    }
}