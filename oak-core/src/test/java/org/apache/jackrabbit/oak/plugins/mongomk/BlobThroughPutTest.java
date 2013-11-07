package org.apache.jackrabbit.oak.plugins.mongomk;

import com.google.common.io.ByteStreams;
import com.mongodb.*;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNotNull;


public class BlobThroughPutTest {
    private static final int NO_OF_NODES = 100;
    private static final int BLOB_SIZE = 1024 * 1024 * 2;

    private static final String TEST_DB1 = "tptest1";
    private static final String TEST_DB2 = "tptest2";

    private static final int MAX_EXEC_TIME = 5; //In seconds
    private static final int[] READERS = {5, 10, 15, 20};
    private static final int[] WRITERS = {0, 1, 2, 4};

    private final List<Result> results = new ArrayList<Result>();

    @Ignore
    @Test
    public void performBenchMark() throws UnknownHostException, InterruptedException {
        Mongo local = new Mongo(new DBAddress("localhost:27017/test"));
        Mongo remote = new Mongo(new DBAddress("remote:27017/test"));

        run(local, false, false);
        run(local, true, false);
        run(remote, false, true);
        run(remote, true, true);

        dumpResult();
    }

    private void dumpResult() {
        PrintStream ps = System.out;

        ps.println(Result.OUTPUT_FORMAT);

        for (Result r : results) {
            ps.println(r.toString());
        }
    }

    private void run(Mongo mongo, boolean useSameDB, boolean remote) throws InterruptedException {
        final DB nodeDB = mongo.getDB(TEST_DB1);
        final DB blobDB = useSameDB ? mongo.getDB(TEST_DB1) : mongo.getDB(TEST_DB2);
        final DBCollection nodes = nodeDB.getCollection("nodes");
        final DBCollection blobs = blobDB.getCollection("blobs");


        final Benchmark b = new Benchmark(nodes, blobs);

        for (int readers : READERS) {
            for (int writers : WRITERS) {
                MongoUtils.dropCollections(nodeDB);
                MongoUtils.dropCollections(blobDB);

                createTestNodes(nodes);

                Result r = b.run(readers, writers, remote);
                results.add(r);
            }
        }
    }

    private void createTestNodes(DBCollection nodes) {
        for (int i = 0; i < NO_OF_NODES; i++) {
            DBObject obj = new BasicDBObject("_id", i)
                    .append("foo", "bar1" + i);
            nodes.insert(obj, WriteConcern.SAFE);
        }
    }

    private static class Result {
        final static String OUTPUT_FORMAT = "remote, samedb, readers, writers, reads, writes, " +
                "time, readThroughPut, writeThroughPut";
        int totalReads;
        int totalWrites = 0;
        int noOfReaders;
        int noOfWriters;
        int execTime;
        int dataSize = BLOB_SIZE;
        boolean sameDB;
        boolean remote;

        double readThroughPut() {
            return totalReads / execTime;
        }

        double writeThroughPut() {
            return totalWrites * dataSize / execTime;
        }

        @Override
        public String toString() {
            return String.format("%s,%s,%d,%d,%d,%d,%d,%1.0f,%s",
                    remote,
                    sameDB,
                    noOfReaders,
                    noOfWriters,
                    totalReads,
                    totalWrites,
                    execTime,
                    readThroughPut(),
                    humanReadableByteCount((long) writeThroughPut(), true));
        }
    }

    private static String humanReadableByteCount(long bytes, boolean si) {
        if (bytes < 0) {
            return "0";
        }
        int unit = si ? 1000 : 1024;
        if (bytes < unit) {
            return bytes + " B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    private static class Benchmark {
        private final DBCollection nodes;
        private final DBCollection blobs;
        private final Random random = new Random();
        private final AtomicBoolean stopTest = new AtomicBoolean(false);
        private final static byte[] DATA;
        private final CountDownLatch startLatch = new CountDownLatch(1);

        static {
            try {
                DATA = ByteStreams.toByteArray(new RandomStream(BLOB_SIZE, 100));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        private Benchmark(DBCollection nodes, DBCollection blobs) {
            this.nodes = nodes;
            this.blobs = blobs;
        }

        public Result run(int noOfReaders, int noOfWriters, boolean remote) throws InterruptedException {
            boolean sameDB = nodes.getDB().getName().equals(blobs.getDB().getName());

            List<Reader> readers = new ArrayList<Reader>(noOfReaders);
            List<Writer> writers = new ArrayList<Writer>(noOfWriters);
            List<Runnable> runnables = new ArrayList<Runnable>(noOfReaders + noOfWriters);
            final CountDownLatch stopLatch = new CountDownLatch(noOfReaders + noOfWriters);

            for (int i = 0; i < noOfReaders; i++) {
                readers.add(new Reader(stopLatch));
            }

            for (int i = 0; i < noOfWriters; i++) {
                writers.add(new Writer(i, stopLatch));
            }

            runnables.addAll(readers);
            runnables.addAll(writers);

            stopTest.set(false);

            List<Thread> threads = new ArrayList<Thread>();

            for (int i = 0; i < runnables.size(); i++) {
                Thread worker = new Thread(runnables.get(i));
                worker.start();
                threads.add(worker);
            }

            System.err.printf("Running with [%d] readers and [%d] writers. " +
                    "Same DB [%s], Remote server [%s], Max Time [%d] seconds %n",
                    noOfReaders, noOfWriters, sameDB, remote, MAX_EXEC_TIME);

            startLatch.countDown();

            TimeUnit.SECONDS.sleep(MAX_EXEC_TIME);
            stopTest.set(true);

            stopLatch.await();

            int totalReads = 0;
            for (Reader r : readers) {
                totalReads += r.readCount;
            }

            int totalWrites = 0;
            for (Writer w : writers) {
                totalWrites += w.writeCount;
            }

            Result r = new Result();
            r.execTime = MAX_EXEC_TIME;
            r.noOfReaders = noOfReaders;
            r.noOfWriters = noOfWriters;
            r.totalReads = totalReads;
            r.totalWrites = totalWrites;
            r.remote = remote;
            r.sameDB = sameDB;

            System.err.printf("Run complete. Reads [%d] and writes [%d] %n", totalReads, totalWrites);
            System.err.println(r.toString());
            return r;
        }

        private void waitForStart() {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        private class Reader implements Runnable {
            int readCount = 0;
            final CountDownLatch stopLatch;

            public Reader(CountDownLatch stopLatch) {
                this.stopLatch = stopLatch;
            }

            public void run() {
                waitForStart();
                while (!stopTest.get()) {
                    int id = random.nextInt(NO_OF_NODES);
                    DBObject o = nodes.findOne(QueryBuilder.start("_id").is(id).get());
                    assertNotNull("did not found object with id " + id, o);
                    readCount++;
                }
                stopLatch.countDown();
            }
        }

        private class Writer implements Runnable {
            int writeCount = 0;
            final int id;
            final CountDownLatch stopLatch;

            private Writer(int id, CountDownLatch stopLatch) {
                this.id = id;
                this.stopLatch = stopLatch;
            }

            public void run() {
                waitForStart();
                while (!stopTest.get()) {
                    String _id = id + "-" + writeCount;
                    DBObject obj = new BasicDBObject()
                            .append("foo", _id);
                    obj.put("blob", DATA);
                    blobs.insert(obj, WriteConcern.SAFE);
                    writeCount++;
                }
                stopLatch.countDown();
            }
        }
    }
}