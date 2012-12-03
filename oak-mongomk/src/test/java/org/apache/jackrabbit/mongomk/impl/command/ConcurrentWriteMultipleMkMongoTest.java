package org.apache.jackrabbit.mongomk.impl.command;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.blob.MongoGridFSBlobStore;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.DB;

/**
 * Tests for multiple MongoMKs writing against the same DB in separate trees.
 */
public class ConcurrentWriteMultipleMkMongoTest extends BaseMongoMicroKernelTest {

    @Test
    public void testSmall() throws Exception {
        doTest(1000);
    }

    @Test
    @Ignore // Ignored only because it takes a while to complete.
    public void testLarge() throws Exception {
        doTest(10000);
    }

    private void doTest(int numberOfNodes) throws Exception {

        int numberOfChildren = 10;
        int numberOfMks = 3;
        String[] prefixes = new String[]{"a", "b", "c", "d", "e", "f"};

        ExecutorService executor = Executors.newFixedThreadPool(numberOfMks);
        for (int i = 0; i < numberOfMks; i++) {
            String diff = buildPyramidDiff("/", 0, numberOfChildren,
                    numberOfNodes, prefixes[i], new StringBuilder()).toString();
            //System.out.println(diff);
            DB db = mongoConnection.getDB();
            MongoMicroKernel mk = new MongoMicroKernel(mongoConnection,
                    new MongoNodeStore(db), new MongoGridFSBlobStore(db));
            GenericWriteTask task = new GenericWriteTask("mk-" + i, mk, diff, 10);
            executor.execute(task);
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);
    }

    private StringBuilder buildPyramidDiff(String startingPoint,
            int index, int numberOfChildren, long nodesNumber,
            String nodePrefixName, StringBuilder diff) {
        if (numberOfChildren == 0) {
            for (long i = 0; i < nodesNumber; i++)
                diff.append(addNodeToDiff(startingPoint, nodePrefixName + i));
            return diff;
        }

        if (index >= nodesNumber) {
            return diff;
        }

        diff.append(addNodeToDiff(startingPoint, nodePrefixName + index));
        for (int i = 1; i <= numberOfChildren; i++) {
            if (!startingPoint.endsWith("/"))
                startingPoint = startingPoint + "/";
            buildPyramidDiff(startingPoint + nodePrefixName + index, index
                    * numberOfChildren + i, numberOfChildren, nodesNumber,
                    nodePrefixName, diff);
        }
        return diff;
    }

    private String addNodeToDiff(String startingPoint, String nodeName) {
        if (!startingPoint.endsWith("/")) {
            startingPoint = startingPoint + "/";
        }
        return ("+\"" + startingPoint + nodeName + "\" : {} \n");
    }

    private static class GenericWriteTask implements Runnable {

        private String id;
        private MicroKernel mk;
        private String diff;
        private int nodesPerCommit;

        public GenericWriteTask(String id, MongoMicroKernel mk, String diff,
                int nodesPerCommit) {
            this.id = id;
            this.mk = mk;
            this.diff = diff;
            this.nodesPerCommit = nodesPerCommit;
        }

        @Override
        public void run() {
            if (nodesPerCommit == 0) {
                mk.commit("", diff.toString(), null, "");
                return;
            }

            int i = 0;
            StringBuilder currentCommit = new StringBuilder();
            String[] diffs = diff.split(System.getProperty("line.separator"));
            for (String diff : diffs) {
                currentCommit.append(diff);
                i++;
                if (i == nodesPerCommit) {
                    //System.out.println("[" + id + "] Committing: " + currentCommit.toString());
                    String rev = mk.commit("", currentCommit.toString(), null, null);
                    //System.out.println("[" + id + "] Committed-" + rev + ":" + currentCommit.toString());
                    currentCommit.setLength(0);
                    i = 0;
                }
            }
            // Commit remaining nodes
            if (currentCommit.length() > 0)
                mk.commit("", currentCommit.toString(), null, null);
        }
    }
}