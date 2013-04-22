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
package org.apache.jackrabbit.mongomk;

/**
 * A revision.
 */
public class Revision {

    static long timestampOffset = java.sql.Timestamp.valueOf("2013-01-01 00:00:00.0").getTime() / 100;
    static volatile long lastTimestamp;
    static volatile int count;
    
    /**
     * The timestamp in milliseconds since 2013 (unlike in seconds since 1970 as
     * in MongoDB).
     */
    private long timestamp;
    
    /**
     * An incrementing counter, for commits that occur within the same
     * millisecond.
     */
    private int counter;
    
    /**
     * The cluster id (the MongoDB machine id).
     */
    private int clusterId;
    
    public Revision(long timestamp, int counter, int clusterId) {
        this.timestamp = timestamp;
        this.counter = counter;
        this.clusterId = clusterId;
    }
    
    /**
     * Compare the time part of two revisions.
     * 
     * @return -1 if this revision occurred earlier, 1 if later, 0 if equal
     */
    int compareRevisionTime(Revision other) {
        int comp = timestamp < other.timestamp ? -1 : timestamp > other.timestamp ? 1 : 0;
        if (comp == 0) {
            comp = counter < other.counter ? -1 : counter > other.counter ? 1 : 0;
        }
        return comp;
    }
    
    /**
     * Create a simple revision id. The format is similar to MongoDB ObjectId.
     * 
     * @param clusterId the unique machineId + processId
     * @return the unique revision id
     */
    static Revision newRevision(int clusterId) {
        long timestamp = getCurrentTimestamp();
        int c;
        synchronized (Revision.class) {
            if (timestamp > lastTimestamp) {
                lastTimestamp = timestamp;
                c = count = 0;
            } else if (timestamp < lastTimestamp) {
                timestamp = lastTimestamp;
                c = ++count;
            } else {
                c = ++count;
            }
            if (c >= 0xfff) {
                timestamp++;
                c = 0;
                lastTimestamp = Math.max(timestamp, lastTimestamp);
            }
        }
        return new Revision(timestamp, c, clusterId);
    }
    
    /**
     * Get the timestamp value of the current date and time.
     * 
     * @return the timestamp
     */
    public static long getCurrentTimestamp() {
        return System.currentTimeMillis() / 100 - timestampOffset;
    }
    
    /**
     * Get the difference between two timestamps (a - b) in milliseconds.
     * 
     * @param a the first timestamp
     * @param b the second timestamp
     * @return the difference in milliseconds
     */
    public static long getTimestampDifference(long a, long b) {
        return (a - b) * 100;
    }
    
    public static Revision fromString(String rev) {
        if (!rev.startsWith("r")) {
            throw new IllegalArgumentException(rev);
        }
        int idxCount = rev.indexOf('-');
        if (idxCount < 0) {
            throw new IllegalArgumentException(rev);
        }
        int idxClusterId = rev.indexOf('-', idxCount + 1);
        if (idxClusterId < 0) {
            throw new IllegalArgumentException(rev);
        }
        String t = rev.substring(1, idxCount);
        long timestamp = Long.parseLong(t, 16);
        t = rev.substring(idxCount + 1, idxClusterId);
        int c = Integer.parseInt(t, 16);
        t = rev.substring(idxClusterId + 1);
        int clusterId = Integer.parseInt(t, 16);
        Revision r = new Revision(timestamp, c, clusterId);
        return r;
    }
    
    public String toString() {
        return new StringBuilder("r").
                append(Long.toHexString(timestamp)).
                append('-').
                append(Integer.toHexString(counter)).
                append('-').
                append(Integer.toHexString(clusterId)).
                toString();
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public int hashCode() {
        return (int) (timestamp >>> 32) ^ (int) timestamp ^ counter ^ clusterId;
    }
    
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other.getClass() != this.getClass()) {
            return false;
        }
        Revision r = (Revision) other;
        return r.timestamp == this.timestamp && 
                r.counter == this.counter && 
                r.clusterId == this.clusterId;
    }

    public int getClusterId() {
        return clusterId;
    }

}
