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
package org.apache.jackrabbit.mongomk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.Revision.RevisionComparator;
import org.junit.Test;

/**
 * Tests the revision class
 */
public class RevisionTest {

    @Test
    public void fromStringToString() {
        for (int i = 0; i < 10000; i++) {
            Revision r = Revision.newRevision(i);
            // System.out.println(r);
            Revision r2 = Revision.fromString(r.toString());
            assertEquals(r.toString(), r2.toString());
            assertEquals(r.hashCode(), r2.hashCode());
            assertTrue(r.equals(r2));
        }
    }
    
    @Test
    public void difference() throws InterruptedException {
        Revision r0 = Revision.newRevision(0);
        Revision r1 = Revision.newRevision(0);
        long timestamp = Revision.getCurrentTimestamp();
        assertTrue(Revision.getTimestampDifference(r1.getTimestamp(), r0.getTimestamp()) < 10);
        assertTrue(Revision.getTimestampDifference(timestamp, r0.getTimestamp()) < 10);
        Thread.sleep(2);
        Revision r2 = Revision.newRevision(0);
        assertTrue(Revision.getTimestampDifference(r2.getTimestamp(), r0.getTimestamp()) > 0);
        assertTrue(Revision.getTimestampDifference(r2.getTimestamp(), r0.getTimestamp()) < 20);
    }
    
    @Test
    public void equalsHashCode() {
        Revision a = Revision.newRevision(0);
        Revision b = Revision.newRevision(0);
        assertTrue(a.equals(a));
        assertFalse(a.equals(b));
        assertFalse(b.equals(a));
        assertFalse(a.hashCode() == b.hashCode());
        Revision a1 = Revision.fromString(a.toString());
        assertTrue(a.equals(a1));
        assertTrue(a1.equals(a));
        Revision a2 = new Revision(a.getTimestamp(), a.getCounter(), a.getClusterId());
        assertTrue(a.equals(a2));
        assertTrue(a2.equals(a));
        assertEquals(a.hashCode(), a1.hashCode());
        assertEquals(a.hashCode(), a2.hashCode());
        Revision x1 = new Revision(a.getTimestamp() + 1, a.getCounter(), a.getClusterId());
        assertFalse(a.equals(x1));
        assertFalse(x1.equals(a));
        assertFalse(a.hashCode() == x1.hashCode());
        Revision x2 = new Revision(a.getTimestamp(), a.getCounter() + 1, a.getClusterId());
        assertFalse(a.equals(x2));
        assertFalse(x2.equals(a));
        assertFalse(a.hashCode() == x2.hashCode());
        Revision x3 = new Revision(a.getTimestamp(), a.getCounter(), a.getClusterId() + 1);
        assertFalse(a.equals(x3));
        assertFalse(x3.equals(a));
        assertFalse(a.hashCode() == x3.hashCode());
    }
    
    @Test
    public void compare() throws InterruptedException {
        Revision last = Revision.newRevision(0);
        try {
            last.compareRevisionTime(null);
            fail();
        } catch (NullPointerException e) {
            // expected
        }
        for (int i = 0; i < 1000; i++) {
            Revision r = Revision.newRevision(0);
            assertTrue(r.compareRevisionTime(r) == 0);
            assertTrue(r.compareRevisionTime(last) > 0);
            assertTrue(last.compareRevisionTime(r) < 0);
            last = r;
            if (i % 100 == 0) {
                // ensure the timestamp part changes as well
                Thread.sleep(1);
            }
        }
    }
    
    @Test
    public void revisionComparatorSimple() {
        RevisionComparator comp = new RevisionComparator();
        Revision r1 = Revision.newRevision(0);
        Revision r2 = Revision.newRevision(0);
        assertEquals(r1.compareRevisionTime(r2), comp.compare(r1, r2));
        assertEquals(r2.compareRevisionTime(r1), comp.compare(r2, r1));
        assertEquals(r1.compareRevisionTime(r1), comp.compare(r1, r1));
    }
    
    @Test
    public void revisionComparatorCluster() {
        
        RevisionComparator comp = new RevisionComparator();
        
        Revision r1c1 = new Revision(0x110, 0, 1);
        Revision r2c1 = new Revision(0x120, 0, 1);
        Revision r3c1 = new Revision(0x130, 0, 1);
        Revision r1c2 = new Revision(0x100, 0, 2);
        Revision r2c2 = new Revision(0x200, 0, 2);
        Revision r3c2 = new Revision(0x300, 0, 2);

        // first, only timestamps are compared
        assertEquals(1, comp.compare(r1c1, r1c2));
        assertEquals(-1, comp.compare(r2c1, r2c2));
        assertEquals(-1, comp.compare(r3c1, r3c2));

        // now we declare r2+r3 of c1 to be after r2+r3 of c2
        comp.add(r2c1, 20);
        comp.add(r2c2, 10);

        assertEquals(
                "1: r120-0-1:20; " + 
                "2: r200-0-2:10; ", comp.toString());

        assertEquals(1, comp.compare(r1c1, r1c2));
        assertEquals(1, comp.compare(r2c1, r2c2));
        assertEquals(1, comp.compare(r3c1, r3c2));
        
        // now we declare r3 of c1 to be before r3 of c2
        // (with the same range timestamp, 
        // the revision timestamps are compared)
        comp.add(r3c1, 30);
        comp.add(r3c2, 30);

        assertEquals(
                "1: r120-0-1:20 r130-0-1:30; " + 
                "2: r200-0-2:10 r300-0-2:30; ", comp.toString());

        assertEquals(1, comp.compare(r1c1, r1c2));
        assertEquals(1, comp.compare(r2c1, r2c2));
        assertEquals(-1, comp.compare(r3c1, r3c2));
        // reverse
        assertEquals(-1, comp.compare(r1c2, r1c1));
        assertEquals(-1, comp.compare(r2c2, r2c1));
        assertEquals(1, comp.compare(r3c2, r3c1));
        
        // get rid of old timestamps
        comp.purge(10);
        assertEquals(
                "1: r120-0-1:20 r130-0-1:30; " + 
                "2: r300-0-2:30; ", comp.toString());
        comp.purge(20);
        assertEquals(
                "1: r130-0-1:30; " + 
                "2: r300-0-2:30; ", comp.toString());
        
        // update an entry
        comp.add(new Revision(0x301, 1, 2), 30);
        assertEquals(
                "1: r130-0-1:30; " + 
                "2: r301-1-2:30; ", comp.toString());
        
        comp.purge(30);
        assertEquals("", comp.toString());

    }

}
