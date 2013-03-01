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
package org.apache.jackrabbit.mongomk.prototype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mongomk.prototype.DocumentStore.Collection;
import org.apache.jackrabbit.mongomk.prototype.Node.Children;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.mongodb.DB;

/**
 * A set of simple tests.
 */
public class SimpleTest {
    
    private static final boolean MONGO_DB = false;
//    private static final boolean MONGO_DB = true;

    @Test
    public void test() {
        MongoMK mk = new MongoMK();
        mk.dispose();
    }
    
    @Test
    public void revision() {
        for (int i = 0; i < 100; i++) {
            Revision r = Revision.newRevision(i);
            // System.out.println(r);
            Revision r2 = Revision.fromString(r.toString());
            assertEquals(r.toString(), r2.toString());
            assertEquals(r.hashCode(), r2.hashCode());
            assertTrue(r.equals(r2));
        }
    }
    
    @Test
    public void addNodeGetNode() {
        MongoMK mk = new MongoMK();
        Revision rev = mk.newRevision();
        Node n = new Node("/test", rev);
        n.setProperty("name", "Hello");
        UpdateOp op = n.asOperation(true);
        DocumentStore s = mk.getDocumentStore();
        assertTrue(s.create(Collection.NODES, Lists.newArrayList(op)));
        Node n2 = mk.getNode("/test", rev);
        assertEquals("Hello", n2.getProperty("name"));
        mk.dispose();
    }

    @Test
    public void reAddDeleted() {
        MongoMK mk = createMK();
        String rev = mk.commit("/", "+\"test\":{\"name\": \"Hello\"}", null, null);
        rev = mk.commit("/", "-\"test\"", rev, null);
        rev = mk.commit("/", "+\"test\":{\"name\": \"Hallo\"}", null, null);
        String test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"name\":\"Hallo\",\":childNodeCount\":0}", test);
        mk.dispose();
    }
    
    @Test
    public void commit() {
        MongoMK mk = createMK();
        
        String rev = mk.commit("/", "+\"test\":{\"name\": \"Hello\"}", null, null);
        String test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"name\":\"Hello\",\":childNodeCount\":0}", test);
        
        rev = mk.commit("/test", "+\"a\":{\"name\": \"World\"}", null, null);
        rev = mk.commit("/test", "+\"b\":{\"name\": \"!\"}", null, null);
        test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        Children c;
        c = mk.readChildren("/", "1",
                Revision.fromString(rev), Integer.MAX_VALUE);
        assertEquals("/: [/test]", c.toString());
        c = mk.readChildren("/test", "2",
                Revision.fromString(rev), Integer.MAX_VALUE);
        assertEquals("/test: [/test/a, /test/b]", c.toString());

        rev = mk.commit("", "^\"/test\":1", null, null);
        test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"test\":1,\"test\":{},\":childNodeCount\":1}", test);

        // System.out.println(test);
        mk.dispose();
    }
    
    @Test
    public void cache() {
        MongoMK mk = createMK();
        
        // BAD
        String rev = mk.commit("/", "+\"testRoot\":{} +\"index\":{}", null, null);
        
        // GOOD
//        String rev = mk.commit("/", "+\"testRoot\":{} ", null, null);
        
        String test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, ":id");
        // System.out.println("  " + test);
//        test = mk.getNodes("/testRoot", rev, 0, 0, Integer.MAX_VALUE, ":id");
//        System.out.println("  " + test);
        rev = mk.commit("/testRoot", "+\"a\":{}", null, null);
//        test = mk.getNodes("/testRoot", rev, 0, 0, Integer.MAX_VALUE, ":id");
//        System.out.println("  " + test);
//        rev = mk.commit("/testRoot/a", "+\"b\":{}", null, null);
//        rev = mk.commit("/testRoot/a/b", "+\"c\":{} +\"d\":{}", null, null);
//        test = mk.getNodes("/testRoot", rev, 0, 0, Integer.MAX_VALUE, ":id");
//        System.out.println("  " + test);
//        test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, ":id");
//        System.out.println("  " + test);
//        rev = mk.commit("/index", "+\"a\":{}", null, null);
        test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, ":id");
        // System.out.println("  " + test);
//        test = mk.getNodes("/testRoot", rev, 0, 0, Integer.MAX_VALUE, ":id");
//        System.out.println("  " + test);
        
//        assertEquals("{\"name\":\"Hello\",\":childNodeCount\":0}", test);
//        
//        rev = mk.commit("/test", "+\"a\":{\"name\": \"World\"}", null, null);
//        rev = mk.commit("/test", "+\"b\":{\"name\": \"!\"}", null, null);
//        test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
//        Children c;
//        c = mk.readChildren("/", "1",
//                Revision.fromString(rev), Integer.MAX_VALUE);
//        assertEquals("/: [/test]", c.toString());
//        c = mk.readChildren("/test", "2",
//                Revision.fromString(rev), Integer.MAX_VALUE);
//        assertEquals("/test: [/test/a, /test/b]", c.toString());
//
//        rev = mk.commit("", "^\"/test\":1", null, null);
//        test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, null);
//        assertEquals("{\"test\":1,\"test\":{},\":childNodeCount\":1}", test);

        // System.out.println(test);
        mk.dispose();
    }


    @Test
    public void testDeletion() {
        MongoMK mk = createMK();

        String rev = mk.commit("/", "+\"testDel\":{\"name\": \"Hello\"}", null,
                null);
        rev = mk.commit("/testDel", "+\"a\":{\"name\": \"World\"}", null, null);
        rev = mk.commit("/testDel", "+\"b\":{\"name\": \"!\"}", null, null);
        rev = mk.commit("/testDel", "+\"c\":{\"name\": \"!\"}", null, null);

        Children c = mk.readChildren("/testDel", "1", Revision.fromString(rev),
                Integer.MAX_VALUE);
        assertEquals(3, c.children.size());

        rev = mk.commit("/testDel", "-\"c\"", null, null);
        c = mk.readChildren("/testDel", "2", Revision.fromString(rev),
                Integer.MAX_VALUE);
        assertEquals(2, c.children.size());

        rev = mk.commit("/", "-\"testDel\"", null, null);
        Node n = mk.getNode("/testDel", Revision.fromString(rev));
        assertNull(n);
    }

    @Test
    public void testAddAndMove() {
        MongoMK mk = createMK();

        String head = mk.getHeadRevision();
        head = mk.commit("",
                "+\"/root\":{}\n" +
                        "+\"/root/a\":{}\n"+
                        "+\"/root/a/b\":{}\n",
                head, "");

        head = mk.commit("",
                ">\"/root/a\":\"/root/c\"\n",
                head, "");

        assertFalse(mk.nodeExists("/root/a", head));
        assertTrue(mk.nodeExists("/root/c/b", head));
    }

    private static MongoMK createMK() {
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            MongoUtils.dropCollections(db);
            return new MongoMK(db, 0);
        }
        return new MongoMK();
    }

}
