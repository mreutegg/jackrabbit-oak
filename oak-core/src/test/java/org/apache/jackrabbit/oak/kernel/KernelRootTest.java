/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PropertyType;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KernelRootTest extends AbstractOakTest {

    private KernelNodeStore store;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        store = new KernelNodeStore(microKernel, valueFactory);
    }

    @Override
    KernelNodeState createInitialState() {
        String jsop =
                "+\"test\":{\"a\":1,\"b\":2,\"c\":3,"
                        + "\"x\":{},\"y\":{},\"z\":{}}";
        String revision = microKernel.commit("/", jsop, microKernel.getHeadRevision(), "test data");
        return new KernelNodeState(microKernel, valueFactory, "/test", revision);
    }

    @Test
    public void getChild() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        Tree child = tree.getChild("any");
        assertNull(child);

        child = tree.getChild("x");
        assertNotNull(child);
    }

    @Test
    public void getProperty() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        PropertyState propertyState = tree.getProperty("any");
        assertNull(propertyState);

        propertyState = tree.getProperty("a");
        assertNotNull(propertyState);
        assertFalse(propertyState.isArray());
        assertEquals(PropertyType.LONG, propertyState.getValue().getType());
        assertEquals(1, propertyState.getValue().getLong());
    }

    @Test
    public void getChildren() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        Iterable<Tree> children = tree.getChildren();

        Set<String> expectedPaths = new HashSet<String>();
        Collections.addAll(expectedPaths, "x", "y", "z");

        for (Tree child : children) {
            assertTrue(expectedPaths.remove(child.getPath()));
        }
        assertTrue(expectedPaths.isEmpty());

        assertEquals(3, tree.getChildrenCount());
    }

    @Test
    public void getProperties() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        Map<String, CoreValue> expectedProperties = new HashMap<String, CoreValue>();
        expectedProperties.put("a", valueFactory.createValue(1));
        expectedProperties.put("b", valueFactory.createValue(2));
        expectedProperties.put("c", valueFactory.createValue(3));

        Iterable<PropertyState> properties = tree.getProperties();
        for (PropertyState property : properties) {
            CoreValue value = expectedProperties.remove(property.getName());
            assertNotNull(value);
            assertFalse(property.isArray());
            assertEquals(value, property.getValue());
        }

        assertTrue(expectedProperties.isEmpty());

        assertEquals(3, tree.getPropertyCount());
    }

    @Test
    public void addChild() throws CommitFailedException {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        assertFalse(tree.hasChild("new"));
        Tree added = tree.addChild("new");
        assertNotNull(added);
        assertEquals("new", added.getName());
        assertTrue(tree.hasChild("new"));

        root.commit();
        tree = root.getTree("/");

        assertTrue(tree.hasChild("new"));
    }

    @Test
    public void addExistingChild() throws CommitFailedException {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        assertFalse(tree.hasChild("new"));
        tree.addChild("new");

        root.commit();
        tree = root.getTree("/");

        assertTrue(tree.hasChild("new"));
        Tree added = tree.addChild("new");
        assertNotNull(added);
        assertEquals("new", added.getName());
    }

    @Test
    public void removeChild() throws CommitFailedException {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        tree.removeChild("x");
        assertFalse(tree.hasChild("x"));

        root.commit();
        tree = root.getTree("/");
        
        assertFalse(tree.hasChild("x"));
    }

    @Test
    public void setProperty() throws CommitFailedException {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        assertFalse(tree.hasProperty("new"));
        CoreValue value = valueFactory.createValue("value");
        tree.setProperty("new", value);
        PropertyState property = tree.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals(value, property.getValue());

        root.commit();
        tree = root.getTree("/");
        
        property = tree.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals(value, property.getValue());
    }

    @Test
    public void removeProperty() throws CommitFailedException {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        assertTrue(tree.hasProperty("a"));
        tree.removeProperty("a");
        assertFalse(tree.hasProperty("a"));

        root.commit();
        tree = root.getTree("/");

        assertFalse(tree.hasProperty("a"));
    }

    @Test
    public void move() throws CommitFailedException {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        Tree y = tree.getChild("y");

        assertTrue(tree.hasChild("x"));
        root.move("x", "y/xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));
        
        root.commit();
        tree = root.getTree("/");

        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChild("y").hasChild("xx"));
    }

    @Test
    public void rename() throws CommitFailedException {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        root.move("x", "xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));
        
        root.commit();
        tree = root.getTree("/");

        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));
    }

    @Test
    public void copy() throws CommitFailedException {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        Tree y = tree.getChild("y");

        assertTrue(tree.hasChild("x"));
        root.copy("x", "y/xx");
        assertTrue(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));
        
        root.commit();
        tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChild("y").hasChild("xx"));
    }

    @Test
    public void deepCopy() throws CommitFailedException {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        Tree y = tree.getChild("y");

        root.getTree("x").addChild("x1");
        root.copy("x", "y/xx");
        assertTrue(y.hasChild("xx"));
        assertTrue(y.getChild("xx").hasChild("x1"));

        root.commit();
        tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChild("y").hasChild("xx"));
        assertTrue(tree.getChild("y").getChild("xx").hasChild("x1"));

        Tree x = tree.getChild("x");
        Tree xx = tree.getChild("y").getChild("xx");
        checkEqual(x, xx);
    }

    @Test
    public void getChildrenCount() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        assertEquals(3, tree.getChildrenCount());

        tree.removeChild("x");
        assertEquals(2, tree.getChildrenCount());

        tree.addChild("a");
        assertEquals(3, tree.getChildrenCount());

        tree.addChild("x");
        assertEquals(4, tree.getChildrenCount());
    }

    @Test
    public void getPropertyCount() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        assertEquals(3, tree.getPropertyCount());

        CoreValue value = valueFactory.createValue("foo");
        tree.setProperty("a", value);
        assertEquals(3, tree.getPropertyCount());

        tree.removeProperty("a");
        assertEquals(2, tree.getPropertyCount());

        tree.setProperty("x", value);
        assertEquals(3, tree.getPropertyCount());

        tree.setProperty("a", value);
        assertEquals(4, tree.getPropertyCount());
    }

    @Test
    public void largeChildList() throws CommitFailedException {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        Set<String> added = new HashSet<String>();

        tree.addChild("large");
        tree = tree.getChild("large");
        for (int c = 0; c < 10000; c++) {
            String name = "n" + c;
            added.add(name);
            tree.addChild(name);
        }

        root.commit();
        tree = root.getTree("/");
        tree = tree.getChild("large");

        for (Tree q : tree.getChildren()) {
            assertTrue(added.remove(q.getName()));
        }

        assertTrue(added.isEmpty());
    }

    private static void checkEqual(Tree tree1, Tree tree2) {
        assertEquals(tree1.getChildrenCount(), tree2.getChildrenCount());
        assertEquals(tree1.getPropertyCount(), tree2.getPropertyCount());

        for (PropertyState property1 : tree1.getProperties()) {
            assertEquals(property1, tree2.getProperty(property1.getName()));
        }

        for (Tree child1 : tree1.getChildren()) {
            checkEqual(child1, tree2.getChild(child1.getName()));
        }
    }


}
