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
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Path.NULL;
import static org.apache.jackrabbit.oak.plugins.document.Path.ROOT;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PathTest {

    private final Path root = ROOT;
    private final Path foo = new Path(root, "foo");
    private final Path fooBar = new Path(foo, "bar");


    @Test
    public void pathToString() {
        assertEquals("/", root.toString());
        assertEquals("/foo", foo.toString());
        assertEquals("/foo/bar", fooBar.toString());
    }

    @Test
    public void toStringBuilder() {
        StringBuilder sb = new StringBuilder();
        root.toStringBuilder(sb);
        assertEquals(root.toString(), sb.toString());
        sb.setLength(0);
        foo.toStringBuilder(sb);
        assertEquals(foo.toString(), sb.toString());
        sb.setLength(0);
        fooBar.toStringBuilder(sb);
        assertEquals(fooBar.toString(), sb.toString());
    }

    @Test
    public void fromString() {
        assertEquals(NULL, Path.fromString(NULL.toString()));
        assertEquals(root, Path.fromString(root.toString()));
        assertEquals(foo, Path.fromString(foo.toString()));
        assertEquals(fooBar, Path.fromString(fooBar.toString()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromStringWithEmptyString() {
        Path.fromString("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromStringWithRelativePath() {
        Path.fromString("foo/bar");
    }

    @Test
    public void length() {
        assertEquals(NULL.toString().length(), NULL.length());
        assertEquals(root.toString().length(), root.length());
        assertEquals(foo.toString().length(), foo.length());
        assertEquals(fooBar.toString().length(), fooBar.length());
    }

    @Test
    public void isRoot() {
        assertFalse(NULL.isRoot());
        assertTrue(root.isRoot());
        assertFalse(foo.isRoot());
        assertFalse(fooBar.isRoot());
    }

    @Test
    public void getParent() {
        assertNull(NULL.getParent());
        assertNull(root.getParent());
        assertEquals(foo.getParent(), root);
        assertEquals(fooBar.getParent(), foo);
    }

    @Test
    public void getDepth() {
        assertEquals(PathUtils.getDepth(root.toString()), root.getDepth());
        assertEquals(root.getDepth(), NULL.getDepth());
        assertEquals(PathUtils.getDepth(foo.toString()), foo.getDepth());
        assertEquals(PathUtils.getDepth(fooBar.toString()), fooBar.getDepth());
    }

    @Test
    public void getAncestor() {
        assertEquals(NULL, NULL.getAncestor(0));
        assertEquals(NULL, NULL.getAncestor(1));
        assertEquals(root, root.getAncestor(-1));
        assertEquals(root, root.getAncestor(0));
        assertEquals(root, root.getAncestor(1));
        assertEquals(foo, foo.getAncestor(0));
        assertEquals(root, foo.getAncestor(1));
        assertEquals(root, foo.getAncestor(2));
        assertEquals(fooBar, fooBar.getAncestor(0));
        assertEquals(foo, fooBar.getAncestor(1));
        assertEquals(root, fooBar.getAncestor(2));
        assertEquals(root, fooBar.getAncestor(3));
    }

    @Test
    public void getName() {
        assertEquals("<null>", NULL.getName());
        assertEquals("", root.getName());
        assertEquals("foo", foo.getName());
        assertEquals("bar", fooBar.getName());
    }

    @Test
    public void elements() {
        assertThat(NULL.elements(), emptyIterable());
        assertThat(root.elements(), emptyIterable());
        assertThat(foo.elements(), contains("foo"));
        assertThat(fooBar.elements(), contains("foo", "bar"));
    }

    @Test
    public void isAncestorOf() {
        assertFalse(NULL.isAncestorOf(root));
        assertFalse(NULL.isAncestorOf(foo));
        assertFalse(NULL.isAncestorOf(NULL));
        assertFalse(root.isAncestorOf(NULL));
        assertTrue(root.isAncestorOf(foo));
        assertTrue(root.isAncestorOf(fooBar));
        assertTrue(foo.isAncestorOf(fooBar));
        assertFalse(root.isAncestorOf(root));
        assertFalse(foo.isAncestorOf(root));
        assertFalse(foo.isAncestorOf(foo));
        assertFalse(fooBar.isAncestorOf(fooBar));
        assertFalse(fooBar.isAncestorOf(foo));
        assertFalse(fooBar.isAncestorOf(root));
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullParent() {
        new Path(NULL, "foo");
    }
}
