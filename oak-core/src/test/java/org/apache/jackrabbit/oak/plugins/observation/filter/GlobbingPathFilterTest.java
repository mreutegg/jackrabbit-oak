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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathFilter.STAR;
import static org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathFilter.STAR_STAR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

public class GlobbingPathFilterTest {

    private ImmutableTree tree;

    @Before
    public void setup() {
        NodeBuilder root = EMPTY_NODE.builder();
        createPath(root, "a/b/c/d");
        createPath(root, "q");
        createPath(root, "x/y/x/y/z");
        createPath(root, "r/s/t/u/v/r/s/t/u/v/r/s/t/u/v/w");
        tree = new ImmutableTree(root.getNodeState());
    }

    private static void createPath(NodeBuilder root, String path) {
        NodeBuilder builder = root;
        for (String name : elements(path)) {
            builder = builder.setChildNode(name);
        }
    }

    /**
     * An empty path pattern should match no path
     */
    @Test
    public void emptyMatchesNothing() {
        Filter rootFilter = new GlobbingPathFilter(tree, tree, "");
        NodeState a = tree.getChild("a").getNodeState();
        assertFalse(rootFilter.includeAdd("a", a));
        assertNull(rootFilter.create("a", a, a));
    }

    /**
     * q should match q
     */
    @Test
    public void singleMatchesSingle() {
        Filter filter = new GlobbingPathFilter(tree, tree, "q");
        ImmutableTree t = tree;

        assertTrue(filter.includeAdd("q", t.getNodeState()));
    }

    /**
     * * should match q
     */
    @Test
    public void starMatchesSingle() {
        Filter filter = new GlobbingPathFilter(tree, tree, STAR);
        ImmutableTree t = tree;

        assertTrue(filter.includeAdd("q", t.getNodeState()));
    }

    /**
     * ** should match every path
     */
    @Test
    public void all() {
        Filter filter = new GlobbingPathFilter(tree, tree, STAR_STAR);
        ImmutableTree t = tree;

        for(String name : elements("a/b/c/d")) {
            assertTrue(filter.includeAdd(name, t.getNodeState()));
            t = t.getChild(name);
            filter = filter.create(name, t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }
    }

    /**
     * a/b/c should match a/b/c
     */
    @Test
    public void literal() {
        Filter rootFilter = new GlobbingPathFilter(tree, tree, "a/b/c");
        NodeState a = tree.getChild("a").getNodeState();
        assertFalse(rootFilter.includeAdd("a", a));

        Filter aFilter = rootFilter.create("a", a, a);
        assertNotNull(aFilter);
        NodeState b = a.getChildNode("b");
        assertFalse(aFilter.includeAdd("b", b));

        Filter bFilter = aFilter.create("b", b, b);
        assertNotNull(bFilter);
        NodeState c = b.getChildNode("c");
        assertTrue(bFilter.includeAdd("c", b));
        assertFalse(bFilter.includeAdd("x", b));

        assertNull(bFilter.create("c", c, c));
    }

    /**
     * a/*&#47c should match a/b/c
     */
    @Test
    public void starGlob() {
        Filter rootFilter = new GlobbingPathFilter(tree, tree, "a/*/c");
        NodeState a = tree.getChild("a").getNodeState();
        assertFalse(rootFilter.includeAdd("a", a));

        Filter aFilter = rootFilter.create("a", a, a);
        assertNotNull(aFilter);
        NodeState b = a.getChildNode("b");
        assertFalse(aFilter.includeAdd("b", b));

        Filter bFilter = aFilter.create("b", b, b);
        assertNotNull(bFilter);
        NodeState c = b.getChildNode("c");
        assertTrue(bFilter.includeAdd("c", b));
        assertFalse(bFilter.includeAdd("x", b));

        assertNull(bFilter.create("c", c, c));
    }

    /**
     * **&#47/y/z should match x/y/x/y/z
     */
    @Test
    public void starStarGlob() {
        Filter rootFilter = new GlobbingPathFilter(tree, tree, "**/y/z");
        NodeState x1 = tree.getChild("x").getNodeState();
        assertFalse(rootFilter.includeAdd("x", x1));

        Filter x1Filter = rootFilter.create("x", x1, x1);
        assertNotNull(x1Filter);
        NodeState y1 = x1.getChildNode("y");
        assertFalse(x1Filter.includeAdd("y", y1));

        Filter y1Filter = x1Filter.create("y", y1, y1);
        assertNotNull(y1Filter);
        NodeState x2 = y1.getChildNode("x");
        assertFalse(y1Filter.includeAdd("x", x2));

        Filter x2Filter = y1Filter.create("x", x2, x2);
        assertNotNull(x2Filter);
        NodeState y2 = x2.getChildNode("y");
        assertFalse(x2Filter.includeAdd("y", y2));

        Filter y2Filter = x2Filter.create("y", y2, y2);
        assertNotNull(y2Filter);
        NodeState z = y2.getChildNode("z");
        assertTrue(y2Filter.includeAdd("z", z));

        Filter zFilter = (y2Filter.create("z", z, z));
        assertFalse(zFilter.includeAdd("x", EMPTY_NODE));
    }

    /**
     * **&#47a/b/c should match a/b/c
     */
    @Test
    public void matchAtStart() {
        Filter rootFilter = new GlobbingPathFilter(tree, tree, "**/a/b/c");
        NodeState a = tree.getChild("a").getNodeState();
        assertFalse(rootFilter.includeAdd("a", a));

        Filter aFilter = rootFilter.create("a", a, a);
        assertNotNull(aFilter);
        NodeState b = a.getChildNode("b");
        assertFalse(aFilter.includeAdd("b", b));

        Filter bFilter = aFilter.create("b", b, b);
        assertNotNull(bFilter);
        NodeState c = b.getChildNode("c");
        assertTrue(bFilter.includeAdd("c", b));
        assertFalse(bFilter.includeAdd("x", b));
    }

    /**
     * **&#47r/s/t/u/v should match r/s/t/u/v and r/s/t/u/v/r/s/t/u/v and r/s/t/u/v/r/s/t/u/v/r/s/t/u/v
     */
    @Test
    public void multipleMatches() {
        Filter filter = new GlobbingPathFilter(tree, tree, "**/r/s/t/u/v");
        ImmutableTree t = tree;

        for(int c = 0; c < 2; c++) {
            for(String name : elements("r/s/t/u")) {
                assertFalse(filter.includeAdd(name, t.getNodeState()));
                t = t.getChild(name);
                filter = filter.create(name, t.getNodeState(), t.getNodeState());
                assertNotNull(filter);
            }

            assertTrue(filter.includeAdd("v", t.getNodeState()));
            t = t.getChild("v");
            filter = filter.create("v", t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }
    }

    /**
     * **&#47r/s/t/u/v/w should match r/s/t/u/v/r/s/t/u/v/r/s/t/u/v/w
     */
    @Test
    public void matchAtEnd() {
        Filter filter = new GlobbingPathFilter(tree, tree, "**/r/s/t/u/v/w");
        ImmutableTree t = tree;

        for(String name : elements("r/s/t/u/v/r/s/t/u/v/r/s/t/u/v")) {
            assertFalse(filter.includeAdd(name, t.getNodeState()));
            t = t.getChild(name);
            filter = filter.create(name, t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }

        assertTrue(filter.includeAdd("w", t.getNodeState()));
        t = t.getChild("w");
        filter = filter.create("w", t.getNodeState(), t.getNodeState());
        assertNotNull(filter);
    }

    /**
     * r/s/t&#47** should match r/s/t and all its descendants
     */
    @Test
    public void matchSuffix() {
        Filter filter = new GlobbingPathFilter(tree, tree, "r/s/t/**");
        ImmutableTree t = tree;

        for(String name : elements("r/s")) {
            assertFalse(filter.includeAdd(name, t.getNodeState()));
            t = t.getChild(name);
            filter = filter.create(name, t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }

        for (String name: elements("t/u/v/r/s/t/u/v/r/s/t/u/v/w")) {
            assertTrue(filter.includeAdd(name, t.getNodeState()));
            t = t.getChild(name);
            filter = filter.create(name, t.getNodeState(), t.getNodeState());
            assertNotNull(filter);
        }
    }

}
