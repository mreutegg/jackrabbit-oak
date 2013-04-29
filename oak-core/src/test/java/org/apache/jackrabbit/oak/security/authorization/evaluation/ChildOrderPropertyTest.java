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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.TreeImpl;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for the hidden {@link TreeImpl#OAK_CHILD_ORDER} property
 *
 * TODO: review if this shouldn't be moved to o.a.jackrabbit.oak.core tests.
 */
public class ChildOrderPropertyTest extends AbstractOakCoreTest {

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        Tree a = root.getTreeOrNull("/a");
        a.setOrderableChildren(true);
        root.commit();
    }

    @Test
    public void testHasProperty() {
        Tree a = root.getTreeOrNull("/a");
        assertFalse(a.hasProperty(TreeImpl.OAK_CHILD_ORDER));
    }

    @Test
    public void testGetProperty() {
        Tree a = root.getTreeOrNull("/a");
        assertNull(a.getProperty(TreeImpl.OAK_CHILD_ORDER));
    }

    @Test
    public void testGetProperties() {
        Set<String> propertyNames = Sets.newHashSet(JcrConstants.JCR_PRIMARYTYPE, "aProp");

        Tree a = root.getTreeOrNull("/a");
        for (PropertyState prop : a.getProperties()) {
            assertTrue(propertyNames.remove(prop.getName()));
        }
        assertTrue(propertyNames.isEmpty());
    }

    @Test
    public void testGetPropertyCount() {
        Tree a = root.getTreeOrNull("/a");
        assertEquals(2, a.getPropertyCount());
    }

    @Test
    public void testGetPropertyStatus() {
        Tree a = root.getTreeOrNull("/a");
        assertNull(a.getPropertyStatus(TreeImpl.OAK_CHILD_ORDER));
    }

}