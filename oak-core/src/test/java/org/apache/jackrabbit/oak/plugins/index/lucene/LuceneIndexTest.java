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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import javax.security.auth.Subject;

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.RootImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinitionImpl;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.security.authorization.AccessControlProviderImpl;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.junit.Test;

public class LuceneIndexTest implements LuceneIndexConstants {

    private static String DEFAULT_INDEX_NAME = "default-lucene";

    @Test
    public void testLucene() throws Exception {
        KernelNodeStore store = new KernelNodeStore(new MicroKernelImpl());

        IndexDefinition testID = new IndexDefinitionImpl(DEFAULT_INDEX_NAME,
                TYPE_LUCENE, "/" + INDEX_DEFINITIONS_NAME + "/"
                        + DEFAULT_INDEX_NAME);

        store.setHook(new LuceneEditor(testID.getPath()));
        Root root = new RootImpl(store, null, new Subject(),
                new AccessControlProviderImpl(),
                new CompositeQueryIndexProvider());

        Tree tree = root.getTree("/");
        tree.setProperty("foo", "bar");
        root.commit();

        QueryIndex index = new LuceneIndex(testID);
        FilterImpl filter = new FilterImpl(null);
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        Cursor cursor = index.query(filter, store.getRoot());
        assertTrue(cursor.next());
        assertEquals("/", cursor.currentRow().getPath());
        assertFalse(cursor.next());
    }

}
