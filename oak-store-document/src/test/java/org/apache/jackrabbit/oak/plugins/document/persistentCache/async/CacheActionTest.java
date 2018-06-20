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
package org.apache.jackrabbit.oak.plugins.document.persistentCache.async;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class CacheActionTest {

    @SuppressWarnings("unchecked")
    @Test
    public void invalidateCacheAction() {
        List<CacheValue> keys = new ArrayList<>();
        long size = 0;
        for (int i = 0; i < 10; i++) {
            CacheValue k = new StringValue("key-" + i);
            size += k.getMemory();
            keys.add(k);
        }
        CacheWriteQueue<?, ?> queue = mock(CacheWriteQueue.class);
        InvalidateCacheAction action = new InvalidateCacheAction(keys, queue);
        assertEquals(size, action.getMemory());

        // add a very big value
        keys.add(new TestValue(Integer.MAX_VALUE - 1));
        action = new InvalidateCacheAction(keys, queue);
        assertEquals(Integer.MAX_VALUE, action.getMemory());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void putToCacheAction() {
        CacheValue key = new StringValue("key");
        CacheValue value = new StringValue("value");
        long size = key.getMemory() + value.getMemory();

        CacheWriteQueue<?, ?> queue = mock(CacheWriteQueue.class);
        PutToCacheAction action = new PutToCacheAction(key, value, queue);
        assertEquals(size, action.getMemory());

        // use a very big value
        value = new TestValue(Integer.MAX_VALUE - 1);
        action = new PutToCacheAction(key, value, queue);
        assertEquals(Integer.MAX_VALUE, action.getMemory());
    }

    private static class TestValue implements CacheValue {

        private final int size;

        TestValue(int size) {
            this.size = size;
        }

        @Override
        public int getMemory() {
            return size;
        }
    }
}
