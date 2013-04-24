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
package org.apache.jackrabbit.oak.security;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

/**
 * ConfigurationParametersTest...
 */
public class ConfigurationParametersTest {

    @Before
    public void setup() {}

    @After
    public void tearDown() {}

    @Test
    public void testContains() {
        ConfigurationParameters params = ConfigurationParameters.EMPTY;
        assertFalse(params.contains("some"));
        assertFalse(params.contains(""));

        Map<String, String> map = new HashMap<String, String>();
        map.put("key1", "v");
        map.put("key2", "v");
        params = new ConfigurationParameters(map);
        assertTrue(params.contains("key1"));
        assertTrue(params.contains("key2"));
        assertFalse(params.contains("another"));
        assertFalse(params.contains(""));
    }

    @Test
    public void testGetConfigValue() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("o1", "v");
        ConfigurationParameters options = new ConfigurationParameters(map);

        assertEquals("v", options.getConfigValue("o1", null));
        assertEquals("v", options.getConfigValue("o1", "v2"));
    }

    @Test
    public void testDefaultValue() {
        TestObject testObject = new TestObject("t");
        Integer int1000 = new Integer(1000);

        ConfigurationParameters options = new ConfigurationParameters();

        assertNull(options.getConfigValue("some", null));
        assertEquals(testObject, options.getConfigValue("some", testObject));
        assertEquals(int1000, options.getConfigValue("some", int1000));
    }

    @Test
    public void testArrayDefaultValue() {
        TestObject[] testArray = new TestObject[] {new TestObject("t")};

        ConfigurationParameters options = new ConfigurationParameters();
        TestObject[] result = options.getConfigValue("test", new TestObject[0]);
        assertNotNull(result);
        assertEquals(0, result.length);

        result = options.getConfigValue("test", testArray);
        assertEquals(result, testArray);

        options = new ConfigurationParameters(Collections.singletonMap("test", testArray));
        result = options.getConfigValue("test", null);
        assertEquals(result, testArray);
    }

    @Test
    public void testConversion() {
        TestObject testObject = new TestObject("t");
        Integer int1000 = new Integer(1000);

        Map<String,Object> m = new HashMap<String, Object>();
        m.put("TEST", testObject);
        m.put("String", "1000");
        m.put("Int2", new Integer(1000));
        m.put("Int3", 1000);


        ConfigurationParameters options = new ConfigurationParameters(m);

        assertNotNull(options.getConfigValue("TEST", null));
        assertEquals(testObject, options.getConfigValue("TEST", null));
        assertEquals(testObject, options.getConfigValue("TEST", testObject));
        assertEquals("t", options.getConfigValue("TEST", "defaultString"));

        assertEquals("1000", options.getConfigValue("String", null));
        assertEquals(int1000, options.getConfigValue("String", new Integer(10)));
        assertEquals(new Long(1000), options.getConfigValue("String", new Long(10)));
        assertEquals("1000", options.getConfigValue("String", "10"));

        assertEquals(int1000, options.getConfigValue("Int2", null));
        assertEquals(int1000, options.getConfigValue("Int2", new Integer(10)));
        assertEquals("1000", options.getConfigValue("Int2", "1000"));

        assertEquals(1000, options.getConfigValue("Int3", null));
        assertEquals(int1000, options.getConfigValue("Int3", null));
        assertEquals(int1000, options.getConfigValue("Int3", new Integer(10)));
        assertEquals("1000", options.getConfigValue("Int3", "1000"));
    }

    @Test
    public void testNullValue() {
        ConfigurationParameters options = new ConfigurationParameters(Collections.singletonMap("test", null));

        assertNull(options.getConfigValue("test", null));
        assertNull(options.getConfigValue("test", "value"));
        assertNull(options.getConfigValue("test", new TestObject("t")));
        assertNull(options.getConfigValue("test", false));
    }

    private class TestObject {

        private final String name;

        private TestObject(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }

        public boolean equals(Object object) {
            if (object == this) {
                return true;
            }
            if (object instanceof TestObject) {
                return name.equals(((TestObject) object).name);
            }
            return false;
        }
    }
}