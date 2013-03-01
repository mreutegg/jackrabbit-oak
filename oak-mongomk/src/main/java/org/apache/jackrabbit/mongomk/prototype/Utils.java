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

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.bson.types.ObjectId;

/**
 * Utility methods.
 */
public class Utils {
    
    static int pathDepth(String path) {
        return path.equals("/") ? 0 : path.replaceAll("[^/]", "").length();
    }
    
    static <K, V> Map<K, V> newMap() {
        return new TreeMap<K, V>();
    }

    @SuppressWarnings("unchecked")
    public static int getMapSize(Map<String, Object> map) {
        int size = 0;
        for (Entry<String, Object> e : map.entrySet()) {
            size += e.getKey().length();
            Object o = e.getValue();
            if (o instanceof String) {
                size += o.toString().length();
            } else if (o instanceof Long) {
                size += 8;
            } else if (o instanceof Map) {
                size += 8 + getMapSize((Map<String, Object>) o);
            }
        }
        return size;
    }

    /**
     * Generate a unique cluster id, similar to the machine id field in MongoDB ObjectId objects.
     * 
     * @return the unique machine id
     */
    public static int getUniqueClusterId() {
        ObjectId objId = new ObjectId();
        return objId._machine();
    }

    public static String escapePropertyName(String propertyName) {
        String key = propertyName;
        if (key.startsWith("$") || key.startsWith("_")) {
            key = "_" + key;
        }
        // '*' in a property name is illegal in JCR I believe
        // TODO find a better solution
        key = key.replace('.', '*');
        return key;
    }
    
    public static String unescapePropertyName(String key) {
        if (key.startsWith("__") || key.startsWith("_$")) {
            key = key.substring(1);
        }
        key = key.replace('*', '.');
        return key;
    }
    
    public static boolean isPropertyName(String key) {
        return !key.startsWith("_") || key.startsWith("__") || key.startsWith("_$");
    }
    
}
