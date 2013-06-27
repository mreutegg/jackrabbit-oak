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

package org.apache.jackrabbit.oak.cache;

import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;

public class CacheStats implements CacheStatsMBean{
    private final Cache<Object,Object> cache;
    private final Weigher weigher;
    private final long maxWeight;
    private final String name;

    public CacheStats(Cache cache, String name, Weigher weigher, long maxWeight) {
        this.cache = cache;
        this.name = name;
        this.weigher = weigher;
        this.maxWeight = maxWeight;
    }

    @Override
    public long getRequestCount() {
        return stats().requestCount();
    }

    @Override
    public long getHitCount() {
        return stats().hitCount();
    }

    @Override
    public double getHitRate() {
        return stats().hitRate();
    }

    @Override
    public long getMissCount() {
        return stats().missCount();
    }

    @Override
    public double getMissRate() {
        return stats().missRate();
    }

    @Override
    public long getLoadCount() {
        return stats().loadCount();
    }

    @Override
    public long getLoadSuccessCount() {
        return stats().loadSuccessCount();
    }

    @Override
    public long getLoadExceptionCount() {
        return stats().loadExceptionCount();
    }

    @Override
    public double getLoadExceptionRate() {
        return stats().loadExceptionRate();
    }

    @Override
    public long getTotalLoadTime() {
        return stats().totalLoadTime();
    }

    @Override
    public double getAverageLoadPenalty() {
        return stats().averageLoadPenalty();
    }

    @Override
    public long getEvictionCount() {
        return stats().evictionCount();
    }

    @Override
    public long getElementCount() {
        return cache.size();
    }

    @Override
    public long estimateCurrentWeight() {
        if(weigher == null){
            return -1;
        }
        long size = 0;
        for(Map.Entry e : cache.asMap().entrySet()){
            size += weigher.weigh(e.getKey(),e.getValue());
        }
        return size;
    }

    @Override
    public long getMaxTotalWeight() {
        return maxWeight;
    }

    @Override
    public String cacheInfoAsString() {
        return Objects.toStringHelper("CacheStats")
                .add("hitCount", getHitCount())
                .add("missCount", getMissCount())
                .add("loadSuccessCount", getLoadSuccessCount())
                .add("lLoadExceptionCount", getLoadExceptionCount())
                .add("totalLoadTime", getTotalLoadTime())
                .add("evictionCount", getEvictionCount())
                .add("elementCount", getElementCount())
                .add("totalWeight", humanReadableByteCount(estimateCurrentWeight(), true))
                .add("maxWeight", humanReadableByteCount(getMaxTotalWeight(),true))
                .toString();
    }

    public String getName() {
        return name;
    }

    private com.google.common.cache.CacheStats stats() {
        return cache.stats();
    }

    /**
     * Based on http://stackoverflow.com/a/3758880/1035417
     */
    private static String humanReadableByteCount(long bytes, boolean si) {
        if(bytes < 0){
            return "0";
        }
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}
