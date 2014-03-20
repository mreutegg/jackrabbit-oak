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

package org.apache.jackrabbit.oak.plugins.blob;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link BlobGCMBean} based on a {@link BlobGarbageCollector}.
 */
public class BlobGC implements BlobGCMBean {
    private static final Logger log = LoggerFactory.getLogger(BlobGC.class);

    private final BlobGarbageCollector blobGarbageCollector;
    private final Executor executor;

    private RunnableFuture<Long> gcOp;

    /**
     * @param blobGarbageCollector  Blob garbage collector
     * @param executor              executor for running the garbage collection task
     */
    public BlobGC(
            @Nonnull BlobGarbageCollector blobGarbageCollector,
            @Nonnull Executor executor) {
        this.blobGarbageCollector = checkNotNull(blobGarbageCollector);
        this.executor = checkNotNull(executor);
    }

    @Nonnull
    @Override
    public String startBlobGC() {
        if (gcOp != null && !gcOp.isDone()) {
            return "Garbage collection already running";
        } else {
            gcOp = new FutureTask<Long>(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long t0 = System.nanoTime();
                    blobGarbageCollector.collectGarbage();
                    return System.nanoTime() - t0;
                }
            });
            executor.execute(gcOp);
            return getBlobGCStatus();
        }
    }

    @Nonnull
    @Override
    public String getBlobGCStatus() {
        if (gcOp == null) {
            return "Garbage collection not started";
        } else if (gcOp.isCancelled()) {
            return "Garbage collection cancelled";
        } else if (gcOp.isDone()) {
            try {
                return "Garbage collection completed in " + formatTime(gcOp.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return "Garbage Collection status unknown: " + e.getMessage();
            } catch (ExecutionException e) {
                log.error("Garbage collection failed", e.getCause());
                return "Garbage collection failed: " + e.getCause().getMessage();
            }
        } else {
            return "Garbage collection running";
        }
    }

    private static String formatTime(long nanos) {
        return TimeUnit.MINUTES.convert(nanos, TimeUnit.NANOSECONDS) + " minutes";
    }

}
