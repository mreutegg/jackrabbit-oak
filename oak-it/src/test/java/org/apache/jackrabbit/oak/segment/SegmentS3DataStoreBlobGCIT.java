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

package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.blob.cloud.S3DataStoreUtils;
import org.junit.After;
import org.junit.BeforeClass;

import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.SEGMENT_TAR;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.getFixtures;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for SegmentNodeStore on S3DataStore GC
 */
public class SegmentS3DataStoreBlobGCIT extends SegmentDataStoreBlobGCIT {

    @BeforeClass
    public static void assumptions() {
        assumeTrue(getFixtures().contains(SEGMENT_TAR));
        assumeTrue(S3DataStoreUtils.isS3DataStore());
    }


    @After
    public void close() throws Exception {
        super.close();
        S3DataStoreUtils.cleanup(blobStore.getDataStore(), startDate);
    }
}

