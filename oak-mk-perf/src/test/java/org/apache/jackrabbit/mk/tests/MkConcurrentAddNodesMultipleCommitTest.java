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
package org.apache.jackrabbit.mk.tests;

import org.apache.jackrabbit.mk.scenarios.ConcurrentAddNodesMultipleCommits;
import org.apache.jackrabbit.mk.testing.ConcurrentMicroKernelTestBase;

import org.junit.Test;

/**
 * Test class for microkernel concurrent writing.The microkernel is adding 1000
 * nodes per commit.
 */

public class MkConcurrentAddNodesMultipleCommitTest extends
        ConcurrentMicroKernelTestBase {

    // nodes for each worker
    int nodesNumber = 1000;
    int numberOfNodesPerCommit = 10;

    /**
     * @Rule public CatchAllExceptionsRule catchAllExceptionsRule = new
     *       CatchAllExceptionsRule();
     */
    @Test
    public void testConcurentWritingFlatStructure() throws InterruptedException {
        ConcurrentAddNodesMultipleCommits.concurentWritingFlatStructure(mks,
                mkNumber, nodesNumber, numberOfNodesPerCommit, chronometer);
    }

    @Test
    public void testConcurentWritingPyramid1() throws InterruptedException {
        ConcurrentAddNodesMultipleCommits.concurentWritingPyramid1(mks,
                mkNumber, nodesNumber, numberOfNodesPerCommit, chronometer);

    }

    @Test
    public void testConcurentWritingPyramid2() throws InterruptedException {
        ConcurrentAddNodesMultipleCommits.concurentWritingPyramid2(mks,
                mkNumber, nodesNumber, numberOfNodesPerCommit, chronometer);
    }

}
