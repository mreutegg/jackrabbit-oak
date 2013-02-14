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
package org.apache.jackrabbit.oak.plugins.segment;

import static junit.framework.Assert.assertEquals;

import java.util.Calendar;
import java.util.Collections;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Test case for ensuring that segment size remains within bounds.
 */
public class SegmentSizeTest {

    private static final int BYTES_PER_REFERENCE = 4;

    @Test
    public void testNodeSize() {
        NodeBuilder builder = MemoryNodeState.EMPTY_NODE.builder();
        assertEquals(8, getSize(builder));
        assertEquals(4, getAmortizedSize(builder));

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.setProperty("foo", "bar");
        assertEquals(25, getSize(builder));
        assertEquals(8, getAmortizedSize(builder));

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.setProperty("foo", "bar");
        builder.setProperty("baz", 123);
        assertEquals(42, getSize(builder));
        assertEquals(12, getAmortizedSize(builder));

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.child("foo");
        assertEquals(28, getSize(builder));
        assertEquals(12, getAmortizedSize(builder));

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.child("foo");
        builder.child("bar");
        assertEquals(60, getSize(builder));
        assertEquals(44, getAmortizedSize(builder));
    }

    @Test
    public void testDuplicateStrings() {
        String string = "More than just a few bytes of example content.";

        NodeBuilder builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(1, string), Type.STRINGS));
        int base = getSize(builder);

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(10, string), Type.STRINGS));
        assertEquals(base + 10 * BYTES_PER_REFERENCE, getSize(builder));

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(100, string), Type.STRINGS));
        assertEquals(base + 100 * BYTES_PER_REFERENCE, getSize(builder));
    }

    @Test
    public void testDuplicateDates() {
        String now = ISO8601.format(Calendar.getInstance());

        NodeBuilder builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(1, now), Type.DATES));
        int base = getSize(builder);

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(10, now), Type.DATES));
        assertEquals(base + 10 * BYTES_PER_REFERENCE, getSize(builder));

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(100, now), Type.DATES));
        assertEquals(base + 100 * BYTES_PER_REFERENCE, getSize(builder));
    }

    @Test
    public void testAccessControlNodes() {
        NodeBuilder builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.setProperty("jcr:primaryType", "rep:ACL", Type.NAME);
        assertEquals(20, getSize(builder));
        assertEquals(4, getAmortizedSize(builder));

        NodeBuilder deny = builder.child("deny");
        deny.setProperty("jcr:primaryType", "rep:DenyACE", Type.NAME);
        deny.setProperty("rep:principalName", "everyone");
        deny.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:read"), Type.NAMES));
        assertEquals(134, getSize(builder));
        assertEquals(28, getAmortizedSize(builder));

        NodeBuilder allow = builder.child("allow");
        allow.setProperty("jcr:primaryType", "rep:GrantACE");
        allow.setProperty("rep:principalName", "administrators");
        allow.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:all"), Type.NAMES));
        assertEquals(259, getSize(builder));
        assertEquals(80, getAmortizedSize(builder));

        NodeBuilder deny0 = builder.child("deny0");
        deny0.setProperty("jcr:primaryType", "rep:DenyACE", Type.NAME);
        deny0.setProperty("rep:principalName", "everyone");
        deny0.setProperty("rep:glob", "*/activities/*");
        builder.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:read"), Type.NAMES));
        assertEquals(348, getSize(builder));
        assertEquals(116, getAmortizedSize(builder));

        NodeBuilder allow0 = builder.child("allow0");
        allow0.setProperty("jcr:primaryType", "rep:GrantACE");
        allow0.setProperty("rep:principalName", "user-administrators");
        allow0.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:all"), Type.NAMES));
        assertEquals(411, getSize(builder));
        assertEquals(152, getAmortizedSize(builder));
    }

    private int getSize(NodeBuilder builder) {
        SegmentStore store = new MemoryStore();
        SegmentWriter writer = new SegmentWriter(store);
        RecordId id = writer.writeNode(builder.getNodeState());
        writer.flush();
        Segment segment = store.readSegment(id.getSegmentId());
        return segment.getData().length;
    }

    private int getAmortizedSize(NodeBuilder builder) {
        SegmentStore store = new MemoryStore();
        SegmentWriter writer = new SegmentWriter(store);
        NodeState state = builder.getNodeState();
        writer.writeNode(state);
        writer.flush();
        RecordId id = writer.writeNode(state);
        writer.flush();
        Segment segment = store.readSegment(id.getSegmentId());
        return segment.getData().length;
    }

}
