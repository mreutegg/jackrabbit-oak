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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import com.google.common.base.Preconditions;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;

import javax.annotation.Nonnull;
import java.util.Iterator;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileBufferLinkedList.NodeIterator.iteratorFor;

/**
 * Linked list implementation which supports multiple iterators. The iterator's state
 * is backed by an actual node in the list. So, modification in the list show up in
 * iterator (assuming thread safely/volatility) getting handled outside of the class.
 */
public class FlatFileBufferLinkedList {

    private ListNode head = new ListNode();
    private ListNode tail = head;

    private int size = 0;

    /**
     * Add {@code item} at the tail of the list
     */
    public void add(@Nonnull NodeStateEntry item) {
        tail.next = new ListNode(item);
        tail = tail.next;
        size++;
    }

    /**
     * Remove the first item from the list
     * @return {@code NodeStateEntry} data in the removed item
     */
    public NodeStateEntry remove() {
        Preconditions.checkState(!isEmpty(), "Cannot remove item from empty list");
        NodeStateEntry ret = head.next.data;
        head.next.isValid = false;
        head.next = head.next.next;
        size--;
        if (size == 0) {
            tail = head;
        }
        return ret;
    }

    /**
     * @return {@link NodeIterator} object which would iterate the whole list
     */
    public NodeIterator iterator() {
        return iteratorFor(head);
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Represents an item in the list.
     */
    static class ListNode {
        private ListNode next;
        private final NodeStateEntry data;
        private boolean isValid = true;

        private ListNode() {
            this.data = null;
            this.next = null;
        }

        ListNode(@Nonnull NodeStateEntry data) {
            this.data = data;
            this.next = null;
        }
    }

    static class NodeIterator implements Iterator<NodeStateEntry> {
        private ListNode current;

        static NodeIterator iteratorFor(@Nonnull ListNode node) {
            return new NodeIterator(node);
        }

        NodeIterator(@Nonnull ListNode start) {
            this.current = start;
        }

        @Override
        public boolean hasNext() {
            return current.next != null;
        }

        @Override
        public NodeStateEntry next() {
            Preconditions.checkState(current.isValid, "Can't call next from a removed node");
            current = current.next;
            return current.data;
        }
    }
}
