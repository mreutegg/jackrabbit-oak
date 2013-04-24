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
package org.apache.jackrabbit.oak.plugins.index.p2;

import java.util.List;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.plugins.index.IndexHookProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableList;

/**
 * Service that provides PropertyIndex based IndexHooks.
 * 
 * @see Property2IndexHook
 * @see IndexHookProvider
 * 
 */
@Component
@Service(IndexHookProvider.class)
public class Property2IndexHookProvider implements IndexHookProvider {

    public static final String TYPE = "p2";

    @Override
    public List<? extends IndexHook> getIndexHooks(
            String type, NodeBuilder builder, NodeState root) {
        if (TYPE.equals(type)) {
            return ImmutableList.of(new Property2IndexHook(builder, root));
        }
        return ImmutableList.of();
    }

}