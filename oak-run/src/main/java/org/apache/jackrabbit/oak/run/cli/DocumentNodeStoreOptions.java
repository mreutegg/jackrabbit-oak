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

package org.apache.jackrabbit.oak.run.cli;

import java.util.Collections;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class DocumentNodeStoreOptions implements OptionsBean {
    private final OptionSpec<Integer> clusterId;
    private final OptionSpec<Void> disableBranchesSpec;
    private final OptionSpec<Integer> cacheSizeSpec;
    private OptionSet options;

    public DocumentNodeStoreOptions(OptionParser parser){
        clusterId = parser.accepts("clusterId", "MongoMK clusterId")
                .withRequiredArg().ofType(Integer.class).defaultsTo(0);
        disableBranchesSpec = parser.
                accepts("disableBranches", "disable branches");
        cacheSizeSpec = parser.
                accepts("cacheSize", "cache size")
                .withRequiredArg().ofType(Integer.class).defaultsTo(0);
    }

    @Override
    public void configure(OptionSet options) {
        this.options = options;
    }

    @Override
    public String title() {
        return "DocumentNodeStore Options";
    }

    @Override
    public String description() {
        return "Options related to constructing DocumentNodeStore";
    }

    @Override
    public int order() {
        return 20;
    }

    @Override
    public Set<String> operationNames() {
        return Collections.emptySet();
    }

    public int getClusterId(){
        return clusterId.value(options);
    }

    public int getCacheSize() {
        return cacheSizeSpec.value(options);
    }

    public boolean disableBranchesSpec() {
        return options.has(disableBranchesSpec);
    }
}
