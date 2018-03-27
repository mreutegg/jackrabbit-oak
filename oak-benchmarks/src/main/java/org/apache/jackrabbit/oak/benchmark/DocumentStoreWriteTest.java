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
package org.apache.jackrabbit.oak.benchmark;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.memory.GenericPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.util.ISO8601;

import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder;

public class DocumentStoreWriteTest extends DocumentStoreTest {

    protected DocumentNodeStore dummy;
    protected RevisionVector rootRevision = new RevisionVector(
            newRevision(1), newRevision(2));

    @Override
    public void run(Iterable<RepositoryFixture> fixtures,
                    List<Integer> concurrencyLevels) {
        dummy = newDocumentNodeStoreBuilder().setAsyncDelay(0).build();
        try {
            super.run(fixtures, concurrencyLevels);
        } finally {
            dummy.dispose();
        }
    }

    @Override
    Operation generateOperation() {
        List<UpdateOp> ops = Collections.singletonList(createUpdateOp());
        return ds -> store(ds, ops);
    }

    protected UpdateOp createUpdateOp() {
        String path = "/test" + AbstractTest.TEST_ID + "/path/" + AbstractTest.nextNodeName();
        Revision commitRev = newRevision(1);
        DocumentNodeState ns = newDocumentNodeState(dummy, path, rootRevision,
                newProperties(), false, null);
        return asOperation(ns, commitRev);
    }

    protected static Iterable<PropertyState> newProperties() {
        List<PropertyState> props = new ArrayList<>();
        props.add(PropertyStates.createProperty("jcr:primaryType", "nt:folder", Type.NAME));
        props.add(GenericPropertyState.dateProperty("jcr:created", ISO8601.format(Calendar.getInstance())));
        return props;
    }

}
