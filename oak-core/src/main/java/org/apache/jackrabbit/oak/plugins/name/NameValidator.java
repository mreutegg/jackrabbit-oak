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
package org.apache.jackrabbit.oak.plugins.name;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Set;

import javax.jcr.PropertyType;

class NameValidator implements Validator {

    private final Set<String> prefixes;

    public NameValidator(Set<String> prefixes) {
        this.prefixes = prefixes;
    }

    protected void checkValidName(String name) throws CommitFailedException {
        int colon = name.indexOf(':');
        if (colon != -1) {
            String prefix = name.substring(0, colon);
            if (prefix.length() == 0 || !prefixes.contains(prefix)) {
                throw new CommitFailedException(
                        "Invalid namespace prefix: " + name);
            }
        }

        String local = name.substring(colon + 1);
        if (!Namespaces.isValidLocalName(local)) {
            throw new CommitFailedException("Invalid name: " + name);
        }
    }

    protected void checkValidValue(PropertyState property)
            throws CommitFailedException {
        if (property.isArray()) {
            for (CoreValue value : property.getValues()) {
                checkValidValue(value);
            }
        } else {
            checkValidValue(property.getValue());
        }
    }

    protected void checkValidValue(CoreValue value)
            throws CommitFailedException {
        if (value.getType() == PropertyType.NAME) {
            checkValidName(value.getString());
        }
    }

    //-------------------------------------------------------< NodeValidator >

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        checkValidName(after.getName());
        checkValidValue(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        checkValidValue(after);
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        // do nothing
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        checkValidName(name);
        return this;
    }

    @Override
    public Validator childNodeChanged(
            String name, NodeState before, NodeState after) {
        return this;
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        return null;
    }

}
