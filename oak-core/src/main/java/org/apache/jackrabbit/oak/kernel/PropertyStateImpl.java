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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.jcr.PropertyType;

public class PropertyStateImpl implements PropertyState {

    private final String name;
    private final CoreValue value;
    private final List<CoreValue> values;

    private PropertyStateImpl(String name, CoreValue value, List<CoreValue> values) {
        assert name != null;

        this.name = name;
        this.value = value;
        this.values = values;
    }

    public PropertyStateImpl(String name, CoreValue value) {
        this(name, value, null);
    }

    public PropertyStateImpl(String name, List<CoreValue> values) {
        this(name, null, Collections.unmodifiableList(values));
    }

    public PropertyStateImpl(String name, String value) {
        this(name, new CoreValueImpl(value, PropertyType.STRING));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isArray() {
        return value == null;
    }
    
    @Override
    public CoreValue getValue() {
        if (value == null) {
            throw new IllegalStateException("Not a single valued property");
        }
        return value;
    }

    @Override
    public Iterable<CoreValue> getValues() {
        if (values == null) {
            throw new IllegalStateException("Not a multi valued property");
        }
        return values;
    }

    //------------------------------------------------------------< Object >----
    /**
     * Checks whether the given object is equal to this one. Two property
     * states are considered equal if both their names and encoded values
     * match. Subclasses may override this method with a more efficient
     * equality check if one is available.
     *
     * @param that target of the comparison
     * @return {@code true} if the objects are equal, {@code false} otherwise
     */
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof PropertyState) {
            PropertyState other = (PropertyState) that;
            return getName().equals(other.getName()) && valueEquals(other);
        } else {
            return false;
        }
    }

    /**
     * Returns a hash code that's compatible with how the
     * {@link #equals(Object)} method is implemented. The current
     * implementation simply returns the hash code of the property name
     * since {@link PropertyState} instances are not intended for use as
     * hash keys.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public String toString() {
        return getName() + '=' + (isArray() ? getValues() : getValue());
    }

    //------------------------------------------------------------< private >---

    private boolean valueEquals(PropertyState other) {
        if (isArray() != other.isArray()) {
            return false;
        } else if (isArray()) {
            Iterator<CoreValue> iterator = other.getValues().iterator();
            for (CoreValue value : getValues()) {
                if (!iterator.hasNext() || !value.equals(iterator.next())) {
                    return false;
                }
            }
            return !iterator.hasNext();
        } else {
            return getValue().equals(other.getValue());
        }
    }
}
