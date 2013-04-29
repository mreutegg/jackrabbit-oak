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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.NamespaceRegistry;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;

/**
 * RestrictionProviderImpl... TODO
 */
public class RestrictionProviderImpl implements RestrictionProvider, AccessControlConstants {

    private final NamePathMapper namePathMapper;
    private Map<String, RestrictionDefinition> supported;

    public RestrictionProviderImpl(NamePathMapper namePathMapper) {
        this.namePathMapper = namePathMapper;

        RestrictionDefinition glob = new RestrictionDefinitionImpl(REP_GLOB, PropertyType.STRING, false, namePathMapper);
        this.supported = ImmutableMap.of(REP_GLOB, glob);
    }

    //------------------------------------------------< RestrictionProvider >---
    @Nonnull
    @Override
    public Set<RestrictionDefinition> getSupportedRestrictions(String oakPath) {
        if (isUnsupportedPath(oakPath)) {
            return Collections.emptySet();
        } else {
            return ImmutableSet.copyOf(supported.values());
        }
    }

    @Override
    public Restriction createRestriction(String oakPath, String jcrName, Value value) throws RepositoryException {
        if (isUnsupportedPath(oakPath)) {
            throw new AccessControlException("Unsupported restriction at " + oakPath);
        }

        String oakName = namePathMapper.getOakName(jcrName);
        RestrictionDefinition definition = supported.get(oakName);
        if (definition == null) {
            throw new AccessControlException("Unsupported restriction: " + oakName);
        }
        int requiredType = definition.getRequiredType();
        if (requiredType != PropertyType.UNDEFINED && requiredType != value.getType()) {
            throw new AccessControlException("Unsupported restriction: Expected value of type " + PropertyType.nameFromValue(definition.getRequiredType()));
        }
        PropertyState propertyState = PropertyStates.createProperty(oakName, value);
        return createRestriction(propertyState, definition);
    }

    @Override
    public Set<Restriction> readRestrictions(String oakPath, Tree aceTree) {
        if (isUnsupportedPath(oakPath)) {
            return Collections.emptySet();
        } else {
            Set<Restriction> restrictions = new HashSet<Restriction>();
            for (PropertyState propertyState : getRestrictionsTree(aceTree).getProperties()) {
                String propName = propertyState.getName();
                if (isRestrictionProperty(propName) && supported.containsKey(propName)) {
                    RestrictionDefinition def = supported.get(propName);
                    if (def.getRequiredType() == propertyState.getType().tag()) {
                        restrictions.add(createRestriction(propertyState, def));
                    }
                }
            }
            return restrictions;
        }
    }

    @Override
    public void writeRestrictions(String oakPath, Tree aceTree, Set<Restriction> restrictions) {
        // validation of the restrictions is delegated to the commit hook
        // see #validateRestrictions below
        if (!restrictions.isEmpty()) {
            NodeUtil aceNode = new NodeUtil(aceTree);
            NodeUtil rNode = aceNode.getOrAddChild(REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
            for (Restriction restriction : restrictions) {
                rNode.getTree().setProperty(restriction.getProperty());
            }
        }
    }

    @Override
    public void validateRestrictions(String oakPath, Tree aceTree) throws AccessControlException {
        Map<String, PropertyState> restrictionProperties = getRestrictionProperties(aceTree);
        if (isUnsupportedPath(oakPath) && !restrictionProperties.isEmpty()) {
            throw new AccessControlException("Restrictions not supported with 'null' path.");
        }
        for (Map.Entry<String, PropertyState> entry : restrictionProperties.entrySet()) {
            String restrName = entry.getKey();
            RestrictionDefinition def = supported.get(restrName);
            if (def == null) {
                throw new AccessControlException("Unsupported restriction: " + restrName);
            }
            int type = entry.getValue().getType().tag();
            if (type != def.getRequiredType()) {
                throw new AccessControlException("Invalid restriction type '" + PropertyType.nameFromValue(type) + "'. Expected " + PropertyType.nameFromValue(def.getRequiredType()));
            }
        }
        for (RestrictionDefinition def : supported.values()) {
            if (def.isMandatory() && !restrictionProperties.containsKey(def.getName())) {
                throw new AccessControlException("Mandatory restriction " + def.getName() + " is missing.");
            }
        }
    }

    @Override
    public RestrictionPattern getPattern(String oakPath, Tree tree) {
        if (oakPath != null) {
            PropertyState glob = tree.getProperty(REP_GLOB);
            if (glob != null) {
                return GlobPattern.create(oakPath, glob.getValue(Type.STRING));
            }
        }
        return RestrictionPattern.EMPTY;
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private Restriction createRestriction(PropertyState propertyState, RestrictionDefinition definition) {
        return new RestrictionImpl(propertyState, definition.isMandatory(), namePathMapper);
    }

    @Nonnull
    private Tree getRestrictionsTree(Tree aceTree) {
        Tree restrictions = aceTree.getChildOrNull(REP_RESTRICTIONS);
        if (restrictions == null) {
            // no rep:restrictions tree -> read from aceTree for backwards compatibility
            restrictions = aceTree;
        }
        return restrictions;
    }

    @Nonnull
    private Map<String, PropertyState> getRestrictionProperties(Tree aceTree) {
        Tree rTree = getRestrictionsTree(aceTree);
        Map<String, PropertyState> restrictionProperties = new HashMap<String, PropertyState>();
        for (PropertyState property : rTree.getProperties()) {
            String name = property.getName();
            if (isRestrictionProperty(name)) {
                restrictionProperties.put(name, property);
            }
        }
        return restrictionProperties;
    }

    private static boolean isRestrictionProperty(String propertyName) {
        return !AccessControlConstants.ACE_PROPERTY_NAMES.contains(propertyName) &&
                !NamespaceRegistry.PREFIX_JCR.equals(Text.getNamespacePrefix(propertyName));
    }

    private static boolean isUnsupportedPath(String oakPath) {
        return oakPath == null;
    }
}
