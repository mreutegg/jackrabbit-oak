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
package org.apache.jackrabbit.oak.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.jcr.Repository.IDENTIFIER_STABILITY;
import static javax.jcr.Repository.LEVEL_1_SUPPORTED;
import static javax.jcr.Repository.LEVEL_2_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_AUTOCREATED_DEFINITIONS_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_INHERITANCE;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_INHERITANCE_SINGLE;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_MULTIPLE_BINARY_PROPERTIES_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_MULTIVALUED_PROPERTIES_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_ORDERABLE_CHILD_NODES_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_OVERRIDES_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_PRIMARY_ITEM_NAME_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_PROPERTY_TYPES;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_RESIDUAL_DEFINITIONS_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_SAME_NAME_SIBLINGS_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_UPDATE_IN_USE_SUPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_VALUE_CONSTRAINTS_SUPPORTED;
import static javax.jcr.Repository.OPTION_ACCESS_CONTROL_SUPPORTED;
import static javax.jcr.Repository.OPTION_ACTIVITIES_SUPPORTED;
import static javax.jcr.Repository.OPTION_BASELINES_SUPPORTED;
import static javax.jcr.Repository.OPTION_JOURNALED_OBSERVATION_SUPPORTED;
import static javax.jcr.Repository.OPTION_LIFECYCLE_SUPPORTED;
import static javax.jcr.Repository.OPTION_LOCKING_SUPPORTED;
import static javax.jcr.Repository.OPTION_NODE_AND_PROPERTY_WITH_SAME_NAME_SUPPORTED;
import static javax.jcr.Repository.OPTION_NODE_TYPE_MANAGEMENT_SUPPORTED;
import static javax.jcr.Repository.OPTION_OBSERVATION_SUPPORTED;
import static javax.jcr.Repository.OPTION_QUERY_SQL_SUPPORTED;
import static javax.jcr.Repository.OPTION_RETENTION_SUPPORTED;
import static javax.jcr.Repository.OPTION_SHAREABLE_NODES_SUPPORTED;
import static javax.jcr.Repository.OPTION_SIMPLE_VERSIONING_SUPPORTED;
import static javax.jcr.Repository.OPTION_TRANSACTIONS_SUPPORTED;
import static javax.jcr.Repository.OPTION_UNFILED_CONTENT_SUPPORTED;
import static javax.jcr.Repository.OPTION_UPDATE_MIXIN_NODE_TYPES_SUPPORTED;
import static javax.jcr.Repository.OPTION_UPDATE_PRIMARY_NODE_TYPE_SUPPORTED;
import static javax.jcr.Repository.OPTION_VERSIONING_SUPPORTED;
import static javax.jcr.Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED;
import static javax.jcr.Repository.OPTION_XML_EXPORT_SUPPORTED;
import static javax.jcr.Repository.OPTION_XML_IMPORT_SUPPORTED;
import static javax.jcr.Repository.QUERY_FULL_TEXT_SEARCH_SUPPORTED;
import static javax.jcr.Repository.QUERY_JOINS;
import static javax.jcr.Repository.QUERY_JOINS_NONE;
import static javax.jcr.Repository.QUERY_LANGUAGES;
import static javax.jcr.Repository.QUERY_STORED_QUERIES_SUPPORTED;
import static javax.jcr.Repository.QUERY_XPATH_DOC_ORDER;
import static javax.jcr.Repository.QUERY_XPATH_POS_INDEX;
import static javax.jcr.Repository.REP_NAME_DESC;
import static javax.jcr.Repository.REP_VENDOR_DESC;
import static javax.jcr.Repository.REP_VENDOR_URL_DESC;
import static javax.jcr.Repository.REP_VERSION_DESC;
import static javax.jcr.Repository.SPEC_NAME_DESC;
import static javax.jcr.Repository.SPEC_VERSION_DESC;
import static javax.jcr.Repository.WRITE_SUPPORTED;

/**
 * {@code MicroKernel}-based implementation of
 * the {@link ContentRepository} interface.
 */
public class ContentRepositoryImpl implements ContentRepository {

    private final NodeStore nodeStore;
    private final CommitHook commitHook;
    private final String defaultWorkspaceName;
    private final SecurityProvider securityProvider;
    private final QueryIndexProvider indexProvider;

    private DescriptorsImpl descriptors;
    
    /**
     * Creates an content repository instance based on the given, already
     * initialized components.
     *
     * @param nodeStore            the node store this repository is based upon.
     * @param commitHook           the hook to use for processing commits
     * @param defaultWorkspaceName the default workspace name;
     * @param indexProvider        index provider
     * @param securityProvider     The configured security provider.
     */
    public ContentRepositoryImpl(@Nonnull NodeStore nodeStore,
                                 @Nonnull CommitHook commitHook,
                                 @Nonnull String defaultWorkspaceName,
                                 @Nullable QueryIndexProvider indexProvider,
                                 @Nonnull SecurityProvider securityProvider) {
        this.nodeStore = checkNotNull(nodeStore);
        this.commitHook = checkNotNull(commitHook);
        this.defaultWorkspaceName = checkNotNull(defaultWorkspaceName);
        this.securityProvider = checkNotNull(securityProvider);
        this.indexProvider = indexProvider != null ? indexProvider : new CompositeQueryIndexProvider();
    }

    @Nonnull
    @Override
    public ContentSession login(Credentials credentials, String workspaceName)
            throws LoginException, NoSuchWorkspaceException {
        if (workspaceName == null) {
            workspaceName = defaultWorkspaceName;
        }

        // TODO: support multiple workspaces. See OAK-118
        if (!defaultWorkspaceName.equals(workspaceName)) {
            throw new NoSuchWorkspaceException(workspaceName);
        }

        LoginContextProvider lcProvider = securityProvider.getConfiguration(AuthenticationConfiguration.class).getLoginContextProvider(this);
        LoginContext loginContext = lcProvider.getLoginContext(credentials, workspaceName);
        loginContext.login();

        return new ContentSessionImpl(loginContext, securityProvider, workspaceName, nodeStore,
                commitHook, indexProvider);
    }

    public NodeStore getNodeStore() {
        return nodeStore;
    }

    @Nonnull
    @Override
    public Descriptors getDescriptors() {
        if (descriptors == null) {
            descriptors = createDescriptors();
        }
        return descriptors;
    }
    
    @SuppressWarnings("deprecation")
    @Nonnull
    protected DescriptorsImpl createDescriptors() {
        final ValueFactory valueFactory = new SimpleValueFactory();
        final Value trueValue = valueFactory.createValue(true);
        final Value falseValue = valueFactory.createValue(false);
        DescriptorsImpl d = new DescriptorsImpl();
        d.put(new DescriptorsImpl.Descriptor(
                IDENTIFIER_STABILITY,
                valueFactory.createValue(Repository.IDENTIFIER_STABILITY_METHOD_DURATION), true, true));
        d.put(new DescriptorsImpl.Descriptor(
                LEVEL_1_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                LEVEL_2_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_NODE_TYPE_MANAGEMENT_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_AUTOCREATED_DEFINITIONS_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_INHERITANCE,
                valueFactory.createValue(NODE_TYPE_MANAGEMENT_INHERITANCE_SINGLE), true, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_MULTIPLE_BINARY_PROPERTIES_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_MULTIVALUED_PROPERTIES_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_ORDERABLE_CHILD_NODES_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_OVERRIDES_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_PRIMARY_ITEM_NAME_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_PROPERTY_TYPES,
                new Value[] {
                        valueFactory.createValue(PropertyType.TYPENAME_STRING),
                        valueFactory.createValue(PropertyType.TYPENAME_BINARY),
                        valueFactory.createValue(PropertyType.TYPENAME_LONG),
                        valueFactory.createValue(PropertyType.TYPENAME_LONG),
                        valueFactory.createValue(PropertyType.TYPENAME_DOUBLE),
                        valueFactory.createValue(PropertyType.TYPENAME_DECIMAL),
                        valueFactory.createValue(PropertyType.TYPENAME_DATE),
                        valueFactory.createValue(PropertyType.TYPENAME_BOOLEAN),
                        valueFactory.createValue(PropertyType.TYPENAME_NAME),
                        valueFactory.createValue(PropertyType.TYPENAME_PATH),
                        valueFactory.createValue(PropertyType.TYPENAME_REFERENCE),
                        valueFactory.createValue(PropertyType.TYPENAME_WEAKREFERENCE),
                        valueFactory.createValue(PropertyType.TYPENAME_URI),
                        valueFactory.createValue(PropertyType.TYPENAME_UNDEFINED)
                }, false, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_RESIDUAL_DEFINITIONS_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_SAME_NAME_SIBLINGS_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_VALUE_CONSTRAINTS_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                NODE_TYPE_MANAGEMENT_UPDATE_IN_USE_SUPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_ACCESS_CONTROL_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_JOURNALED_OBSERVATION_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_LIFECYCLE_SUPPORTED,
                falseValue, true, true));
        // locking support added via JCR layer
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_LOCKING_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_OBSERVATION_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_NODE_AND_PROPERTY_WITH_SAME_NAME_SUPPORTED,
                supportsSameNameNodeAndProperties() ? trueValue : falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_QUERY_SQL_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_RETENTION_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_SHAREABLE_NODES_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_SIMPLE_VERSIONING_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_TRANSACTIONS_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_UNFILED_CONTENT_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_UPDATE_MIXIN_NODE_TYPES_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_UPDATE_PRIMARY_NODE_TYPE_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_VERSIONING_SUPPORTED,
                trueValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_WORKSPACE_MANAGEMENT_SUPPORTED,
                trueValue, true, true));
        // xml export support added via JCR layer
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_XML_EXPORT_SUPPORTED,
                falseValue, true, true));
        // xml import support added via JCR layer
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_XML_IMPORT_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_ACTIVITIES_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                OPTION_BASELINES_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                QUERY_FULL_TEXT_SEARCH_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                QUERY_JOINS,
                valueFactory.createValue(QUERY_JOINS_NONE), true, true));
        d.put(new DescriptorsImpl.Descriptor(
                QUERY_LANGUAGES,
                new Value[0], false, true));
        d.put(new DescriptorsImpl.Descriptor(
                QUERY_STORED_QUERIES_SUPPORTED,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                QUERY_XPATH_DOC_ORDER,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                QUERY_XPATH_POS_INDEX,
                falseValue, true, true));
        d.put(new DescriptorsImpl.Descriptor(
                REP_NAME_DESC,
                valueFactory.createValue("Apache Jackrabbit Oak"), true, true));
        d.put(new DescriptorsImpl.Descriptor(
                REP_VERSION_DESC,
                valueFactory.createValue(getVersion()), true, true));
        d.put(new DescriptorsImpl.Descriptor(
                REP_VENDOR_DESC,
                valueFactory.createValue("The Apache Software Foundation"), true, true));
        d.put(new DescriptorsImpl.Descriptor(
                REP_VENDOR_URL_DESC,
                valueFactory.createValue("http://www.apache.org/"), true, true));
        d.put(new DescriptorsImpl.Descriptor(
                SPEC_NAME_DESC,
                valueFactory.createValue("Content Repository for Java Technology API"), true, true));
        d.put(new DescriptorsImpl.Descriptor(
                SPEC_VERSION_DESC,
                valueFactory.createValue("2.0"), true, true));
        d.put(new DescriptorsImpl.Descriptor(
                WRITE_SUPPORTED,
                trueValue, true, true));

        return d;
    }

    /**
     * Checks if this repository supports same name node and properties. currently this is tied to the underlying
     * node store implementation class.
     *
     * @return {@code true} if this repository supports SNNP.
     */
    private boolean supportsSameNameNodeAndProperties() {
        return !(nodeStore instanceof KernelNodeStore);
    }

    /**
     * Returns the version of this repository implementation.
     * @return the version
     */
    @Nonnull
    private static String getVersion() {
        InputStream stream = ContentRepositoryImpl.class.getResourceAsStream(
                "/META-INF/maven/org.apache.jackrabbit/oak-core/pom.properties");
        if (stream != null) {
            try {
                try {
                    Properties properties = new Properties();
                    properties.load(stream);
                    return properties.getProperty("version");
                } finally {
                    stream.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
        return "SNAPSHOT";
    }
}
