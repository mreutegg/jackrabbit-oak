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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import java.util.Collections;
import java.util.Map;

import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test compatibility with Jackrabbit 2.x using the
 * {@link AccessControlConstants#PARAM_PERMISSIONS_JR2} configuration parameter.
 */
public class Jr2CompatibilityTest extends AbstractOakCoreTest {

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        setupPermission("/", getTestUser().getPrincipal(), true, PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            AccessControlManager acMgr = getAccessControlManager(root);
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
            if (acl != null) {
                boolean modified = false;
                for (AccessControlEntry entry : acl.getAccessControlEntries()) {
                    if (entry.getPrincipal().equals(getTestUser().getPrincipal())) {
                        acl.removeAccessControlEntry(entry);
                        modified = true;
                    }
                }
                if (modified) {
                    acMgr.setPolicy("/", acl);
                    root.commit();
                }
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        Map<String, Boolean> map = Collections.singletonMap(AccessControlConstants.PARAM_PERMISSIONS_JR2, Boolean.TRUE);
        ConfigurationParameters acConfig = new ConfigurationParameters(map);

        return new ConfigurationParameters(ImmutableMap.of(AccessControlConfiguration.PARAM_ACCESS_CONTROL_OPTIONS, acConfig));
    }

    @Test
    public void testUserManagementPermissionWithJr2Flag() throws Exception {
        Root testRoot = getTestRoot();
        testRoot.refresh();

        UserManager testUserMgr = getUserConfiguration().getUserManager(testRoot, NamePathMapper.DEFAULT);
        try {
            User u = testUserMgr.createUser("a", "b");
            testRoot.commit();

            u.changePassword("c");
            testRoot.commit();

            u.remove();
            testRoot.commit();
        } finally {
            root.refresh();
            Authorizable user = getUserManager().getAuthorizable("a");
            if (user != null) {
                user.remove();
                root.commit();
            }
        }
    }
}