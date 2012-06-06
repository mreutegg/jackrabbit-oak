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
package org.apache.jackrabbit.oak.security.privilege;

/**
 * PrivilegeConstants... TODO
 */
interface PrivilegeConstants {
    
    String JCR_READ = "jcr:read";
    String JCR_MODIFY_PROPERTIES = "jcr:modifyProperties";
    String JCR_ADD_CHILD_NODES = "jcr:addChildNodes";
    String JCR_REMOVE_NODE = "jcr:removeNode";
    String JCR_REMOVE_CHILD_NODES = "jcr:removeChildNodes";
    String JCR_WRITE = "jcr:write";
    String JCR_READ_ACCESS_CONTROL = "jcr:readAccessControl";
    String JCR_MODIFY_ACCESS_CONTROL = "jcr:modifyAccessControl";
    String JCR_LOCK_MANAGEMENT = "jcr:lockManagement";
    String JCR_VERSION_MANAGEMENT = "jcr:versionManagement";
    String JCR_NODE_TYPE_MANAGEMENT = "jcr:nodeTypeManagement";
    String JCR_RETENTION_MANAGEMENT = "jcr:retentionManagement";
    String JCR_LIFECYCLE_MANAGEMENT = "jcr:lifecycleManagement";
    String JCR_WORKSPACE_MANAGEMENT = "jcr:workspaceManagement";
    String JCR_NODE_TYPE_DEFINITION_MANAGEMENT = "jcr:nodeTypeDefinitionManagement";
    String JCR_NAMESPACE_MANAGEMENT = "jcr:namespaceManagement";
    String JCR_ALL = "jcr:all";

    String REP_PRIVILEGE_MANAGEMENT = "rep:privilegeManagement";
    String REP_WRITE = "rep:write";
    String REP_ADD_PROPERTIES = "rep:addProperties";
    String REP_ALTER_PROPERTIES = "rep:alterProperties";
    String REP_REMOVE_PROPERTIES = "rep:removeProperties";
}