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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;

/**
 * Implementation specific constants related to permission evaluation.
 *
 * @since OAK 1.0
 */
public interface PermissionConstants {

    String NT_REP_PERMISSIONS = "rep:Permissions";
    String NT_REP_PERMISSION_STORE = "rep:PermissionStore";
    String REP_PERMISSION_STORE = "rep:permissionStore";
    String PERMISSIONS_STORE_PATH = '/' + JcrConstants.JCR_SYSTEM + '/' + REP_PERMISSION_STORE;

    String REP_ACCESS_CONTROLLED_PATH = "rep:accessControlledPath";
    String REP_PRIVILEGE_BITS = "rep:privileges";
    String REP_INDEX = "rep:index";
    char PREFIX_ALLOW = 'a';
    char PREFIX_DENY = 'd';

    Set<String> PERMISSION_NODETYPE_NAMES = ImmutableSet.of(NT_REP_PERMISSIONS, NT_REP_PERMISSION_STORE);
}