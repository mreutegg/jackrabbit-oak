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
package org.apache.jackrabbit.oak.spi.security.authentication;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;

/**
 * Interface for the authentication setup.
 */
public interface AuthenticationConfiguration extends SecurityConfiguration {

    String PARAM_AUTHENTICATION_OPTIONS = "org.apache.jackrabbit.oak.authentication.options";
    String PARAM_TOKEN_OPTIONS = "org.apache.jackrabbit.oak.authentication.token.options";

    String PARAM_APP_NAME = "org.apache.jackrabbit.oak.auth.appName";
    String DEFAULT_APP_NAME = "jackrabbit.oak";

    @Nonnull
    LoginContextProvider getLoginContextProvider(ContentRepository contentRepository);

    @Nonnull
    TokenProvider getTokenProvider(Root root);
}