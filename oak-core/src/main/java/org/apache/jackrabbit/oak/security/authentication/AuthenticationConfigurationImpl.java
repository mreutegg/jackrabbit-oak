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
package org.apache.jackrabbit.oak.security.authentication;

import javax.annotation.Nonnull;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.security.authentication.token.TokenProviderImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuthenticationConfigurationImpl... TODO
 */
public class AuthenticationConfigurationImpl extends SecurityConfiguration.Default implements AuthenticationConfiguration {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationConfigurationImpl.class);

    private static final String DEFAULT_APP_NAME = "jackrabbit.oak";

    public static final String PARAM_TOKEN_OPTIONS = "org.apache.jackrabbit.oak.token.options";

    private final SecurityProvider securityProvider;
    private final ConfigurationParameters config;

    public AuthenticationConfigurationImpl(SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
        this.config = securityProvider.getConfiguration(PARAM_AUTHENTICATION_OPTIONS);
    }

    @Nonnull
    @Override
    public LoginContextProvider getLoginContextProvider(ContentRepository contentRepository) {
        String appName = config.getConfigValue(PARAM_APP_NAME, DEFAULT_APP_NAME);
        Configuration loginConfig = null;
        try {
            loginConfig = Configuration.getConfiguration();
            // FIXME: workaround for Java7 behavior. needs clean up (see OAK-497)
            if (loginConfig.getAppConfigurationEntry(appName) == null) {
                log.debug("No login configuration available for {}; using default", appName);
                loginConfig = null;
            }
        } catch (SecurityException e) {
            log.info("Failed to retrieve login configuration: using default. " + e);
        }
        if (loginConfig == null) {
            // TODO: define configuration structure
            // TODO: review if having a default is desirable or if login should fail without valid login configuration.
            loginConfig = ConfigurationUtil.getDefaultConfiguration(config);
        }
        return new LoginContextProviderImpl(appName, loginConfig, contentRepository, securityProvider);
    }

    @Nonnull
    @Override
    public TokenProvider getTokenProvider(Root root) {
        ConfigurationParameters tokenOptions = config.getConfigValue(PARAM_TOKEN_OPTIONS, new ConfigurationParameters());
        return new TokenProviderImpl(root, tokenOptions, securityProvider.getUserConfiguration());
    }
}