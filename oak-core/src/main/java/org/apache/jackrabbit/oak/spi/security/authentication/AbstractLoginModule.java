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

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.jcr.Credentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractLoginModule... TODO
 */
public abstract class AbstractLoginModule implements LoginModule {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(AbstractLoginModule.class);

    /**
     * Key of the sharedState entry referring to validated Credentials that is
     * shared between multiple login modules.
     */
    public static final String SHARED_KEY_CREDENTIALS = "org.apache.jackrabbit.credentials";

    protected Subject subject;
    protected CallbackHandler callbackHandler;
    protected Map sharedState;

    //--------------------------------------------------------< LoginModule >---
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.sharedState = sharedState;
    }

    @Override
    public boolean logout() throws LoginException {
        if (subject.getPrincipals().isEmpty() || subject.getPublicCredentials(Credentials.class).isEmpty()) {
            return false;
        } else {
            // clear subject if not readonly
            if (!subject.isReadOnly()) {
                subject.getPrincipals().clear();
                subject.getPublicCredentials().clear();
            }
            return true;
        }
    }

    //--------------------------------------------------------------------------
    protected abstract Set<Class> getSupportedCredentials();

    protected Credentials getCredentials() {
        if (callbackHandler != null) {
            log.debug("Login: retrieving Credentials using callback.");
            try {
                CredentialsCallback callback = new CredentialsCallback();
                callbackHandler.handle(new Callback[]{callback});
                Credentials creds = callback.getCredentials();
                if (creds != null) {
                    log.debug("Login: Credentials '{}' obtained from callback", creds);
                    return creds;
                } else {
                    log.debug("Login: No credentials obtained from callback; trying shared state.");
                }
            } catch (UnsupportedCallbackException e) {
                log.warn(e.getMessage());
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }

        Credentials creds = getSharedCredentials();
        if (creds == null) {
            log.debug("Login: No credentials found in shared state; looking for supported credentials in subject.");
            for (Class clz : getSupportedCredentials()) {
                Set<Credentials> cds = subject.getPublicCredentials(clz);
                if (!cds.isEmpty()) {
                    log.debug("Login: Credentials found in subject.");
                    return cds.iterator().next();
                }
            }
        }

        log.debug("No credentials found.");
        return null;
    }

    protected Credentials getSharedCredentials() {
        Credentials shared = null;
        if (sharedState.containsKey(SHARED_KEY_CREDENTIALS)) {
            Object sc = sharedState.get(SHARED_KEY_CREDENTIALS);
            if (sc instanceof Credentials) {
                shared = (Credentials) sc;
            } else {
                log.debug("Login: Invalid value for share state entry " + SHARED_KEY_CREDENTIALS + ". Credentials expected.");
            }
        }

        return shared;
    }
}
