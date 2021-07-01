/*************************************************************************
 * ADOBE CONFIDENTIAL
 * ___________________
 *
 *  Copyright 2021 Adobe
 *  All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains
 * the property of Adobe and its suppliers, if any. The intellectual
 * and technical concepts contained herein are proprietary to Adobe
 * and its suppliers and are protected by all applicable intellectual
 * property laws, including trade secret and copyright laws.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe.
 **************************************************************************/
package org.apache.jackrabbit.oak.run;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Closer;
import com.mongodb.MongoClientURI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookupCommand
        implements org.apache.jackrabbit.oak.run.commons.Command {

    private static final Logger LOG = LoggerFactory.getLogger(LookupCommand.class);

    @Override
    public void execute(String... args) throws Exception {
        Closer closer = Utils.createCloserWithShutdownHook();
        try {
            String h = "lookup mongodb://host:port/database|jdbc:...";
            ConnectOptions options = new ConnectOptions(h).parse(args);
            if (options.isHelp()) {
                options.printHelpOn(System.out);
                System.exit(0);
            }

            MongoClientURI uri = new MongoClientURI(options.getStoreArg());
            for (;;) {
                lookupHosts(uri.getHosts());
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            }
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private void lookupHosts(List<String> hosts) {
        for (String h : hosts) {
            lookupHost(stripPort(h));
        }
    }

    private static String stripPort(String h) {
        if (h.contains(":")) {
            h = h.substring(0, h.indexOf(':'));
        }
        return h;
    }

    private void lookupHost(String h) {
        LOG.info("Looking up {}", h);
        try {
            LOG.info("{} resolves to {}", h, InetAddress.getByName(h).getHostAddress());
        } catch (UnknownHostException e) {
            LOG.info("{} lookup failed with {}", h, e.toString());
        }
    }

    private static final class ConnectOptions extends Utils.NodeStoreOptions {

        ConnectOptions(String usage) {
            super(usage);
        }

        @Override
        public ConnectOptions parse(String[] args) {
            super.parse(args);
            return this;
        }

        boolean isHelp() {
            return options.has(help);
        }
    }
}
