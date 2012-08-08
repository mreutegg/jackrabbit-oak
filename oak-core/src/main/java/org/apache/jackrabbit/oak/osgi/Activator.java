/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.osgi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.plugins.type.DefaultTypeEditor;
import org.apache.jackrabbit.oak.spi.commit.CommitEditor;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.ValidatingEditor;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

public class Activator implements BundleActivator, ServiceTrackerCustomizer {

    private BundleContext context;

    private ServiceTracker tracker;

    private final OsgiIndexProvider indexProvider = new OsgiIndexProvider();

    private final OsgiValidatorProvider validatorProvider = new OsgiValidatorProvider();

    private final Map<ServiceReference, ServiceRegistration> services =
            new HashMap<ServiceReference, ServiceRegistration>();

    //----------------------------------------------------< BundleActivator >---

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        context = bundleContext;

        indexProvider.start(bundleContext);
        validatorProvider.start(bundleContext);

        tracker = new ServiceTracker(
                context, MicroKernel.class.getName(), this);
        tracker.open();
    }

    @Override
    public void stop(BundleContext bundleContext) throws Exception {
        tracker.close();

        indexProvider.stop();
        validatorProvider.stop();
    }

    //-------------------------------------------< ServiceTrackerCustomizer >---

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        if (service instanceof MicroKernel) {
            List<CommitEditor> editors = new ArrayList<CommitEditor>();
            editors.add(new DefaultTypeEditor());
            editors.add(new ValidatingEditor(validatorProvider));
            // editors.add(new LuceneEditor());

            MicroKernel kernel = (MicroKernel) service;
            services.put(reference, context.registerService(
                    ContentRepository.class.getName(),
                    new ContentRepositoryImpl(
                            kernel, indexProvider, new CompositeEditor(editors)),
                    new Properties()));
            return service;
        } else {
            context.ungetService(reference);
            return null;
        }
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
        // nothing to do
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        ServiceRegistration registration = services.remove(reference);
        registration.unregister();
        context.ungetService(reference);
    }

}
