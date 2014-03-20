/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.osgi.ObserverTracker;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.BlobGC;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.cache.CachingDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The OSGi service to start/stop a DocumentNodeStore instance.
 */
@Component(policy = ConfigurationPolicy.REQUIRE)
public class DocumentNodeStoreService {
    private static final String DEFAULT_URI = "mongodb://localhost:27017/oak";
    private static final int DEFAULT_CACHE = 256;
    private static final int DEFAULT_OFF_HEAP_CACHE = 0;
    private static final String DEFAULT_DB = "oak";
    private static final String PREFIX = "oak.documentstore.";

    /**
     * Name of framework property to configure Mongo Connection URI
     */
    private static final String FWK_PROP_URI = "oak.mongo.uri";

    /**
     * Name of framework property to configure Mongo Database name
     * to use
     */
    private static final String FWK_PROP_DB = "oak.mongo.db";

    //DocumentMK would be done away with so better not
    //to expose this setting in config ui
    @Property(boolValue = false, propertyPrivate = true)
    private static final String PROP_USE_MK = "useMK";

    @Property(value = DEFAULT_URI)
    private static final String PROP_URI = "mongouri";

    @Property(value = DEFAULT_DB)
    private static final String PROP_DB = "db";

    @Property(intValue = DEFAULT_CACHE)
    private static final String PROP_CACHE = "cache";

    @Property(intValue = DEFAULT_OFF_HEAP_CACHE)
    private static final String PROP_OFF_HEAP_CACHE = "offHeapCache";

    /**
     * Boolean value indicating a blobStore is to be used
     */
    public static final String CUSTOM_BLOB_STORE = "customBlobStore";

    private static final long MB = 1024 * 1024;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private ServiceRegistration reg;
    private final List<Registration> registrations = new ArrayList<Registration>();
    private WhiteboardExecutor executor;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC)
    private volatile BlobStore blobStore;

    private DocumentMK mk;
    private ObserverTracker observerTracker;
    private ComponentContext context;

    @Activate
    protected void activate(ComponentContext context, Map<String, ?> config) throws Exception {
        this.context = context;

        if(blobStore == null &&
                PropertiesUtil.toBoolean(prop(CUSTOM_BLOB_STORE), false)){
            log.info("BlobStore use enabled. DocumentNodeStoreService would be initialized when " +
                    "BlobStore would be available");
        }else{
            registerNodeStore();
        }
    }

    protected void registerNodeStore() throws IOException {
        if(context == null){
            log.info("Component still not activated. Ignoring the initialization call");
            return;
        }
        String uri = PropertiesUtil.toString(prop(PROP_URI, FWK_PROP_URI), DEFAULT_URI);
        String db = PropertiesUtil.toString(prop(PROP_DB, FWK_PROP_DB), DEFAULT_DB);

        int offHeapCache = PropertiesUtil.toInteger(prop(PROP_OFF_HEAP_CACHE), DEFAULT_OFF_HEAP_CACHE);
        int cacheSize = PropertiesUtil.toInteger(prop(PROP_CACHE), DEFAULT_CACHE);
        boolean useMK = PropertiesUtil.toBoolean(context.getProperties().get(PROP_USE_MK), false);


        MongoClientOptions.Builder builder = MongoConnection.getDefaultBuilder();
        MongoClientURI mongoURI = new MongoClientURI(uri, builder);

        if (log.isInfoEnabled()) {
            // Take care around not logging the uri directly as it
            // might contain passwords
            String type = useMK ? "MK" : "NodeStore";
            log.info("Starting Document{} with host={}, db={}, cache size (MB)={}, Off Heap Cache size (MB)={}",
                    type, mongoURI.getHosts(), db, cacheSize, offHeapCache);
            log.info("Mongo Connection details {}", MongoConnection.toString(mongoURI.getOptions()));
        }

        MongoClient client = new MongoClient(mongoURI);
        DB mongoDB = client.getDB(db);

        DocumentMK.Builder mkBuilder =
                new DocumentMK.Builder().
                memoryCacheSize(cacheSize * MB).
                offHeapCacheSize(offHeapCache * MB);

        //Set blobstore before setting the DB
        if (blobStore != null) {
            mkBuilder.setBlobStore(blobStore);
        }

        mkBuilder.setMongoDB(mongoDB);

        mk = mkBuilder.open();

        log.info("Connected to database {}", mongoDB);

        registerJMXBeans(mk.getNodeStore(), context.getBundleContext());

        NodeStore store;
        if (useMK) {
            KernelNodeStore kns = new KernelNodeStore(mk);
            store = kns;
            observerTracker = new ObserverTracker(kns);
        } else {
            DocumentNodeStore mns = mk.getNodeStore();
            store = mns;
            observerTracker = new ObserverTracker(mns);
        }

        observerTracker.start(context.getBundleContext());

        Dictionary<String, String> props = new Hashtable<String, String>();
        props.put(Constants.SERVICE_PID, DocumentNodeStore.class.getName());
        reg = context.getBundleContext().registerService(NodeStore.class.getName(), store, props);
    }

    private Object prop(String propName) {
        return prop(propName, PREFIX + propName);
    }

    private Object prop(String propName, String fwkPropName) {
        //Prefer framework property first
        Object value = context.getBundleContext().getProperty(fwkPropName);
        if (value != null) {
            return value;
        }

        //Fallback to one from config
        return context.getProperties().get(propName);
    }

    @Deactivate
    protected void deactivate() {
        if (observerTracker != null) {
            observerTracker.stop();
        }

        unregisterNodeStore();
    }

    protected void bindBlobStore(BlobStore blobStore) throws IOException {
        log.info("Initializing DocumentNodeStore with BlobStore [{}]", blobStore);
        this.blobStore = blobStore;
        registerNodeStore();
    }

    protected void unbindBlobStore(BlobStore blobStore){
        this.blobStore = null;
        unregisterNodeStore();
    }

    private void unregisterNodeStore() {
        for (Registration r : registrations) {
            r.unregister();
        }

        if (reg != null) {
            reg.unregister();
        }

        if (mk != null) {
            mk.dispose();
        }

        if (executor != null) {
            executor.stop();
            executor = null;
        }
    }

    private void registerJMXBeans(DocumentNodeStore store, BundleContext context) throws IOException {
        Whiteboard wb = new OsgiWhiteboard(context);
        registrations.add(
                registerMBean(wb,
                        CacheStatsMBean.class,
                        store.getNodeCacheStats(),
                        CacheStatsMBean.TYPE,
                        store.getNodeCacheStats().getName()));
        registrations.add(
                registerMBean(wb,
                        CacheStatsMBean.class,
                        store.getNodeChildrenCacheStats(),
                        CacheStatsMBean.TYPE,
                        store.getNodeChildrenCacheStats().getName())
        );
        registrations.add(
                registerMBean(wb,
                        CacheStatsMBean.class,
                        store.getDocChildrenCacheStats(),
                        CacheStatsMBean.TYPE,
                        store.getDocChildrenCacheStats().getName())
        );
        DiffCache cl = store.getDiffCache();
        if (cl instanceof MemoryDiffCache) {
            MemoryDiffCache mcl = (MemoryDiffCache) cl;
            registrations.add(
                    registerMBean(wb,
                            CacheStatsMBean.class,
                            mcl.getDiffCacheStats(),
                            CacheStatsMBean.TYPE,
                            mcl.getDiffCacheStats().getName()));
            
        }

        DocumentStore ds = store.getDocumentStore();
        if (ds instanceof CachingDocumentStore) {
            CachingDocumentStore cds = (CachingDocumentStore) ds;
            registrations.add(
                    registerMBean(wb,
                            CacheStatsMBean.class,
                            cds.getCacheStats(),
                            CacheStatsMBean.TYPE,
                            cds.getCacheStats().getName())
            );
        }
        if (blobStore instanceof GarbageCollectableBlobStore) {
            executor = new WhiteboardExecutor();
            executor.start(wb);
            MarkSweepGarbageCollector gc = new MarkSweepGarbageCollector();
            gc.init(store);  // FIXME OAK-1582 ClassCastException in MarkSweepGarbageCollector#init()
            registrations.add(registerMBean(wb, BlobGCMBean.class, new BlobGC(gc, executor),
                    BlobGCMBean.TYPE, "Segment node store blob garbage collection"));
        }

        //TODO Register JMX bean for Off Heap Cache stats
    }
}
