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
package org.apache.jackrabbit.oak.plugins.sqlpersistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.plugins.mongomk.Collection;
import org.apache.jackrabbit.oak.plugins.mongomk.Document;
import org.apache.jackrabbit.oak.plugins.mongomk.DocumentStore;
import org.apache.jackrabbit.oak.plugins.mongomk.Revision;
import org.apache.jackrabbit.oak.plugins.mongomk.StableRevisionComparator;
import org.apache.jackrabbit.oak.plugins.mongomk.UpdateOp;
import org.apache.jackrabbit.oak.plugins.mongomk.UpdateUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLDocumentStore implements DocumentStore {

    /**
     * Creates a {@linkplain SQLDocumentStore} instance using an embedded H2
     * database in in-memory mode.
     */
    public SQLDocumentStore() {
        try {
            String jdbcurl = "jdbc:h2:mem:oaknodes";
            initialize(jdbcurl, "sa", "");
        } catch (Exception ex) {
            throw new MicroKernelException("initializing SQL document store", ex);
        }
    }

    /**
     * Creates a {@linkplain SQLDocumentStore} instance using the provided JDBC
     * connection information.
     */
    public SQLDocumentStore(String jdbcurl, String username, String password) {
        try {
            initialize(jdbcurl, username, password);
        } catch (Exception ex) {
            throw new MicroKernelException("initializing SQL document store", ex);
        }
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String id) {
        return find(collection, id, 0);
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String id, int maxCacheAge) {
        // TODO handle maxCacheAge
        return readDocument(collection, id);
    }

    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
        return query(collection, fromKey, toKey, null, 0, limit);
    }

    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, String indexedProperty,
            long startValue, int limit) {
        return internalQuery(collection, fromKey, toKey, indexedProperty, startValue, limit);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String id) {
        delete(collection, id);
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        return internalCreate(collection, updateOps);
    }

    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        internalUpdate(collection, keys, updateOp);
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) throws MicroKernelException {
        return internalCreateOrUpdate(collection, update, true, false);
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) throws MicroKernelException {
        return internalCreateOrUpdate(collection, update, false, true);
    }

    @Override
    public void invalidateCache() {
        // TODO
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String id) {
        // TODO
    }

    @Override
    public void dispose() {
        try {
            this.connection.close();
            this.connection = null;
        } catch (SQLException ex) {
            throw new MicroKernelException(ex);
        }
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String id) {
        return null;
    }

    // implementation

    private final String MODIFIED = "_modified";
    private final String MODCOUNT = "_modCount";

    private static final Logger LOG = LoggerFactory.getLogger(SQLDocumentStore.class);

    private final Comparator<Revision> comparator = Collections.reverseOrder(new StableRevisionComparator());

    private Connection connection;

    private void initialize(String jdbcurl, String username, String password) throws Exception {
        connection = DriverManager.getConnection(jdbcurl, username, password);
        connection.setAutoCommit(false);
        Statement stmt = connection.createStatement();

        stmt.execute("drop table if exists CLUSTERNODES");
        stmt.execute("drop table if exists NODES");
        stmt.execute("create table if not exists CLUSTERNODES(ID varchar primary key, MODIFIED bigint, MODCOUNT bigint, DATA varchar)");
        stmt.execute("create table if not exists NODES(ID varchar primary key, MODIFIED bigint, MODCOUNT bigint, DATA varchar)");
    }

    @CheckForNull
    private <T extends Document> boolean internalCreate(Collection<T> collection, List<UpdateOp> updates) {
        try {
            for (UpdateOp update : updates) {
                T doc = collection.newDocument(this);
                update.increment(MODCOUNT, 1);
                UpdateUtils.applyChanges(doc, update, comparator);
                insertDocument(collection, doc);
            }
            // FIXME to be atomic
            return true;
        } catch (MicroKernelException ex) {
            return false;
        }
    }

    @CheckForNull
    private <T extends Document> T internalCreateOrUpdate(Collection<T> collection, UpdateOp update, boolean allowCreate,
            boolean checkConditions) {
        T oldDoc = readDocument(collection, update.getId());

        if (oldDoc == null) {
            if (!update.isNew() || !allowCreate) {
                throw new MicroKernelException("Document does not exist: " + update.getId());
            }
            T doc = collection.newDocument(this);
            if (checkConditions && !UpdateUtils.checkConditions(doc, update)) {
                return null;
            }
            update.increment(MODCOUNT, 1);
            UpdateUtils.applyChanges(doc, update, comparator);
            doc.seal();
            insertDocument(collection, doc);
        } else {
            T doc = applyChanges(collection, oldDoc, update, checkConditions);
            if (doc == null) {
                return null;
            }

            int retries = 5; // TODO
            boolean success = false;

            while (!success && retries > 0) {
                success = updateDocument(collection, doc, (Long) oldDoc.get(MODCOUNT));
                if (!success) {
                    // retry with a fresh document
                    retries -= 1;
                    oldDoc = readDocument(collection, update.getId());
                    doc = applyChanges(collection, oldDoc, update, checkConditions);
                    if (doc == null) {
                        return null;
                    }
                }
            }

            if (!success) {
                throw new MicroKernelException("failed update (race?)");
            }
        }

        return oldDoc;
    }

    @CheckForNull
    private <T extends Document> T applyChanges(Collection<T> collection, T oldDoc, UpdateOp update, boolean checkConditions) {
        T doc = collection.newDocument(this);
        oldDoc.deepCopy(doc);
        if (checkConditions && !UpdateUtils.checkConditions(doc, update)) {
            return null;
        }
        update.increment(MODCOUNT, 1);
        UpdateUtils.applyChanges(doc, update, comparator);
        doc.seal();
        return doc;
    }

    @CheckForNull
    private <T extends Document> void internalUpdate(Collection<T> collection, List<String> ids, UpdateOp update) {
        String tableName = getTable(collection);
        try {
            for (String id : ids) {
                String in = dbRead(connection, tableName, id);
                if (in == null) {
                    throw new MicroKernelException(tableName + " " + id + " not found");
                }
                T doc = fromString(collection, in);
                Long oldmodcount = (Long) doc.get(MODCOUNT);
                update.increment(MODCOUNT, 1);
                UpdateUtils.applyChanges(doc, update, comparator);
                String data = asString(doc);
                Long modified = (Long) doc.get(MODIFIED);
                Long modcount = (Long) doc.get(MODCOUNT);
                dbUpdate(connection, tableName, id, modified, modcount, oldmodcount, data);
            }
            connection.commit();
        } catch (Exception ex) {
            throw new MicroKernelException(ex);
        }
    }

    private <T extends Document> List<T> internalQuery(Collection<T> collection, String fromKey, String toKey,
            String indexedProperty, long startValue, int limit) {
        String tableName = getTable(collection);
        List<T> result = new ArrayList<T>();
        if (indexedProperty != null && !MODIFIED.equals(indexedProperty)) {
            throw new MicroKernelException("indexed property " + indexedProperty + " not supported");
        }
        try {
            List<String> dbresult = dbQuery(connection, tableName, fromKey, toKey, indexedProperty, startValue, limit);
            for (String data : dbresult) {
                T doc = fromString(collection, data);
                doc.seal();
                result.add(doc);
            }
        } catch (Exception ex) {
            throw new MicroKernelException(ex);
        }
        return result;
    }

    private static <T extends Document> String getTable(Collection<T> collection) {
        if (collection == Collection.CLUSTER_NODES) {
            return "CLUSTERNODES";
        } else if (collection == Collection.NODES) {
            return "NODES";
        } else {
            throw new IllegalArgumentException("Unknown collection: " + collection.toString());
        }
    }

    private static String asString(@Nonnull Document doc) {
        JSONObject obj = new JSONObject();
        for (String key : doc.keySet()) {
            Object value = doc.get(key);
            obj.put(key, value);
        }
        return obj.toJSONString();
    }

    private <T extends Document> T fromString(Collection<T> collection, String data) throws ParseException {
        T doc = collection.newDocument(this);
        Map<String, Object> obj = (Map<String, Object>) new JSONParser().parse(data);
        for (Map.Entry<String, Object> entry : obj.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value == null) {
                // ???
                doc.put(key, value);
            } else if (value instanceof Boolean || value instanceof Long || value instanceof String) {
                doc.put(key, value);
            } else if (value instanceof JSONObject) {
                doc.put(key, convertJsonObject((JSONObject) value));
            } else {
                throw new RuntimeException("unexpected type: " + value.getClass());
            }
        }
        return doc;
    }

    @Nonnull
    private Map<Revision, Object> convertJsonObject(@Nonnull JSONObject obj) {
        Map<Revision, Object> map = new TreeMap<Revision, Object>(comparator);
        Set<Map.Entry> entries = obj.entrySet();
        for (Map.Entry entry : entries) {
            // not clear why every persisted map is a revision map
            map.put(Revision.fromString(entry.getKey().toString()), entry.getValue());
        }
        return map;
    }

    @CheckForNull
    private <T extends Document> T readDocument(Collection<T> collection, String id) {
        String tableName = getTable(collection);
        try {
            String in = dbRead(connection, tableName, id);
            return in != null ? fromString(collection, in) : null;
        } catch (Exception ex) {
            throw new MicroKernelException(ex);
        }
    }

    private <T extends Document> void delete(Collection<T> collection, String id) {
        String tableName = getTable(collection);
        try {
            dbDelete(connection, tableName, id);
            connection.commit();
        } catch (Exception ex) {
            throw new MicroKernelException(ex);
        }
    }

    private <T extends Document> boolean updateDocument(@Nonnull Collection<T> collection, @Nonnull T document, Long oldmodcount) {
        String tableName = getTable(collection);
        try {
            String data = asString(document);
            Long modified = (Long) document.get(MODIFIED);
            Long modcount = (Long) document.get(MODCOUNT);
            boolean success = dbUpdate(connection, tableName, document.getId(), modified, modcount, oldmodcount, data);
            connection.commit();
            return success;
        } catch (SQLException ex) {
            throw new MicroKernelException(ex);
        }
    }

    private <T extends Document> void insertDocument(Collection<T> collection, T document) {
        String tableName = getTable(collection);
        try {
            String data = asString(document);
            Long modified = (Long) document.get(MODIFIED);
            Long modcount = (Long) document.get(MODCOUNT);
            dbInsert(connection, tableName, document.getId(), modified, modcount, data);
            connection.commit();
        } catch (SQLException ex) {
            throw new MicroKernelException(ex);
        }
    }

    // low level operations

    @CheckForNull
    private String dbRead(Connection connection, String tableName, String id) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("select DATA from " + tableName + " where ID = ?");
        try {
            stmt.setString(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            } else {
                return null;
            }
        } finally {
            stmt.close();
        }
    }

    private List<String> dbQuery(Connection connection, String tableName, String minId, String maxId, String indexedProperty,
            long startValue, int limit) throws SQLException {
        String t = "select DATA from " + tableName + " where ID > ? and ID < ?";
        if (indexedProperty != null) {
            t += " and MODIFIED >= ?";
        }
        t += " order by ID";
        if (limit != Integer.MAX_VALUE) {
            t += " limit ?";
        }
        PreparedStatement stmt = connection.prepareStatement(t);
        List<String> result = new ArrayList<String>();
        try {
            int si = 1;
            stmt.setString(si++, minId);
            stmt.setString(si++, maxId);
            if (indexedProperty != null) {
                stmt.setLong(si++, startValue);
            }
            if (limit != Integer.MAX_VALUE) {
                stmt.setInt(si++, limit);
            }
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String data = rs.getString(1);
                result.add(data);
            }
        } finally {
            stmt.close();
        }
        return result;
    }

    private boolean dbUpdate(Connection connection, String tableName, String id, Long modified, Long modcount, Long oldmodcount,
            String data) throws SQLException {
        String t = "update " + tableName + " set MODIFIED = ?, MODCOUNT = ?, DATA = ? where ID = ?";
        if (oldmodcount != null) {
            t += " and MODCOUNT = ?";
        }
        PreparedStatement stmt = connection.prepareStatement(t);
        try {
            int si = 1;
            stmt.setObject(si++, modified, Types.BIGINT);
            stmt.setObject(si++, modcount, Types.BIGINT);
            stmt.setString(si++, data);
            stmt.setString(si++, id);
            if (oldmodcount != null) {
                stmt.setObject(si++, oldmodcount, Types.BIGINT);
            }
            int result = stmt.executeUpdate();
            if (result != 1) {
                LOG.debug("DB update failed for " + tableName + "/" + id + " with oldmodcount=" + oldmodcount);
            }
            return result == 1;
        } finally {
            stmt.close();
        }
    }

    private boolean dbInsert(Connection connection, String tableName, String id, Long modified, Long modcount, String data)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("insert into " + tableName + " values(?, ?, ?, ?)");
        try {
            stmt.setString(1, id);
            stmt.setObject(2, modified, Types.BIGINT);
            stmt.setObject(3, modcount, Types.BIGINT);
            stmt.setString(4, data);
            int result = stmt.executeUpdate();
            if (result != 1) {
                LOG.debug("DB insert failed for " + tableName + "/" + id);
            }
            return result == 1;
        } finally {
            stmt.close();
        }
    }

    private boolean dbDelete(Connection connection, String tableName, String id) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("delete from " + tableName + " where ID = ?");
        try {
            stmt.setString(1, id);
            int result = stmt.executeUpdate();
            if (result != 1) {
                LOG.debug("DB delete failed for " + tableName + "/" + id);
            }
            return result == 1;
        } finally {
            stmt.close();
        }
    }
}
