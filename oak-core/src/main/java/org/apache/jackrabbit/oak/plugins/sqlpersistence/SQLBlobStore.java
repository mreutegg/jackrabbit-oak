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
package org.apache.jackrabbit.oak.plugins.sqlpersistence;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.AbstractBlobStore;
import org.apache.jackrabbit.mk.util.StringUtils;

// mostly copied from DbBlobStore, removing the ConnectionPool dependency

public class SQLBlobStore extends AbstractBlobStore {

    /**
     * Creates a {@linkplain SQLBlobStore} instance using an embedded H2
     * database in in-memory mode.
     */
    public SQLBlobStore() {
        try {
            String jdbcurl = "jdbc:h2:mem:oaknodes";
            initialize(jdbcurl, "sa", "");
        } catch (Exception ex) {
            throw new MicroKernelException("initializing SQL blob store", ex);
        }
    }

    /**
     * Creates a {@linkplain SQLBlobStore} instance using the provided JDBC
     * connection information.
     */
    public SQLBlobStore(String jdbcurl, String username, String password) {
        try {
            initialize(jdbcurl, username, password);
        } catch (Exception ex) {
            throw new MicroKernelException("initializing SQL blob store", ex);
        }
    }

    private Connection connection;

    private void initialize(String jdbcurl, String username, String password) throws Exception {
        connection = DriverManager.getConnection(jdbcurl, username, password);
        connection.setAutoCommit(false);
        Statement stmt = connection.createStatement();

        stmt.execute("create table if not exists datastore_meta" + "(id varchar primary key, level int, lastMod bigint)");
        stmt.execute("create table if not exists datastore_data" + "(id varchar primary key, data binary)");
        stmt.close();
    }

    private long minLastModified;

    @Override
    protected void storeBlock(byte[] digest, int level, byte[] data) throws SQLException {
        try {
            String id = StringUtils.convertBytesToHex(digest);
            long now = System.currentTimeMillis();
            PreparedStatement prep = connection.prepareStatement("update datastore_meta set lastMod = ? where id = ?");
            int count;
            try {
                prep.setLong(1, now);
                prep.setString(2, id);
                count = prep.executeUpdate();
            } finally {
                prep.close();
            }
            if (count == 0) {
                try {
                    prep = connection.prepareStatement("insert into datastore_data(id, data) values(?, ?)");
                    try {
                        prep.setString(1, id);
                        prep.setBytes(2, data);
                        prep.execute();
                    } finally {
                        prep.close();
                    }
                } catch (SQLException e) {
                    // already exists - ok
                }
                try {
                    prep = connection.prepareStatement("insert into datastore_meta(id, level, lastMod) values(?, ?, ?)");
                    try {
                        prep.setString(1, id);
                        prep.setInt(2, level);
                        prep.setLong(3, now);
                        prep.execute();
                    } finally {
                        prep.close();
                    }
                } catch (SQLException e) {
                    // already exists - ok
                }
            }
        } finally {
            connection.commit();
        }
    }

    @Override
    protected byte[] readBlockFromBackend(BlockId blockId) throws Exception {
        try {
            PreparedStatement prep = connection.prepareStatement("select data from datastore_data where id = ?");
            try {
                String id = StringUtils.convertBytesToHex(blockId.getDigest());
                prep.setString(1, id);
                ResultSet rs = prep.executeQuery();
                if (!rs.next()) {
                    throw new IOException("Datastore block " + id + " not found");
                }
                byte[] data = rs.getBytes(1);
                // System.out.println("    read block " + id + " blockLen: " +
                // data.length + " [0]: " + data[0]);
                if (blockId.getPos() == 0) {
                    return data;
                }
                int len = (int) (data.length - blockId.getPos());
                if (len < 0) {
                    return new byte[0];
                }
                byte[] d2 = new byte[len];
                System.arraycopy(data, (int) blockId.getPos(), d2, 0, len);
                return d2;
            } finally {
                prep.close();
            }
        } finally {
            connection.commit();
        }
    }

    @Override
    public void startMark() throws Exception {
        minLastModified = System.currentTimeMillis();
        markInUse();
    }

    @Override
    protected boolean isMarkEnabled() {
        return minLastModified != 0;
    }

    @Override
    protected void mark(BlockId blockId) throws Exception {
        try {
            if (minLastModified == 0) {
                return;
            }
            String id = StringUtils.convertBytesToHex(blockId.getDigest());
            PreparedStatement prep = connection
                    .prepareStatement("update datastore_meta set lastMod = ? where id = ? and lastMod < ?");
            prep.setLong(1, System.currentTimeMillis());
            prep.setString(2, id);
            prep.setLong(3, minLastModified);
            prep.executeUpdate();
            prep.close();
        } finally {
            connection.commit();
        }
    }

    @Override
    public int sweep() throws Exception {
        try {
            int count = 0;
            PreparedStatement prep = connection.prepareStatement("select id from datastore_meta where lastMod < ?");
            prep.setLong(1, minLastModified);
            ResultSet rs = prep.executeQuery();
            ArrayList<String> ids = new ArrayList<String>();
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
            prep = connection.prepareStatement("delete from datastore_meta where id = ?");
            PreparedStatement prepData = connection.prepareStatement("delete from datastore_data where id = ?");
            for (String id : ids) {
                prep.setString(1, id);
                prep.execute();
                prepData.setString(1, id);
                prepData.execute();
                count++;
            }
            prepData.close();
            prep.close();
            minLastModified = 0;
            return count;
        } finally {
            connection.commit();
        }
    }
}
