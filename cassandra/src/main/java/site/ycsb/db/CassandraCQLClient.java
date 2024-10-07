/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 *
 * <p>Submitted by Chrisjan Matser on 10/11/2010.
 */
package site.ycsb.db;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Cassandra 4.x CQL client.
 *
 * <p>See {@code cassandra/README.md} for details.
 *
 * @author cmatser
 * @author smatiolids
 */
public class CassandraCQLClient extends DB {

  private static Logger logger = LoggerFactory.getLogger(CassandraCQLClient.class);

  private static CqlSessionBuilder builder = CqlSession.builder();
  private static CqlSession session = null;

  private static ConcurrentMap<Set<String>, PreparedStatement> readStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> scanStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> insertStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> updateStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static AtomicReference<PreparedStatement> readAllStmt =
      new AtomicReference<PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> scanAllStmt =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  // private static AtomicReference<PreparedStatement> scanAllStmt =
  //     new AtomicReference<PreparedStatement>();
  private static AtomicReference<PreparedStatement> deleteStmt =
      new AtomicReference<PreparedStatement>();

  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.QUORUM;
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.QUORUM;

  public static final String YCSB_KEY = "y_id";
  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String HOSTS_PROPERTY = "hosts";
  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";
  public static final String SECURE_CONNECT_BUNDLE_PROPERTY = "cassandra.scb_path";

  public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = readConsistencyLevel.name();
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT =
      writeConsistencyLevel.name();

  public static final String MAX_CONNECTIONS_PROPERTY = "cassandra.maxconnections";
  public static final String CORE_CONNECTIONS_PROPERTY = "cassandra.coreconnections";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY = "cassandra.connecttimeoutmillis";
  public static final String READ_TIMEOUT_MILLIS_PROPERTY = "cassandra.readtimeoutmillis";

  public static final String TRACING_PROPERTY = "cassandra.tracing";
  public static final String TRACING_PROPERTY_DEFAULT = "false";

  public static final String USE_SSL_CONNECTION = "cassandra.useSSL";
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";

  /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static boolean debug = true;

  private static boolean trace = true;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the cluster has already been initialized
      if (session != null) {
        return;
      }

      try {

        debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
        trace =
            Boolean.valueOf(
                getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        String scb = getProperties().getProperty(SECURE_CONNECT_BUNDLE_PROPERTY);

        if (host == null && scb == null) {
          throw new DBException(
              String.format(
                  "Required properties \"%s\" or \"%s\" missing for CassandraCQLClient",
                  HOSTS_PROPERTY, SECURE_CONNECT_BUNDLE_PROPERTY));
        }

        builder = CqlSession.builder();

        if (scb != null) {
          logger.info("Connecting with SCB");
          builder.withCloudSecureConnectBundle(
              Paths.get(getProperties().getProperty(SECURE_CONNECT_BUNDLE_PROPERTY)));
        } else if (host != null) {
          String[] hosts = host.split(",");
          int port =
              Integer.parseInt(getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT));

          for (String h : hosts) {
            builder.addContactPoint(new InetSocketAddress(h.trim(), port));
          }
        }

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        builder.withAuthCredentials(username, password);

        String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);

        builder.withKeyspace(keyspace);

        // readConsistencyLevel = ConsistencyLevel.valueOf(
        //         getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
        //                 READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        // writeConsistencyLevel = ConsistencyLevel.valueOf(
        //         getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
        //                 WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        // Boolean useSSL = Boolean.parseBoolean(getProperties().getProperty(USE_SSL_CONNECTION,
        //         DEFAULT_USE_SSL_CONNECTION));
        // if ((username != null) && !username.isEmpty()) {
        //     Cluster.Builder clusterBuilder = Cluster.builder().withCredentials(username,
        // password)
        //             .withPort(Integer.valueOf(port)).addContactPoints(hosts);
        //     if (useSSL) {
        //         clusterBuilder = clusterBuilder.withSSL();
        //     }
        //     cluster = clusterBuilder.build();
        // } else {
        //     cluster = Cluster.builder().withPort(Integer.valueOf(port))
        //             .addContactPoints(hosts).build();
        // }
        // String maxConnections = getProperties().getProperty(
        //         MAX_CONNECTIONS_PROPERTY);
        // if (maxConnections != null) {
        //     cluster.getConfiguration().getPoolingOptions()
        //             .setMaxConnectionsPerHost(HostDistance.LOCAL,
        //                     Integer.valueOf(maxConnections));
        // }
        // String coreConnections = getProperties().getProperty(
        //         CORE_CONNECTIONS_PROPERTY);
        // if (coreConnections != null) {
        //     cluster.getConfiguration().getPoolingOptions()
        //             .setCoreConnectionsPerHost(HostDistance.LOCAL,
        //                     Integer.valueOf(coreConnections));
        // }
        // String connectTimoutMillis = getProperties().getProperty(
        //         CONNECT_TIMEOUT_MILLIS_PROPERTY);
        // if (connectTimoutMillis != null) {
        //     cluster.getConfiguration().getSocketOptions()
        //             .setConnectTimeoutMillis(Integer.valueOf(connectTimoutMillis));
        // }
        // String readTimoutMillis = getProperties().getProperty(
        //         READ_TIMEOUT_MILLIS_PROPERTY);
        // if (readTimoutMillis != null) {
        //     cluster.getConfiguration().getSocketOptions()
        //             .setReadTimeoutMillis(Integer.valueOf(readTimoutMillis));
        // }
        // Metadata metadata = cluster.getMetadata();
        // logger.info("Connected to cluster: {}\n",
        //         metadata.getClusterName());
        // for (Host discoveredHost : metadata.getAllHosts()) {
        //     logger.info("Datacenter: {}; Host: {}; Rack: {}\n",
        //             discoveredHost.getDatacenter(), discoveredHost.getAddress(),
        //             discoveredHost.getRack());
        // }
        session = builder.build();

        Metadata metadata = session.getMetadata();
        logger.info("Connected to cluster: {}\n", metadata.getClusterName());
      } catch (Exception e) {
        logger.error("Failed to connect to Cassandra");
        throw new DBException(e);
      }
    } // synchronized
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB instance per client
   * thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        readStmts.clear();
        scanStmts.clear();
        insertStmts.clear();
        updateStmts.clear();
        readAllStmt.set(null);
        // scanAllStmt.set(null);
        scanAllStmt.clear();
        deleteStmt.set(null);
        session.close();
        session = null;
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a
   * HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      PreparedStatement stmt = (fields == null) ? readAllStmt.get() : readStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        SelectFrom selectFrom = selectFrom(table);
        Select select = null;

        if (fields == null) {
          select = selectFrom.all();
        } else {
          for (String col : fields) {
            select = selectFrom.column(col);
          }
        }

        select = select.whereColumn(YCSB_KEY).isEqualTo(QueryBuilder.bindMarker(YCSB_KEY));
        select = select.limit(1);

        logger.info("Command generated for Select Read : {}", select.asCql());

        stmt =
            session.prepare(
                select.build().setConsistencyLevel(readConsistencyLevel).setTracing(trace));

        PreparedStatement prevStmt =
            (fields == null)
                ? readAllStmt.getAndSet(stmt)
                : readStmts.putIfAbsent(new HashSet(fields), stmt);

        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.getQuery());
      logger.debug("key = {}", key);

      ResultSet rs = session.execute(stmt.bind().setString(YCSB_KEY, key));

      // Should be only 1 row
      Row row = rs.one();

      if (row == null) {
        logger.info("Not found");
        return Status.NOT_FOUND;
      }

      ColumnDefinitions cd = row.getColumnDefinitions();

      for (ColumnDefinition def : cd) {
        ByteBuffer val = row.getBytesUnsafe(def.getName());
        if (val != null) {
          result.put(def.getName().toString(), new ByteArrayByteIterator(val.array()));
        } else {
          result.put(def.getName().toString(), null);
        }
      }

      return Status.OK;

    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the
   * result will be stored in a HashMap.
   *
   * <p>Cassandra CQL uses "token" method for range scan which doesn't always yield intuitive
   * results.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one
   *     record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {

    try {

      Set<String> fieldSet = new HashSet<>();

      if (fields == null) {
        fieldSet.add(String.valueOf(recordcount));
      } else {
        fieldSet = fields;
        fieldSet.add(String.valueOf(recordcount));
      }

      PreparedStatement stmt =
          (fields == null) ? scanAllStmt.get(fieldSet) : scanStmts.get(fieldSet);

      // Prepare statement on demand
      if (stmt == null) {
        SelectFrom selectFrom = selectFrom(table);
        Select select = null;

        if (fields == null) {
          select = selectFrom.all();
        } else {
          for (String col : fields) {
            select = selectFrom.column(col);
          }
        }

        select = select.whereRaw(String.format("token(%s) >= token(?)", YCSB_KEY, YCSB_KEY));
        select = select.limit(recordcount);
        // select = select.limit(QueryBuilder.bindMarker());

        logger.info("Command generated for Select Scan : {}", select.asCql());

        stmt =
            session.prepare(
                select.build().setConsistencyLevel(readConsistencyLevel).setTracing(trace));

        // PreparedStatement prevStmt =
        //     (fields == null)
        //         ? readAllStmt.getAndSet(stmt)
        //         : readStmts.putIfAbsent(new HashSet(fields), stmt);

        PreparedStatement prevStmt =
            (fields == null)
                ? scanAllStmt.putIfAbsent(fieldSet, stmt)
                : scanStmts.putIfAbsent(fieldSet, stmt);


        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.getQuery());
      logger.debug("startKey = {}, recordcount = {}", startkey, recordcount);

      // ResultSet rs = session.execute(stmt.bind(startkey, Integer.valueOf(recordcount)));
      ResultSet rs = session.execute(stmt.bind(startkey));

      HashMap<String, ByteIterator> tuple;
      while (rs.iterator().hasNext()) {
        Row row = rs.one();
        tuple = new HashMap<String, ByteIterator>();

        ColumnDefinitions cd = row.getColumnDefinitions();

        for (ColumnDefinition def : cd) {
          ByteBuffer val = row.getBytesUnsafe(def.getName());
          if (val != null) {
            tuple.put(def.getName().toString(), new ByteArrayByteIterator(val.array()));
          } else {
            tuple.put(def.getName().toString(), null);
          }
        }

        result.add(tuple);
      }

      return Status.OK;

    } catch (Exception e) {
      logger.error(
          MessageFormatter.format("Error scanning with startkey: {}", startkey).getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be
   * written into the record with the specified record key, overwriting any existing values with the
   * same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = updateStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        UpdateStart updateStart = QueryBuilder.update(table);
        UpdateWithAssignments updateWithAssignments = null;

        // Add fields
        for (String field : fields) {
          if (updateWithAssignments == null) {
            updateWithAssignments = updateStart.setColumn(field, QueryBuilder.bindMarker(field));
          } else {
            updateWithAssignments =
                updateWithAssignments.setColumn(field, QueryBuilder.bindMarker(field));
          }
        }
        // Add key
        Update updateStmt =
            updateWithAssignments
                .whereColumn(YCSB_KEY)
                .isEqualTo(QueryBuilder.bindMarker(YCSB_KEY));

        logger.info("Command generated for Update: {}", updateStmt.asCql());

        stmt =
            session.prepare(
                updateStmt.build().setConsistencyLevel(writeConsistencyLevel).setTracing(trace));

        PreparedStatement prevStmt = updateStmts.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.toString());
      logger.debug("key = {}", key);

      BoundStatement boundStmt = stmt.bind().setString(YCSB_KEY, key);
      // Add fields
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        boundStmt = boundStmt.setString(entry.getKey(), entry.getValue().toString());
      }
      session.execute(boundStmt);

      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be
   * written into the record with the specified record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = insertStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        InsertInto insertStmt = QueryBuilder.insertInto(table);
        RegularInsert insertValues = insertStmt.value(YCSB_KEY, QueryBuilder.bindMarker(YCSB_KEY));

        for (String field : fields) {
          insertValues = insertValues.value(field, QueryBuilder.bindMarker(field));
        }

        logger.info("Command generated for Insert: {}", insertValues.asCql());

        stmt =
            session.prepare(
                insertValues.build().setConsistencyLevel(writeConsistencyLevel).setTracing(trace));

        PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.getQuery());
      logger.debug("key = {}", key);

      // Add key
      BoundStatement boundStmt = stmt.bind().setString(YCSB_KEY, key);

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        boundStmt = boundStmt.setString(entry.getKey(), entry.getValue().toString());
      }

      session.execute(boundStmt);

      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {

    try {
      PreparedStatement stmt = deleteStmt.get();

      // Prepare statement on demand
      if (stmt == null) {
        DeleteSelection deleteSelection = QueryBuilder.deleteFrom(table);
        Delete delete = (Delete) deleteSelection;
        delete.whereColumn(YCSB_KEY).equals(QueryBuilder.bindMarker(YCSB_KEY));

        logger.info("Command generated for Delete: {}", delete.asCql());
        stmt =
            session.prepare(
                delete.build().setConsistencyLevel(writeConsistencyLevel).setTracing(trace));

        PreparedStatement prevStmt = stmt;
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.toString());
      logger.debug("key = {}", key);

      session.execute(stmt.bind().setString(YCSB_KEY, key));

      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }
}
