/**
 * Copyright (c) 2015 YCSB contributors All rights reserved.
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
 */
package site.ycsb.db;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.truncate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

/** Integration tests for the Cassandra client */
public class CassandraCQLClientTest {
  // Change the default Cassandra timeout from 10s to 120s for slow CI machines

  private static final long timeout = 120000L;

  private static final String TABLE = "usertable";
  private static final String HOST = "localhost";
  private static final int PORT = 9142;
  private static final String DEFAULT_ROW_KEY = "user1";

  private CassandraCQLClient client;
  private CqlSession session;

  @ClassRule
  public static CassandraCQLUnit cassandraUnit =
      new CassandraCQLUnit(new ClassPathCQLDataSet("ycsb.cql", "ycsb"), null, timeout);

  @Before
  public void setUp() throws Exception {
    session = cassandraUnit.getSession();

    Properties p = new Properties();
    p.setProperty("hosts", HOST);
    p.setProperty("port", Integer.toString(PORT));
    p.setProperty("table", TABLE);

    Measurements.setProperties(p);
    final CoreWorkload workload = new CoreWorkload();
    workload.init(p);
    client = new CassandraCQLClient();
    client.setProperties(p);
    client.init();
  }

  @After
  public void tearDownClient() throws Exception {
    if (client != null) {
      client.cleanup();
    }
    client = null;
  }

  @After
  public void clearTable() throws Exception {
    // Clear the table so that each test starts fresh.
    final Truncate truncate = truncate(TABLE);
    if (cassandraUnit != null) {
      cassandraUnit.getSession().execute(truncate.build());
    }
  }

  @Test
  public void testReadMissingRow() throws Exception {
    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Status status = client.read(TABLE, "Missing row", null, result);
    assertThat(result.size(), is(0));
    assertThat(status, is(Status.NOT_FOUND));
  }

  private void insertRow() {
    final String rowKey = DEFAULT_ROW_KEY;

    Insert insertStmt =
        QueryBuilder.insertInto("my_keyspace", TABLE) // Add keyspace if necessary
            .value("ycsb_key", QueryBuilder.literal(rowKey))
            .value("field0", QueryBuilder.literal("value1"))
            .value("field1", QueryBuilder.literal("value2"));

    SimpleStatement statement = insertStmt.build();

    session.execute(statement);
  }

  @Test
  public void testRead() throws Exception {
    insertRow();

    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Status status = client.read(TABLE, DEFAULT_ROW_KEY, null, result);
    assertThat(status, is(Status.OK));
    assertThat(result.entrySet(), hasSize(11));
    assertThat(result, hasEntry("field2", null));

    final HashMap<String, String> strResult = new HashMap<String, String>();
    for (final Map.Entry<String, ByteIterator> e : result.entrySet()) {
      if (e.getValue() != null) {
        strResult.put(e.getKey(), e.getValue().toString());
      }
    }
    assertThat(strResult, hasEntry(CassandraCQLClient.YCSB_KEY, DEFAULT_ROW_KEY));
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testReadSingleColumn() throws Exception {
    insertRow();
    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Set<String> fields = Sets.newHashSet("field1");
    final Status status = client.read(TABLE, DEFAULT_ROW_KEY, fields, result);
    assertThat(status, is(Status.OK));
    assertThat(result.entrySet(), hasSize(1));
    final Map<String, String> strResult = StringByteIterator.getStringMap(result);
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testInsert() throws Exception {
    final String key = "key";
    final Map<String, String> input = new HashMap<String, String>();
    input.put("field0", "value1");
    input.put("field1", "value2");

    final Status status = client.insert(TABLE, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    // Verify result
    Select selectStmt =
        QueryBuilder.selectFrom("my_keyspace", TABLE)
            .columns("field0", "field1")
            .whereColumn(CassandraCQLClient.YCSB_KEY)
            .isEqualTo(QueryBuilder.literal(key))
            .limit(1);

    // Build the final SimpleStatement from the query
    SimpleStatement statement = selectStmt.build();

    // Execute the query and fetch the result
    ResultSet rs = session.execute(statement);
    Row row = rs.one();

    assertThat(row, notNullValue());
    assertThat(rs.isFullyFetched(), is(true));
    assertThat(row.getString("field0"), is("value1"));
    assertThat(row.getString("field1"), is("value2"));
  }

  @Test
  public void testUpdate() throws Exception {
    insertRow();
    final Map<String, String> input = new HashMap<String, String>();
    input.put("field0", "new-value1");
    input.put("field1", "new-value2");

    final Status status =
        client.update(TABLE, DEFAULT_ROW_KEY, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    // Verify result
    Select selectStmt =
        QueryBuilder.selectFrom("my_keyspace", TABLE)
            .columns("field0", "field1")
            .whereColumn(CassandraCQLClient.YCSB_KEY)
            .isEqualTo(QueryBuilder.literal(DEFAULT_ROW_KEY))
            .limit(1);

    // Build the final SimpleStatement from the query
    SimpleStatement statement = selectStmt.build();

    // Execute the query and fetch the result
    ResultSet rs = session.execute(statement);
    Row row = rs.one();

    assertThat(row, notNullValue());
    assertThat(rs.isFullyFetched(), is(true));
    assertThat(row.getString("field0"), is("new-value1"));
    assertThat(row.getString("field1"), is("new-value2"));
  }

  @Test
  public void testDelete() throws Exception {
    insertRow();

    final Status status = client.delete(TABLE, DEFAULT_ROW_KEY);
    assertThat(status, is(Status.OK));

    // Verify result
    Select selectStmt =
        QueryBuilder.selectFrom("my_keyspace", TABLE)
            .columns("field0", "field1")
            .whereColumn(CassandraCQLClient.YCSB_KEY)
            .isEqualTo(QueryBuilder.literal(DEFAULT_ROW_KEY))
            .limit(1);

    // Build the final SimpleStatement from the query
    SimpleStatement statement = selectStmt.build();

    // Execute the query and fetch the result
    ResultSet rs = session.execute(statement);
    Row row = rs.one();

    assertThat(row, nullValue());
  }

  @Test
  public void testPreparedStatements() throws Exception {
    final int LOOP_COUNT = 3;
    for (int i = 0; i < LOOP_COUNT; i++) {
      testInsert();
      testUpdate();
      testRead();
      testReadSingleColumn();
      testReadMissingRow();
      testDelete();
    }
  }
}
