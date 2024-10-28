/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.bindParams;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(ParallelStatsDisabledIT.class)
public class ConditionTTLExpressionIT extends ParallelStatsDisabledIT {

    private static final Logger LOG = LoggerFactory.getLogger(ConditionTTLExpression.class);
    ManualEnvironmentEdge injectEdge;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void beforeTest(){
        EnvironmentEdgeManager.reset();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    @After
    public synchronized void afterTest() {
        EnvironmentEdgeManager.reset();
    }

    @Test
    public void testMaxLookback() throws Exception {
        final String tablename = "T_" + generateUniqueName();
        final int maxlookback = 600; // 600ms
        final int ttl = 1000;
        final String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "val varchar, expired boolean constraint pk primary key (k1,k2))" +
                "TTL = 1, MAX_LOOKBACK_AGE=%d";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = String.format(ddlTemplate, tablename, maxlookback);
            conn.createStatement().execute(ddl);
            conn.commit();
            long startTime = System.currentTimeMillis() + 1000;
            startTime = (startTime / 1000) * 1000;
            injectEdge.setValue(startTime);
            EnvironmentEdgeManager.injectEdge(injectEdge);
            String dml = "upsert into " + tablename + " VALUES(1,1, 'val_1', false)";
            conn.createStatement().execute(dml);
            conn.commit();
            injectEdge.incrementValue(2*maxlookback + 1);
            //dml = "upsert into " + tablename + " VALUES(1,1, 'val_1', true)";
            //conn.createStatement().execute(dml);
            //conn.commit();
            TestUtil.flush(getUtility(), TableName.valueOf(tablename));
            //injectEdge.incrementValue(500);
            TestUtil.majorCompact(getUtility(), TableName.valueOf(tablename));
        }
    }

    @Test
    public void testMaskingAndCompaction() throws Exception {
        final String tablename = "T_" + generateUniqueName();
        final String indexName = "I_" + generateUniqueName();
        final int maxlookback = 15; // 50ms
        final String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "val varchar, expired boolean constraint pk primary key (k1,k2))" +
                "TTL = 'expired', MAX_LOOKBACK_AGE=%d";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = String.format(ddlTemplate, tablename, maxlookback);
            conn.createStatement().execute(ddl);
            conn.commit();
            long startTime = System.currentTimeMillis() + 1000;
            startTime = (startTime / 1000) * 1000;
            injectEdge.setValue(startTime);
            EnvironmentEdgeManager.injectEdge(injectEdge);
            PreparedStatement dml = conn.prepareStatement("upsert into " + tablename + " VALUES(?, ?, ?, ?)");
            int rows = 5, cols = 2;
            int total = rows * cols;
            for (int i = 0; i < rows; ++i) {
                for (int j = 0; j < cols; ++j) {
                    dml.setInt(1, i);
                    dml.setInt(2, j);
                    dml.setString(3, "val_" +i);
                    dml.setBoolean(4, false);
                    dml.executeUpdate();
                }
            }
            conn.commit();
            PreparedStatement dql = conn.prepareStatement("select count(*) from " + tablename);
            ResultSet rs = dql.executeQuery();
            assertTrue(rs.next());
            assertEquals(total, rs.getInt(1));

            injectEdge.incrementValue(10);

            ddl = "create index " + indexName + " ON " + tablename + " (val) INCLUDE (expired)";
            conn.createStatement().execute(ddl);
            conn.commit();

            // expire odd rows by setting expired to true
            dml = conn.prepareStatement("upsert into " + tablename + "(k1, k2, expired) VALUES(?, ?, ?)");
            for (int i = 0; i < rows; ++i) {
                dml.setInt(1, i);
                dml.setInt(2, 1);
                dml.setBoolean(3, true);
                dml.executeUpdate();
            }
            conn.commit();

            rs = dql.executeQuery();
            PhoenixResultSet prs = rs.unwrap(PhoenixResultSet.class);
            String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
            assertTrue(explainPlan.contains(indexName));
            assertTrue(rs.next());
            // half the rows should be masked
            assertEquals(total/2, rs.getInt(1));

            dql = conn.prepareStatement("select /*+ NO_INDEX */ count(*) from " + tablename);
            rs = dql.executeQuery();
            assertTrue(rs.next());
            // half the rows should be masked
            assertEquals(total/2, rs.getInt(1));

            dql = conn.prepareStatement(
                    "select k1,k2 from " + tablename + " where val='val_3'");
            rs = dql.executeQuery();
            prs = rs.unwrap(PhoenixResultSet.class);
            explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
            assertTrue(explainPlan.contains(indexName));
            // only even row expected (3,0)
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
            assertFalse(rs.next());

            injectEdge.incrementValue(10);
            // now update the row again and set expired = false
            dml.setInt(1, 3);
            dml.setInt(2, 1);
            dml.setBoolean(3, false);
            dml.executeUpdate();
            conn.commit();

            // run the above query again 2 rows expected (3,0) and (3,1)
            rs = dql.executeQuery();
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());

            TestUtil.flush(getUtility(), TableName.valueOf(tablename));
            TestUtil.flush(getUtility(), TableName.valueOf(indexName));
            TestUtil.dumpTable(conn, TableName.valueOf(tablename));
            TestUtil.dumpTable(conn, TableName.valueOf(indexName));
            //injectEdge.incrementValue(maxlookback + 10);
            injectEdge.incrementValue(10);
            TestUtil.majorCompact(getUtility(), TableName.valueOf(tablename));
            TestUtil.dumpTable(conn, TableName.valueOf(tablename));
            dql = conn.prepareStatement("select /*+ NO_INDEX */ count(*) from " + tablename);
            rs = dql.executeQuery();
            assertTrue(rs.next());
            assertEquals(total/2 + 1, rs.getInt(1));
            TestUtil.dumpTable(conn, TableName.valueOf(indexName));
            TestUtil.majorCompact(getUtility(), TableName.valueOf(indexName));
            TestUtil.dumpTable(conn, TableName.valueOf(indexName));
        }
    }

    @Test
    public void testPhoenixRowTimestamp() throws Exception {
        final String tablename = "T_" + generateUniqueName();
        final String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "val varchar constraint pk primary key (k1,k2))" +
                "TTL = 'TO_NUMBER(CURRENT_TIME()) - TO_NUMBER(PHOENIX_ROW_TIMESTAMP()) >= 50'"; // 50ms ttl
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = String.format(ddlTemplate, tablename);
            conn.createStatement().execute(ddl);
            conn.commit();
            PreparedStatement dml = conn.prepareStatement("upsert into " + tablename + " VALUES(?, ?, ?)");
            int rows = 10, cols = 2;
            int total = rows * cols;
            for (int i = 0; i < rows; ++i) {
                for (int j = 0; j < cols; ++j) {
                    dml.setInt(1, i);
                    dml.setInt(2, j);
                    dml.setString(3, "val_" +i);
                    dml.executeUpdate();
                }
            }
            conn.commit();
            PreparedStatement dql = conn.prepareStatement("select count(*) from " + tablename);
            ResultSet rs = dql.executeQuery();
            assertTrue(rs.next());
            assertEquals(total, rs.getInt(1));
            // bump the current time to go past ttl value
            injectEdge.incrementValue(100);
            dql = conn.prepareStatement("select count(*) from " + tablename);
            rs = dql.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));

            // update 1 row
            dml.setInt(1, 7);
            dml.setInt(2, 1);
            dml.setString(3, "val_foo");
            dml.executeUpdate();
            conn.commit();

            dql = conn.prepareStatement("select count(*) from " + tablename);
            rs = dql.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }
}
