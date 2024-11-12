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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableOptions;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.TestUtil.CellCount;
import org.apache.phoenix.util.bson.TestFieldValue;
import org.apache.phoenix.util.bson.TestFieldsMap;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(ParallelStatsDisabledIT.class)
@RunWith(Parameterized.class)
public class ConditionTTLExpressionIT extends ParallelStatsDisabledIT {
    private static final Logger LOG = LoggerFactory.getLogger(ConditionTTLExpression.class);
    private static final Random RAND = new Random(11);
    private static final int MAX_ROWS = 1000;
    private static final String[] PK_COLUMNS = {"ID1", "ID2"};
    private static final String[] PK_COLUMN_TYPES = {"VARCHAR", "BIGINT"};
    private static final String[] COLUMNS = {
            "VAL1", "VAL2", "VAL3", "VAL4", "VAL5", "VAL6"
    };
    private static final String[] COLUMN_TYPES = {
            "CHAR(15)", "SMALLINT", "DATE", "TIMESTAMP", "BOOLEAN", "BSON"
    };
    // initialized to null
    private static final String[] DEFAULT_COLUMN_FAMILIES = new String [COLUMNS.length];
    // define for each column
    private static final String[] MULTI_COLUMN_FAMILIES = {
            null, "A", "A", "B", "C", "C"
    };

    static {
        assert COLUMNS.length == COLUMN_TYPES.length;
        assert COLUMNS.length == DEFAULT_COLUMN_FAMILIES.length;
        assert COLUMNS.length == MULTI_COLUMN_FAMILIES.length;
    }

    private ManualEnvironmentEdge injectEdge;
    private String tableDDLOptions;
    private final boolean multiCF;
    private final boolean columnEncoded;
    private final Integer tableLevelMaxLookback;
    // column names -> fully qualified column names
    private Map<String, String> columns = Maps.newHashMap();
    private SchemaBuilder schemaBuilder;
    // map of row-pos -> HBase row-key, used for verification
    private Map<Integer, String> rowPosToKey = Maps.newHashMap();

    public ConditionTTLExpressionIT(boolean multiCF,
                                    boolean columnEncoded,
                                    Integer tableLevelMaxLooback) {
        this.multiCF = multiCF;
        this.columnEncoded = columnEncoded;
        this.tableLevelMaxLookback = tableLevelMaxLooback; // in ms
        String[] columnFamilies = this.multiCF ? MULTI_COLUMN_FAMILIES : DEFAULT_COLUMN_FAMILIES;
        for (int i = 0; i < COLUMNS.length; ++i) {
            columns.put(COLUMNS[i], SchemaUtil.getColumnName(columnFamilies[i], COLUMNS[i]));
        }
        schemaBuilder = new SchemaBuilder(getUrl());
    }

    @Parameterized.Parameters(name = "multiCF={0}, columnEncoded={1}, tableLevelMaxLookback={2}")
    public static synchronized Collection<Object[]> data() {
        // maxlookback value is in ms
        return Arrays.asList(new Object[][]{
                {false, false, 0},
                {false, true, 0},
                {false, false, 15},
                {false, true, 15}
               // {true, false, 0},
                //{true, true, 15}
        });
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        // disabling global max lookback, will use table level max lookback
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(0));
        props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void beforeTest() {
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append(" TTL = '%s'"); // placeholder for TTL
        optionBuilder.append(", MAX_LOOKBACK_AGE=" + tableLevelMaxLookback);
        if (columnEncoded) {
            optionBuilder.append(", COLUMN_ENCODED_BYTES=2");
        } else {
            optionBuilder.append(", COLUMN_ENCODED_BYTES=0");
        }
        this.tableDDLOptions = optionBuilder.toString();
        EnvironmentEdgeManager.reset();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    @After
    public synchronized void afterTest() {
        EnvironmentEdgeManager.reset();
    }

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
                    dml.setString(3, "val_" + i);
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
            assertEquals(total / 2, rs.getInt(1));

            dql = conn.prepareStatement("select /*+ NO_INDEX */ count(*) from " + tablename);
            rs = dql.executeQuery();
            assertTrue(rs.next());
            // half the rows should be masked
            assertEquals(total / 2, rs.getInt(1));

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
            assertEquals(total / 2 + 1, rs.getInt(1));
            TestUtil.dumpTable(conn, TableName.valueOf(indexName));
            TestUtil.majorCompact(getUtility(), TableName.valueOf(indexName));
            TestUtil.dumpTable(conn, TableName.valueOf(indexName));
        }
    }

    @Test
    public void testBasicMaskingAndCompaction() throws Exception {
        String ttlCol = columns.get("VAL5");
        // ttl = '<FAMILY>.VAL4 = TRUE'
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            ResultSet rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(ttlCol));

            // expire 1 row by setting to true
            injectEdge.incrementValue(10);
            updateColumn(conn, 3, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 1, actual);

            // read the row again, this time it should be masked
            rs = readRow(conn, 3);
            assertFalse(rs.next());

            // expire 1 more row
            injectEdge.incrementValue(10);
            updateColumn(conn, 2, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 2, actual);

            // refresh the row again
            injectEdge.incrementValue(10);
            updateColumn(conn, 3, ttlCol, false);
            rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(ttlCol));
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 1, actual);

            // expire the row again
            injectEdge.incrementValue(10);
            updateColumn(conn, 3, ttlCol, true);

            // increment by atleast 2*maxlookback so that there are no updates within the
            // maxlookback window and no updates visible through the maxlookback window
            injectEdge.incrementValue(2* tableLevelMaxLookback + 5);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.addRow(rowPosToKey.get(i), COLUMNS.length + 1);
            }
            // remove the expired rows
            expectedCellCount.removeRow(rowPosToKey.get(2));
            expectedCellCount.removeRow(rowPosToKey.get(3));
            validateTable(conn, tableName, expectedCellCount);
        }
    }

    @Test
    public void testEverythingRetainedWithinMaxLookBack() throws Exception {
        if (tableLevelMaxLookback == 0) {
            return;
        }
        String ttlCol = columns.get("VAL5");
        // ttl = '<FAMILY>.VAL4 = TRUE'
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        long startTime = injectEdge.currentTime();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            ResultSet rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(ttlCol));

            // expire 1 row by setting to true
            injectEdge.incrementValue(1);
            updateColumn(conn, 3, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount - 1, actual);

            // read the row again, this time it should be masked
            rs = readRow(conn, 3);
            assertFalse(rs.next());

            // expire 1 more row
            injectEdge.incrementValue(1);
            updateColumn(conn, 2, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount - 2, actual);

            // refresh the row again
            injectEdge.incrementValue(1);
            updateColumn(conn, 3, ttlCol, false);
            rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(ttlCol));
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount - 1, actual);

            // expire the row again
            injectEdge.incrementValue(1);
            updateColumn(conn, 3, ttlCol, true);

            // all the updates are within the maxlookback window
            injectEdge.setValue(startTime + tableLevelMaxLookback);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.addRow(rowPosToKey.get(i), COLUMNS.length + 1);
            }
            // update cell count for expired rows
            updateExpectedCellCountForRow(2, 1, expectedCellCount); // updated 1 time
            updateExpectedCellCountForRow(3, 3, expectedCellCount); // updated 3 times
            validateTable(conn, tableName, expectedCellCount);
        }
    }

    @Test
    public void testLastVersionRetainedVisibleThroughMaxLookBack() throws Exception {
        if (tableLevelMaxLookback == 0) {
            return;
        }
        String ttlCol = columns.get("VAL5");
        // ttl = '<FAMILY>.VAL4 = TRUE'
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        long startTime = injectEdge.currentTime();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            ResultSet rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(ttlCol));

            // expire 1 row by setting to true
            injectEdge.incrementValue(1);
            updateColumn(conn, 3, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount - 1, actual);

            // read the row again, this time it should be masked
            rs = readRow(conn, 3);
            assertFalse(rs.next());

            // expire 1 more row
            injectEdge.incrementValue(1);
            updateColumn(conn, 2, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount - 2, actual);

            // refresh the row again
            injectEdge.incrementValue(1);
            updateColumn(conn, 3, ttlCol, false);
            rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(ttlCol));
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount - 1, actual);

            // expire the row again
            injectEdge.incrementValue(tableLevelMaxLookback);
            updateColumn(conn, 3, ttlCol, true);

            // only the last update should be visible through the maxlookback window
            injectEdge.incrementValue(tableLevelMaxLookback + 2);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.addRow(rowPosToKey.get(i), COLUMNS.length + 1);
            }
            // 1 row is expired
            expectedCellCount.removeRow(rowPosToKey.get(2));
            // Add 1 empty column cell to cover the gap
            expectedCellCount.addCell(rowPosToKey.get(3));
            validateTable(conn, tableName, expectedCellCount);
            actual = TestUtil.getRowCount(conn, tableName, true);
            // 1 row purged and 1 row masked
            assertEquals(rowCount - 2, actual);
        }
    }

    @Test
    public void testPhoenixRowTimestamp() throws Exception {
        int ttl = 50;
        String ttlExpression = String.format("TO_NUMBER(CURRENT_TIME()) - TO_NUMBER(PHOENIX_ROW_TIMESTAMP())" +
                " >= %d", ttl); // equivalent to a ttl of 50ms
        createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);

            // bump the time so that the ttl expression evaluates to true
            injectEdge.incrementValue(ttl);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(0, actual);

            // update VAL3 column of row 1
            updateColumn(conn, 1, columns.get("VAL4"), injectEdge.currentTime());
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(1, actual);

            // advance the time by maxlookbackwindow but still within ttl
            // only the last version is retained no bread crumbs
            injectEdge.incrementValue(tableLevelMaxLookback + 2);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            expectedCellCount.addRow(rowPosToKey.get(1), COLUMNS.length + 1);
            validateTable(conn, tableName, expectedCellCount);
        }
    }

    @Test
    public void testDeleteMarkers() throws Exception {
        String ttlCol = columns.get("VAL5");
        // ttl = '<FAMILY>.VAL4 = TRUE'
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);
            injectEdge.incrementValue(5);
            String dml = String.format("delete from %s where ID1 = ? and ID2 = ?", tableName);
            PreparedStatement ps = conn.prepareStatement(dml);
            // delete rows 2, 3
            int [] rowsToDelete = new int[]{2, 3};
            for (int rowPosition : rowsToDelete) {
                List<Object> pkCols = generatePKColumnValues(rowPosition);
                for (int i = 0; i < pkCols.size(); ++i) {
                    ps.setObject(i + 1, pkCols.get(i));
                }
                ps.executeUpdate();
            }
            conn.commit();
            // expire row # 1
            injectEdge.incrementValue(1);
            updateColumn(conn, 1, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            // 1 row expired, 2 deleted
            assertEquals(2, actual);
            if (tableLevelMaxLookback == 0) {
                // increment so that all updates are outside of max lookback
                injectEdge.incrementValue(2);
                doMajorCompaction(tableName);
                // only 2 rows should be retained
                CellCount expectedCellCount = new CellCount();
                expectedCellCount.addRow(rowPosToKey.get(0), COLUMNS.length + 1);
                expectedCellCount.addRow(rowPosToKey.get(4), COLUMNS.length + 1);
                validateTable(conn, tableName, expectedCellCount);
            } else {
                // all updates within the max lookback window, retain everything
                doMajorCompaction(tableName);
                CellCount expectedCellCount = new CellCount();
                for (int i = 0; i < rowCount; ++i) {
                    // additional cell for empty column
                    expectedCellCount.addRow(rowPosToKey.get(i), COLUMNS.length + 1);
                }
                // update cell count for expired rows
                updateExpectedCellCountForRow(1, 1, expectedCellCount); // updated 1 time
                for (int rowPosition : rowsToDelete) {
                    // one DeleteFamily cell
                    expectedCellCount.addCell(rowPosToKey.get(rowPosition));
                }
                validateTable(conn, tableName, expectedCellCount);
                // increment so that the delete markers are outside of max lookback but the
                // expired row is still visible
                injectEdge.incrementValue(tableLevelMaxLookback + 1);
                doMajorCompaction(tableName);
                for (int rowPosition : rowsToDelete) {
                    expectedCellCount.removeRow(rowPosToKey.get(rowPosition));
                }
                // only the latest version of expired row is retained
                expectedCellCount.addRow(rowPosToKey.get(1), COLUMNS.length + 1);
                validateTable(conn, tableName, expectedCellCount);

                // purge the expired row also
                injectEdge.incrementValue(tableLevelMaxLookback + 1);
                doMajorCompaction(tableName);
                expectedCellCount.removeRow(rowPosToKey.get(1));
                validateTable(conn, tableName, expectedCellCount);
            }
        }
    }

    @Test
    public void testDateExpression() throws Exception {
        // ttl = 'CURRENT_DATE() >= <FAMILY>.VAL2 + 1'  // 1 day beyond the value stored in VAL2
        String ttlCol = columns.get("VAL3");
        String ttlExpression = String.format("CURRENT_DATE() >= %s + 1", ttlCol);
        createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);

            // bump the time so that the ttl expression evaluates to true
            injectEdge.incrementValue(QueryConstants.MILLIS_IN_DAY + 1200);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(0, actual);

            // update VAL2 column of row 2
            updateColumn(conn, 2, ttlCol, new Date(injectEdge.currentTime()));
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(1, actual);

            // advance the time by maxlookbackwindow but still within ttl
            // only the last version is retained no bread crumbs
            injectEdge.incrementValue(tableLevelMaxLookback + 2);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            expectedCellCount.addRow(rowPosToKey.get(2), COLUMNS.length + 1);
            validateTable(conn, tableName, expectedCellCount);
        }
    }

    @Test
    public void testBsonDataType() throws Exception {
        String ttlCol = columns.get("VAL6");
        String ttlExpression = String.format(
                "BSON_VALUE(%s, ''attr_0'', ''VARCHAR'') IS NULL", ttlCol);
        createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            actual = TestUtil.getRowCount(conn, tableName, true);
            // only odd rows (1,3) have non null attribute value
            assertEquals(2, actual);

            // increment by atleast 2*maxlookback so that there are no updates within the
            // maxlookback window and no updates visible through the maxlookback window
            injectEdge.incrementValue(2* tableLevelMaxLookback + 5);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // only odd rows should be retained
                if (i % 2 != 0) {
                    // additional cell for empty column
                    expectedCellCount.addRow(rowPosToKey.get(i), COLUMNS.length + 1);
                }
            }
            validateTable(conn, tableName, expectedCellCount);
        }
    }

    @Test
    public void testCDCIndex() throws Exception {
        String ttlCol = columns.get("VAL2");
        // VAL2 = -1
        String ttlExpression = String.format("%s = -1", ttlCol);
        createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String cdcName = generateUniqueName();
            String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            conn.createStatement().execute(cdc_sql);
            populateTable(conn, rowCount);
            String schemaName = SchemaUtil.getSchemaNameFromFullName(tableName);
            String cdcIndexName = SchemaUtil.getTableName(schemaName,
                    CDCUtil.getCDCIndexName(cdcName));
            PTable cdcIndex = ((PhoenixConnection) conn).getTableNoCache(cdcIndexName);
            assertEquals(cdcIndex.getTTL(), TTLExpression.TTL_EXPRESSION_FORVER);

            // get row count on base table no row should be masked
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);

            // get raw row count on cdc index table
            actual = TestUtil.getRawRowCount(conn, TableName.valueOf(cdcIndexName));
            assertEquals(rowCount, actual);

            // Advance time by the max lookback age. This will cause all rows in cdc index to expire
            injectEdge.incrementValue(tableLevelMaxLookback + 2);

            // Major compact the CDC index. This will remove all expired rows
            TestUtil.doMajorCompaction(conn, cdcIndexName);
            // get raw row count on cdc index table
            actual = TestUtil.getRawRowCount(conn, TableName.valueOf(cdcIndexName));
            assertEquals(0, actual);

            // table should still have all the rows intact
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);
        }
    }

    private void validateTable(Connection conn, String tableName, CellCount expectedCellCount)
            throws Exception {
        CellCount actualCellCount = TestUtil.getRawCellCount(conn, TableName.valueOf(tableName));
        try {
            assertEquals(expectedCellCount, actualCellCount);
        } catch (AssertionError e) {
            try {
                TestUtil.dumpTable(conn, TableName.valueOf(tableName));
                for (Map.Entry<Integer, String> entry : rowPosToKey.entrySet()) {
                    String rowKey = entry.getValue();
                    LOG.info(String.format("Key=%s expected=%d, actual=%d",
                            Bytes.toStringBinary(rowKey.getBytes()),
                            expectedCellCount.getCellCount(rowKey),
                            actualCellCount.getCellCount(rowKey)));
                }
            } finally {
                throw e;
            }
        }
    }

    private void doMajorCompaction(String tableName) throws IOException, InterruptedException {
        TestUtil.flush(getUtility(), TableName.valueOf(tableName));
        TestUtil.majorCompact(getUtility(), TableName.valueOf(tableName));
    }

    private void createTable(String ttlExpression,
                             boolean createIndex) throws Exception {
        TableOptions tableOptions = new TableOptions();
        tableOptions.setTablePKColumns(Arrays.asList(PK_COLUMNS));
        tableOptions.setTablePKColumnTypes(Arrays.asList(PK_COLUMN_TYPES));
        tableOptions.setTableColumns(Arrays.asList(COLUMNS));
        tableOptions.setTableColumnTypes(Arrays.asList(COLUMN_TYPES));
        tableOptions.setTableProps(String.format(tableDDLOptions, ttlExpression));
        SchemaBuilder.OtherOptions otherOptions = new SchemaBuilder.OtherOptions();
        String[] columnFamilies = this.multiCF ? MULTI_COLUMN_FAMILIES : DEFAULT_COLUMN_FAMILIES;
        otherOptions.setTableCFs(Arrays.asList(columnFamilies));
        schemaBuilder.withTableOptions(tableOptions).withOtherOptions(otherOptions).build();
    }

    private void injectEdge() {
        long startTime = System.currentTimeMillis() + 1000;
        startTime = (startTime / 1000) * 1000;
        injectEdge.setValue(startTime);
        EnvironmentEdgeManager.injectEdge(injectEdge);
    }

    private List<Object> generatePKColumnValues(int rowPosition) {
        final String ID1_FORMAT = "id1_%d";
        String id1 = String.format(ID1_FORMAT, rowPosition / 2);
        int id2 = rowPosition;
        return Lists.newArrayList(id1, id2);
    }

    private BsonDocument generateBsonDocument(int rowPosition) {
        Map<String, TestFieldValue> map = new HashMap<>();
        if (rowPosition % 2 != 0) {
            map.put("attr_0", new TestFieldValue().withS("str_val_" + rowPosition));
        }
        map.put("attr_1", new TestFieldValue().withN(rowPosition * rowPosition));
        map.put("attr_2", new TestFieldValue().withBOOL(rowPosition % 2 == 0));
        TestFieldsMap testFieldsMap = new TestFieldsMap();
        testFieldsMap.setMap(map);
        return org.apache.phoenix.util.bson.TestUtil.getBsonDocument(testFieldsMap);
    }

    private List<Object> generateRow(int rowPosition) {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        List<Object> pkCols = generatePKColumnValues(rowPosition);
        String val1 = "val1_" + RAND.nextInt(MAX_ROWS);
        int val2 = RAND.nextInt(MAX_ROWS);
        Date val3 = new Date(startTime + RAND.nextInt(MAX_ROWS));
        Timestamp val4 = new Timestamp(val3.getTime());
        boolean val5 = false;
        BsonDocument val6 = generateBsonDocument(rowPosition);
        List<Object> cols = Lists.newArrayList(val1, val2, val3, val4, val5, val6);
        List<Object> values = Lists.newArrayListWithExpectedSize(pkCols.size() + cols.size());
        values.addAll(pkCols);
        values.addAll(cols);
        return values;
    }

    private void updateColumn(Connection conn,
                              int rowPosition,
                              String columnName,
                              Object newColumnValue) throws Exception {
        String tableName = schemaBuilder.getEntityTableName();
        List<String> upsertColumns = Lists.newArrayList();
        upsertColumns.addAll(Arrays.asList(PK_COLUMNS));
        upsertColumns.add(columnName);
        StringBuilder buf = new StringBuilder("UPSERT INTO ");
        buf.append(tableName);
        buf.append(" (").append(Joiner.on(",").join(upsertColumns)).append(") VALUES(");
        for (int i = 0; i < upsertColumns.size(); i++) {
            buf.append("?,");
        }
        buf.setCharAt(buf.length() - 1, ')');
        List<Object> upsertValues = Lists.newArrayList();
        upsertValues.addAll(generatePKColumnValues(rowPosition));
        upsertValues.add(newColumnValue);
        try (PreparedStatement stmt = conn.prepareStatement(buf.toString())) {
            for (int i = 0; i < upsertValues.size(); i++) {
                stmt.setObject(i + 1, upsertValues.get(i));
            }
            stmt.executeUpdate();
            conn.commit();
        }
    }

    private void updateRow(Connection conn, int rowPosition) throws Exception {
        String tableName = schemaBuilder.getEntityTableName();
        List<Object> upsertValues = generateRow(rowPosition);
        StringBuilder buf = new StringBuilder("UPSERT INTO ");
        buf.append(tableName);
        buf.append(" VALUES(");
        for (int i = 0; i < upsertValues.size(); i++) {
            buf.append("?,");
        }
        buf.setCharAt(buf.length() - 1, ')');
        try (PreparedStatement stmt = conn.prepareStatement(buf.toString())) {
            for (int i = 0; i < upsertValues.size(); i++) {
                stmt.setObject(i + 1, upsertValues.get(i));
            }
            stmt.executeUpdate();
            conn.commit();
        }
    }

    private void populateTable(Connection conn, int rowCount) throws Exception {
        for (int i = 0; i < rowCount; ++i) {
            updateRow(conn, i);
        }
        // used for verification purposes
        populateRowPosToRowKey(conn);
    }

    /**
     * TODO
     * @param conn
     * @throws SQLException
     */
    private void populateRowPosToRowKey(Connection conn) throws SQLException {
        String tableName = schemaBuilder.getEntityTableName();
        String query = "SELECT ID2, ROWKEY_BYTES_STRING() FROM " + tableName;
        try (ResultSet rs = conn.createStatement().executeQuery(query)) {
            while (rs.next()) {
                int rowPos = rs.getInt(1);
                String rowKey = rs.getString(2); // StringBinary format
                rowPosToKey.put(rowPos, Bytes.toString(Bytes.toBytesBinary(rowKey)));
            }
        }
    }

    private ResultSet readRow(Connection conn, int rowPosition) throws SQLException {
        String tableName = schemaBuilder.getEntityTableName();
        String query = String.format("select * FROM %s where ID1 = ? AND ID2 = ?", tableName);
        List<Object> pkCols = generatePKColumnValues(rowPosition);
        PreparedStatement ps = conn.prepareStatement(query);
        for (int i = 0; i < pkCols.size(); ++i) {
            ps.setObject(i + 1, pkCols.get(i));
        }
        return ps.executeQuery();
    }

    private void updateExpectedCellCountForRow(int rowPosition,
                                               int updateCount,
                                               CellCount expectedCellCount) {
        String rowKey = rowPosToKey.get(rowPosition);
        for (int update = 0; update < updateCount; ++update) {
            // 1 update adds 2 cells 1 for the column and 1 empty cell
            expectedCellCount.addCell(rowKey);
            expectedCellCount.addCell(rowKey);
        }
    }
}