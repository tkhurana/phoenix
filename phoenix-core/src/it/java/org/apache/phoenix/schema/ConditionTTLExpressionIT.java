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

import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.SCANNED_DATA_ROW_COUNT;
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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.OtherOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableIndexOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableOptions;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
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
    private Map<Integer, String> dataRowPosToKey = Maps.newHashMap();
    private Map<Integer, String> indexRowPosToKey = Maps.newHashMap();

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

    @Test
    public void testBasicMaskingAndCompaction() throws Exception {
        String ttlCol = columns.get("VAL5");
        // ttl = '<FAMILY>.VAL4 = TRUE'
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        List<String> indexedColumns = Lists.newArrayList(columns.get("VAL1"));
        List<String> includedColumns = Lists.newArrayList(ttlCol);
        createTable(ttlExpression, indexedColumns, includedColumns, false);
        String tableName = schemaBuilder.getEntityTableName();
        String indexName = schemaBuilder.getEntityTableIndexName();
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
            injectEdge.incrementValue(1);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 1, actual);
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            Assert.assertEquals(rowCount - 1, actual);

            // read the row again, this time it should be masked
            rs = readRow(conn, 3);
            assertFalse(rs.next());

            // expire 1 more row
            injectEdge.incrementValue(10);
            updateColumn(conn, 2, ttlCol, true);
            injectEdge.incrementValue(1);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 2, actual);
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            Assert.assertEquals(rowCount - 2, actual);

            // refresh the row again
            injectEdge.incrementValue(10);
            updateColumn(conn, 3, ttlCol, false);
            injectEdge.incrementValue(1);
            rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(ttlCol));
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 1, actual);
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
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
                expectedCellCount.addRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
            }
            // remove the expired rows
            expectedCellCount.removeRow(dataRowPosToKey.get(2));
            expectedCellCount.removeRow(dataRowPosToKey.get(3));
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey);
            doMajorCompaction(indexName);
            expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.addRow(indexRowPosToKey.get(i), includedColumns.size() + 1);
            }
            // remove the expired rows
            expectedCellCount.removeRow(indexRowPosToKey.get(2));
            expectedCellCount.removeRow(indexRowPosToKey.get(3));
            validateTable(conn, indexName, expectedCellCount, indexRowPosToKey);
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
        createTable(ttlExpression);
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
                expectedCellCount.addRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
            }
            // update cell count for expired rows
            updateExpectedCellCountForRow(2, 1,
                    dataRowPosToKey, expectedCellCount); // updated 1 time
            updateExpectedCellCountForRow(3, 3,
                    dataRowPosToKey, expectedCellCount); // updated 3 times
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey);
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
        createTable(ttlExpression);
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
                expectedCellCount.addRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
            }
            // 1 row is expired
            expectedCellCount.removeRow(dataRowPosToKey.get(2));
            // Add 1 empty column cell to cover the gap
            expectedCellCount.addCell(dataRowPosToKey.get(3));
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey);
            actual = TestUtil.getRowCount(conn, tableName, true);
            // 1 row purged and 1 row masked
            assertEquals(rowCount - 2, actual);
        }
    }

    @Test
    public void testPartialRowRetainedInMaxLookBack() throws Exception {
        if (tableLevelMaxLookback == 0) {
            return;
        }
        String ttlCol = columns.get("VAL5");
        // ttl = '<FAMILY>.VAL4 = TRUE'
        String ttlExpression = String.format("%s=TRUE OR %s is null", ttlCol, ttlCol);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 1;
        long actual;
        long startTime = injectEdge.currentTime();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            // expire 1 row by setting to true
            injectEdge.incrementValue(1);
            updateColumn(conn, 0, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount - 1, actual);
            // all previous updates of the expired row fall out of maxlookback + ttl window
            injectEdge.incrementValue(2*tableLevelMaxLookback+5);
            // update another column not part of ttl expression
            updateColumn(conn, 0, columns.get("VAL2"), 2345);
            // only the last update should be visible through the maxlookback window
            injectEdge.incrementValue(1);
            doMajorCompaction(tableName);
            injectEdge.incrementValue(tableLevelMaxLookback + 5);
            doMajorCompaction(tableName);
            /*CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.addRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
            }
            // 1 row is expired
            expectedCellCount.removeRow(dataRowPosToKey.get(2));
            // Add 1 empty column cell to cover the gap
            expectedCellCount.addCell(dataRowPosToKey.get(3));
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey);
            actual = TestUtil.getRowCount(conn, tableName, true);
            // 1 row purged and 1 row masked
            assertEquals(rowCount - 2, actual);
             */
        }
    }

    @Test
    public void testPhoenixRowTimestamp() throws Exception {
        int ttl = 50;
        String ttlExpression = String.format("TO_NUMBER(CURRENT_TIME()) - TO_NUMBER(PHOENIX_ROW_TIMESTAMP())" +
                " >= %d", ttl); // equivalent to a ttl of 50ms
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);

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
            expectedCellCount.addRow(dataRowPosToKey.get(1), COLUMNS.length + 1);
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey);
        }
    }

    @Test
    public void testDeleteMarkers() throws Exception {
        String ttlCol = columns.get("VAL5");
        // ttl = '<FAMILY>.VAL4 = TRUE'
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
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
                expectedCellCount.addRow(dataRowPosToKey.get(0), COLUMNS.length + 1);
                expectedCellCount.addRow(dataRowPosToKey.get(4), COLUMNS.length + 1);
                validateTable(conn, tableName, expectedCellCount, dataRowPosToKey);
            } else {
                // all updates within the max lookback window, retain everything
                doMajorCompaction(tableName);
                CellCount expectedCellCount = new CellCount();
                for (int i = 0; i < rowCount; ++i) {
                    // additional cell for empty column
                    expectedCellCount.addRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
                }
                // update cell count for expired rows
                updateExpectedCellCountForRow(1, 1,
                        dataRowPosToKey, expectedCellCount); // updated 1 time
                for (int rowPosition : rowsToDelete) {
                    // one DeleteFamily cell
                    expectedCellCount.addCell(dataRowPosToKey.get(rowPosition));
                }
                validateTable(conn, tableName, expectedCellCount, dataRowPosToKey);
                // increment so that the delete markers are outside of max lookback but the
                // expired row is still visible
                injectEdge.incrementValue(tableLevelMaxLookback + 1);
                doMajorCompaction(tableName);
                for (int rowPosition : rowsToDelete) {
                    expectedCellCount.removeRow(dataRowPosToKey.get(rowPosition));
                }
                // only the latest version of expired row is retained
                expectedCellCount.addRow(dataRowPosToKey.get(1), COLUMNS.length + 1);
                validateTable(conn, tableName, expectedCellCount, dataRowPosToKey);

                // purge the expired row also
                injectEdge.incrementValue(tableLevelMaxLookback + 1);
                doMajorCompaction(tableName);
                expectedCellCount.removeRow(dataRowPosToKey.get(1));
                validateTable(conn, tableName, expectedCellCount, dataRowPosToKey);
            }
        }
    }

    @Test
    public void testDateExpression() throws Exception {
        // ttl = 'CURRENT_DATE() >= <FAMILY>.VAL3 + 1'  // 1 day beyond the value stored in VAL3
        String ttlCol = columns.get("VAL3");
        String ttlExpression = String.format("CURRENT_DATE() >= %s + 1", ttlCol);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            // bump the time so that the ttl expression evaluates to true
            injectEdge.incrementValue(QueryConstants.MILLIS_IN_DAY + 1200);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(0, actual);

            // update column of row 2
            updateColumn(conn, 2, ttlCol, new Date(injectEdge.currentTime()));
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(1, actual);

            // advance the time by maxlookbackwindow but still within ttl
            // only the last version is retained no bread crumbs
            injectEdge.incrementValue(tableLevelMaxLookback + 2);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            expectedCellCount.addRow(dataRowPosToKey.get(2), COLUMNS.length + 1);
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey);
        }
    }

    @Test
    public void testNull() throws Exception {
        createTable("3600");
        String ttlCol = columns.get("VAL2");
        String ttlExpression = String.format("%s > 5", ttlCol);
        String fullDataTableName = schemaBuilder.getEntityTableName();
        int rowCount = 1;
        injectEdge();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            injectEdge.incrementValue(10);
            updateColumn(conn, 0, ttlCol, null);
            injectEdge.incrementValue(10);
            updateColumn(conn, 0, columns.get("VAL1"), "qwer");
            injectEdge.incrementValue(10);
            TestUtil.dumpTable(conn, TableName.valueOf(fullDataTableName));
            String dql = String.format("SELECT count(*) from %s where NOT (%s)", fullDataTableName, ttlExpression);
            try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
                assertTrue(rs.next());
                System.out.println(rs.getInt(1));
            }
            dql = String.format("SELECT count(*) from %s where %s", fullDataTableName, ttlExpression);
            try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
                assertTrue(rs.next());
                System.out.println(rs.getInt(1));
            }
        }
    }

    @Test
    public void testIndexTool() throws Exception {
        String ttlCol = columns.get("VAL5");
        // ttl = '<FAMILY>.VAL4 = TRUE'
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression);
        String fullDataTableName = schemaBuilder.getEntityTableName();
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullDataTableName);
        String tableName = SchemaUtil.getTableNameFromFullName(fullDataTableName);
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            injectEdge.incrementValue(10);
            deleteRow(conn, 2);
            injectEdge.incrementValue(10);
            // expire some rows
            updateColumn(conn, 0, ttlCol, true);
            updateColumn(conn, 4, ttlCol, true);
            injectEdge.incrementValue(10);
            // now create the index async
            String indexName = generateUniqueName();
            String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
            String indexDDL = String.format("create index %s on %s (%s) include (%s) async",
                    indexName, fullDataTableName, columns.get("VAL1"), ttlCol);
            conn.createStatement().execute(indexDDL);
            IndexToolIT.runIndexTool(false, schemaName, tableName, indexName);
            populateRowPosToRowKey(conn, false);

            // Both the tables should have the same row count from Phoenix
            actual = TestUtil.getRowCount(conn, fullDataTableName, true);
            assertEquals(rowCount -(2+1), actual); // 2 expired, 1 deleted
            actual = TestUtil.getRowCountFromIndex(conn, fullDataTableName, fullIndexName);
            assertEquals(rowCount -(2+1), actual);

            // raw row count should include masked rows
            actual = TestUtil.getRawRowCount(conn, TableName.valueOf(fullDataTableName));
            assertEquals(rowCount, actual);
            actual = TestUtil.getRawRowCount(conn, TableName.valueOf(fullIndexName));
            assertEquals(rowCount, actual);

            // run index verification
            IndexTool it = IndexToolIT.runIndexTool(false, schemaName, tableName, indexName,
                    null, 0, IndexTool.IndexVerifyType.ONLY);
            CounterGroup mrJobCounters = IndexToolIT.getMRJobCounters(it);
            try {
                assertEquals(rowCount,
                        mrJobCounters.findCounter(SCANNED_DATA_ROW_COUNT.name()).getValue());
                assertEquals(rowCount,
                        mrJobCounters.findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT.name()).getValue());
            } catch (AssertionError e) {
                IndexToolIT.dumpMRJobCounters(mrJobCounters);
                throw e;
            }
            injectEdge.incrementValue(2*tableLevelMaxLookback + 5);
            doMajorCompaction(fullDataTableName);
            doMajorCompaction(fullIndexName);

            CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.addRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
            }
            // remove the expired rows
            expectedCellCount.removeRow(dataRowPosToKey.get(0));
            expectedCellCount.removeRow(dataRowPosToKey.get(4));
            // remove the deleted row
            expectedCellCount.removeRow(dataRowPosToKey.get(2));
            validateTable(conn, fullDataTableName, expectedCellCount, dataRowPosToKey);

            expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // 1 cell for empty column and 1 for included column
                expectedCellCount.addRow(indexRowPosToKey.get(i), 2);
            }
            // remove the expired rows
            expectedCellCount.removeRow(indexRowPosToKey.get(0));
            expectedCellCount.removeRow(indexRowPosToKey.get(4));
            // remove the deleted row
            expectedCellCount.removeRow(indexRowPosToKey.get(2));
            validateTable(conn, fullIndexName, expectedCellCount, indexRowPosToKey);
        }
    }

    @Test
    public void testBsonDataType() throws Exception {
        String ttlCol = columns.get("VAL6");
        String ttlExpression = String.format(
                "BSON_VALUE(%s, ''attr_0'', ''VARCHAR'') IS NULL", ttlCol);
        createTable(ttlExpression);
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
                    expectedCellCount.addRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
                }
            }
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey);
        }
    }

    @Test
    public void testCDCIndex() throws Exception {
        String ttlCol = columns.get("VAL2");
        // VAL2 = -1
        String ttlExpression = String.format("%s = -1", ttlCol);
        createTable(ttlExpression);
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

    private void validateTable(Connection conn,
                               String tableName,
                               CellCount expectedCellCount,
                               Map<Integer, String> rowPosToKey) throws Exception {

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

    private void createTable(String ttlExpression) throws Exception {
        createTable(ttlExpression, Collections.EMPTY_LIST, Collections.EMPTY_LIST, false);
    }

    private void createTable(String ttlExpression,
                             List<String> indexedColumns,
                             List<String> includedColumns,
                             boolean isAsync) throws Exception {
        TableOptions tableOptions = new TableOptions();
        tableOptions.setTablePKColumns(Arrays.asList(PK_COLUMNS));
        tableOptions.setTablePKColumnTypes(Arrays.asList(PK_COLUMN_TYPES));
        tableOptions.setTableColumns(Arrays.asList(COLUMNS));
        tableOptions.setTableColumnTypes(Arrays.asList(COLUMN_TYPES));
        tableOptions.setTableProps(String.format(tableDDLOptions, ttlExpression));
        tableOptions.setMultiTenant(false);
        OtherOptions otherOptions = new OtherOptions();
        String[] columnFamilies = this.multiCF ? MULTI_COLUMN_FAMILIES : DEFAULT_COLUMN_FAMILIES;
        otherOptions.setTableCFs(Arrays.asList(columnFamilies));
        schemaBuilder.withTableOptions(tableOptions).withOtherOptions(otherOptions);
        if (!indexedColumns.isEmpty()) {
            TableIndexOptions indexOptions = new TableIndexOptions();
            indexOptions.setTableIndexColumns(indexedColumns);
            indexOptions.setTableIncludeColumns(includedColumns);
            String indexProps = isAsync ? "ASYNC" : "";
            indexOptions.setIndexProps(indexProps);
            schemaBuilder.withTableIndexOptions(indexOptions);
        }
        schemaBuilder.build();
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

    private void deleteRow(Connection conn, int rowPosition) throws SQLException {
        String tableName = schemaBuilder.getEntityTableName();
        String dml = String.format("delete from %s where ID1 = ? and ID2 = ?", tableName);
        try (PreparedStatement ps = conn.prepareStatement(dml)) {
            List<Object> pkCols = generatePKColumnValues(rowPosition);
            for (int i = 0; i < pkCols.size(); ++i) {
                ps.setObject(i + 1, pkCols.get(i));
            }
            ps.executeUpdate();
            conn.commit();
        }
    }

    private void populateTable(Connection conn, int rowCount) throws Exception {
        for (int i = 0; i < rowCount; ++i) {
            updateRow(conn, i);
        }
        // used for verification purposes
        populateRowPosToRowKey(conn, true);
        if (schemaBuilder.isTableIndexCreated()) {
            populateRowPosToRowKey(conn, false);
        }
    }

    /**
     * TODO
     * @param conn
     * @throws SQLException
     */
    private void populateRowPosToRowKey(Connection conn, boolean skipIndex) throws SQLException {
        String tableName = schemaBuilder.getEntityTableName();
        String query = String.format("SELECT %s ID2, ROWKEY_BYTES_STRING() FROM %s",
                (skipIndex ? "/*+ NO_INDEX */" : ""), tableName);
        Map<Integer, String> rowPosToKey = skipIndex ? dataRowPosToKey : indexRowPosToKey;
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
                                               Map<Integer, String> rowPosToKey,
                                               CellCount expectedCellCount) {
        String rowKey = rowPosToKey.get(rowPosition);
        for (int update = 0; update < updateCount; ++update) {
            // 1 update adds 2 cells 1 for the column and 1 empty cell
            expectedCellCount.addCell(rowKey);
            expectedCellCount.addCell(rowKey);
        }
    }
}