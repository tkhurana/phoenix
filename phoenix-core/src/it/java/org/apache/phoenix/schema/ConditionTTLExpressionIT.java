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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.DataSupplier;
import org.apache.phoenix.query.PhoenixTestBuilder.DataWriter;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableOptions;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.TestUtil.CellCount;
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
            "VAL0", "VAL1", "VAL2", "VAL3", "VAL4"
    };
    // define for each column
    private static final String[] DEFAULT_COLUMN_FAMILIES = {
            null, null, null, null, null
    };
    // define for each column
    private static final String[] MULTI_COLUMN_FAMILIES = {
            null, "A", "A", "B", "C"
    };
    private static final String[] COLUMN_TYPES = {
            "CHAR(15)", "SMALLINT", "DATE", "TIMESTAMP", "BOOLEAN"
    };

    private ManualEnvironmentEdge injectEdge;
    private String tableDDLOptions;
    private final boolean multiCF;
    private final boolean columnEncoded;
    private final Integer tableLevelMaxLooback;
    private final String[] allColumns = new String[PK_COLUMNS.length + COLUMNS.length];
    private SchemaBuilder schemaBuilder;
    // map of row-pos -> HBase row-key
    private Map<Integer, String> rowPosToKey = Maps.newHashMap();

    public ConditionTTLExpressionIT(boolean multiCF,
                                    boolean columnEncoded,
                                    Integer tableLevelMaxLooback) {
        this.multiCF = multiCF;
        this.columnEncoded = columnEncoded;
        this.tableLevelMaxLooback = tableLevelMaxLooback; // in ms
        int pos = 0;
        for (int i = 0; i < PK_COLUMNS.length; ++i) {
            allColumns[pos++] = PK_COLUMNS[i];
        }
        String[] columnFamilies = this.multiCF ? MULTI_COLUMN_FAMILIES : DEFAULT_COLUMN_FAMILIES;
        for (int i = 0; i < COLUMNS.length; ++i) {
            allColumns[pos++] = SchemaUtil.getColumnName(columnFamilies[i], COLUMNS[i]);
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
                {false, true, 15},
                {true, false, 0},
                {true, true, 15}
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
        optionBuilder.append(", MAX_LOOKBACK_AGE=" + tableLevelMaxLooback);
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
    public void testDeleteMarkers() throws Exception {
        final String tablename = "T_" + generateUniqueName();
        final String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "val varchar, expired boolean constraint pk primary key (k1,k2))" +
                "TTL = 'expired'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = String.format(ddlTemplate, tablename);
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
            injectEdge.incrementValue(10);
            dml = conn.prepareStatement("delete from " + tablename + " where k2=1");
            dml.executeUpdate();
            conn.commit();
            injectEdge.incrementValue(10);
            dml = conn.prepareStatement("upsert into " + tablename + "(k1, k2, expired) VALUES(?, ?, ?)");
            for (int i = 0; i < rows; ++i) {
                dml.setInt(1, i);
                dml.setInt(2, 1);
                dml.setBoolean(3, true);
                dml.executeUpdate();
            }
            conn.commit();
            TestUtil.flush(getUtility(), TableName.valueOf(tablename));
            TestUtil.majorCompact(getUtility(), TableName.valueOf(tablename));
        }
    }

    @Test
    public void testBasicMaskingAndCompaction() throws Exception {
        // ttl = 'VAL4 = TRUE'
        int colPos = PK_COLUMNS.length + 4;
        String ttlExpression = String.format("%s=TRUE", allColumns[colPos]);
        createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            ResultSet rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(colPos + 1));
            // expire 1 row by setting to true
            injectEdge.incrementValue(10);
            updateColumn(conn, 3, colPos, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 1, actual);
            // read the row again, this time it should be masked
            rs = readRow(conn, 3);
            assertFalse(rs.next());
            // expire 1 more row
            injectEdge.incrementValue(10);
            updateColumn(conn, 2, colPos, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 2, actual);
            // refresh the row again
            injectEdge.incrementValue(10);
            updateColumn(conn, 3, colPos, false);
            rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(colPos + 1));
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 1, actual);
            // expire the row again
            injectEdge.incrementValue(10);
            updateColumn(conn, 3, colPos, true);
            // increment by atleast 2*maxlookback so that there are no updates within the
            // maxlookback window and no updates visible through the maxlookback window
            injectEdge.incrementValue(2*tableLevelMaxLooback + 5);
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
        if (tableLevelMaxLooback == 0) {
            return;
        }
        // ttl = 'VAL4 = TRUE'
        int colPos = PK_COLUMNS.length + 4;
        String ttlExpression = String.format("%s=TRUE", allColumns[colPos]);
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
            assertFalse(rs.getBoolean(colPos + 1));
            // expire 1 row by setting to true
            injectEdge.incrementValue(1);
            updateColumn(conn, 3, colPos, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 1, actual);
            // read the row again, this time it should be masked
            rs = readRow(conn, 3);
            assertFalse(rs.next());
            // expire 1 more row
            injectEdge.incrementValue(1);
            updateColumn(conn, 2, colPos, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 2, actual);
            // refresh the row again
            injectEdge.incrementValue(1);
            updateColumn(conn, 3, colPos, false);
            rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(colPos + 1));
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 1, actual);
            // expire the row again
            injectEdge.incrementValue(1);
            updateColumn(conn, 3, colPos, true);
            // all the updates are within the maxlookback window
            injectEdge.setValue(startTime + tableLevelMaxLooback);
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
        if (tableLevelMaxLooback == 0) {
            return;
        }
        // ttl = 'VAL4 = TRUE'
        int colPos = PK_COLUMNS.length + 4;
        String ttlExpression = String.format("%s=TRUE", allColumns[colPos]);
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
            assertFalse(rs.getBoolean(colPos + 1));
            // expire 1 row by setting to true
            injectEdge.incrementValue(1);
            updateColumn(conn, 3, colPos, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 1, actual);
            // read the row again, this time it should be masked
            rs = readRow(conn, 3);
            assertFalse(rs.next());
            // expire 1 more row
            injectEdge.incrementValue(1);
            updateColumn(conn, 2, colPos, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 2, actual);
            // refresh the row again
            injectEdge.incrementValue(1);
            updateColumn(conn, 3, colPos, false);
            rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(colPos + 1));
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(rowCount - 1, actual);
            // expire the row again
            injectEdge.incrementValue(tableLevelMaxLooback);
            updateColumn(conn, 3, colPos, true);
            // only the last update is visible through the maxlookback window
            injectEdge.incrementValue(tableLevelMaxLooback + 2);
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
            Assert.assertEquals(rowCount - 2, actual);
        }
    }

    @Test
    public void testPhoenixRowTimestamp() throws Exception {
        String ttlExpression =
                "TO_NUMBER(CURRENT_TIME()) - TO_NUMBER(PHOENIX_ROW_TIMESTAMP()) >= 50"; // 50ms TTL
        createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        int ttl = 50;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);
            // bump the time so that the ttl expression evaluates to true
            injectEdge.incrementValue(ttl);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(0, actual);
            // update VAL3 column of row 1
            updateColumn(conn, 1, PK_COLUMNS.length + 3, injectEdge.currentTime());
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(1, actual);
            // advance the time by maxlookbackwindow but still within ttl
            // only the last version is retained no bread crumbs
            injectEdge.incrementValue(tableLevelMaxLooback + 2);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            expectedCellCount.addRow(rowPosToKey.get(1), COLUMNS.length + 1);
            validateTable(conn, tableName, expectedCellCount);
        }
    }

    @Test
    public void testDateExpression() throws Exception {
        // ttl = 'CURRENT_DATE() >= VAL2 + 1'  // 1 day beyond the value stored in VAL2
        int colPos = PK_COLUMNS.length + 2;  // VAL2
        String ttlExpression = String.format("CURRENT_DATE() >= %s + 1", allColumns[colPos]);
        createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 3;
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
            updateColumn(conn, 2, colPos, new Date(injectEdge.currentTime()));
            if (columnEncoded == true) {
                // update VAL3
                //updateColumn(conn, 2, colPos + 1,
                        //new Timestamp(injectEdge.currentTime()));
                // update VAL4
                //updateColumn(conn, 2,colPos + 2, false);
            }
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(1, actual);
            // advance the time by maxlookbackwindow but still within ttl
            // only the last version is retained no bread crumbs
            injectEdge.incrementValue(tableLevelMaxLooback + 2);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            expectedCellCount.addRow(rowPosToKey.get(2), COLUMNS.length + 1);
            validateTable(conn, tableName, expectedCellCount);
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

    private Object[] generatePKColumnValues(int rowPosition) {
        final String ID1_FORMAT = "id1_%d";
        String id1 = String.format(ID1_FORMAT, rowPosition / 2);
        int id2 = rowPosition;
        return new Object[] {id1, id2};
    }

    private List<Object> generateRow(int rowPosition) {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        Object[] pkCols = generatePKColumnValues(rowPosition);
        String val0 = "val0_" + RAND.nextInt(MAX_ROWS);
        int val1 = RAND.nextInt(MAX_ROWS);
        Date val2 = new Date(startTime + RAND.nextInt(MAX_ROWS));
        Timestamp val3 = new Timestamp(val2.getTime());
        boolean val4 = false;
        List<Object> cols = Lists.newArrayList(val0, val1, val2, val3, val4);
        List<Object> values = Lists.newArrayListWithExpectedSize(
                pkCols.length + cols.size());
        values.addAll(Arrays.asList(pkCols));
        values.addAll(cols);
        return values;
    }

    private void populateTable(Connection conn, int rowCount) throws Exception {
        for (int i = 0; i < rowCount; ++i) {
            upsertRow(conn, i, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        }
        populateRowPosToRowKey(conn);
    }

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

    private void updateColumn(Connection conn,
                              int rowPosition,
                              int columnPosition,
                              Object newColumnValue) throws Exception {
        upsertRow(conn, rowPosition,
                Lists.newArrayList(columnPosition),
                Lists.newArrayList(newColumnValue));
    }

    private void upsertRow(Connection conn,
                           int rowPosition,
                           List<Integer> columnsToUpdate,
                           List<Object> columnValues) throws Exception {
        assert (columnsToUpdate.size() == columnValues.size());
        DataSupplier dataSupplier = new DataSupplier() {
            @Override
            public List<Object> getValues(int rowPosition) throws Exception {
                if (columnsToUpdate.isEmpty()) {
                    // full row update
                    return generateRow(rowPosition);
                }
                Object[] allColumnValues = new Object[allColumns.length];
                Object[] pkCols = generatePKColumnValues(rowPosition);
                for (int i = 0; i < pkCols.length; ++i) {
                    allColumnValues[i] = pkCols[i];
                }
                for (int i = 0; i < columnsToUpdate.size(); ++i) {
                    int colPos = columnsToUpdate.get(i);
                    Object val = columnValues.get(i);
                    allColumnValues[colPos] = val;
                }
                return Arrays.asList(allColumnValues);
            }
        };
        DataWriter dataWriter = new PhoenixTestBuilder.BasicDataWriter();
        dataWriter.setConnection(conn);
        dataWriter.setDataSupplier(dataSupplier);
        dataWriter.setRowKeyColumns(Arrays.asList(PK_COLUMNS));
        dataWriter.setUpsertColumns(Arrays.asList(allColumns));
        dataWriter.setTargetEntity(schemaBuilder.getEntityTableName());
        if (!columnsToUpdate.isEmpty()) {
            List<Integer> columnsToUpdateWithPKCols = Lists.newArrayList();
            for (int i = 0; i < PK_COLUMNS.length; ++i) {
                columnsToUpdateWithPKCols.add(i);
            }
            columnsToUpdateWithPKCols.addAll(columnsToUpdate);
            dataWriter.setColumnPositionsToUpdate(columnsToUpdateWithPKCols);
        }
        dataWriter.upsertRow(rowPosition);
    }

    private ResultSet readRow(Connection conn, int rowPosition) throws SQLException {
        String tableName = schemaBuilder.getEntityTableName();
        String query = String.format("select * FROM %s where ID1 = ? AND ID2 = ?", tableName);
        Object[] pkCols = generatePKColumnValues(rowPosition);
        PreparedStatement ps = conn.prepareStatement(query);
        for (int i = 0; i < pkCols.length; ++i) {
            ps.setObject(i + 1, pkCols[i]);
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