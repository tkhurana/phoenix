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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.DataSupplier;
import org.apache.phoenix.query.PhoenixTestBuilder.DataWriter;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableOptions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
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
            "VAL0", "VAL1", "VAL2", "VAL3", "VAL4", "VAL5", "VAL6"
    };
    // define for each column
    private static final String[] DEFAULT_COLUMN_FAMILIES = {
            null, null, null, null, null, null, null
    };
    // define for each column
    private static final String[] MULTI_COLUMN_FAMILIES = {
            null, "A", "A", "B", "B", "B", "C"
    };
    private static final String[] COLUMN_TYPES = {
            "CHAR(15)", "SMALLINT", "DATE", "TIMESTAMP", "BOOLEAN", "JSON", "BSON"
    };
    private static final String ID1_FORMAT = "00A0y000%07d";

    private ManualEnvironmentEdge injectEdge;
    private String tableDDLOptions;
    private final boolean multiCF;
    private final boolean columnEncoded;
    private final Integer tableLevelMaxLooback;
    private final String[] columnFamilies;
    private final String[] columnNames = new String[COLUMNS.length];

    public ConditionTTLExpressionIT(boolean multiCF,
                                    boolean columnEncoded,
                                    Integer tableLevelMaxLooback) {
        this.multiCF = multiCF;
        this.columnEncoded = columnEncoded;
        this.tableLevelMaxLooback = tableLevelMaxLooback; // in ms
        this.columnFamilies = this.multiCF ? MULTI_COLUMN_FAMILIES : DEFAULT_COLUMN_FAMILIES;
        for (int colIndex = 0; colIndex < COLUMNS.length; ++colIndex) {
            columnNames[colIndex] =
                    SchemaUtil.getColumnName(columnFamilies[colIndex], COLUMNS[colIndex]);
        }
    }

    @Parameterized.Parameters(name = "multiCF={0}, columnEncoded={1}, tableLevelMaxLookback={3}")
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
    public void beforeTest(){
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
    public void testMultipleCF() throws Exception {
        final String tablename = "T_" + generateUniqueName();
        final String ddlTemplate = "create table %s (id varchar not null primary key," +
                "val2 bigint, a.val3 smallint, b.val4 varchar)" +
                "TTL = 'val2 = 0 AND a.val3 = -1 AND b.val4 is null'";
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
            int total = 10;
            for (int i = 0; i < total; ++i) {
                dml.setString(1, "id_" + i);
                dml.setInt(2, i);
                dml.setInt(3, i*i);
                dml.setString(4, "val4_" + i);
                dml.executeUpdate();
            }
            conn.commit();
            injectEdge.incrementValue(10);
            // update 1 row
            dml.setString(1, "id_" + 1);
            dml.setInt(2, 0);
            dml.setInt(3, -1);
            dml.setString(4, null);
            dml.executeUpdate();
            conn.commit();
            TestUtil.flush(getUtility(), TableName.valueOf(tablename));
            TestUtil.dumpTable(conn, TableName.valueOf(tablename));
            TestUtil.majorCompact(getUtility(), TableName.valueOf(tablename));
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

    @Test
    public void testBasic() throws Exception {
        // ttl = 'VAL4 = TRUE'
        String ttlExpression = String.format("%s=TRUE", columnNames[4]);
        SchemaBuilder schemaBuilder = createTable(ttlExpression, false);
        String tableName = schemaBuilder.getEntityTableName();
        LOG.info("created table {}", tableName);
        injectEdge();
        int rowCount = 5;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, schemaBuilder, rowCount);
            TestUtil.dumpTable(conn, TableName.valueOf(tableName));
        }
    }

    private SchemaBuilder createTable(String ttlExpression,
                                      boolean createIndex) throws Exception {
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        TableOptions tableOptions = new TableOptions();
        tableOptions.setTablePKColumns(Arrays.asList(PK_COLUMNS));
        tableOptions.setTablePKColumnTypes(Arrays.asList(PK_COLUMN_TYPES));
        tableOptions.setTableColumns(Arrays.asList(COLUMNS));
        tableOptions.setTableColumnTypes(Arrays.asList(COLUMN_TYPES));
        tableOptions.setTableProps(String.format(tableDDLOptions, ttlExpression));
        SchemaBuilder.OtherOptions otherOptions = new SchemaBuilder.OtherOptions();
        otherOptions.setTableCFs(Arrays.asList(columnFamilies));
        schemaBuilder.withTableOptions(tableOptions).withOtherOptions(otherOptions).build();
        return schemaBuilder;
    }

    private void injectEdge() {
        long startTime = System.currentTimeMillis() + 1000;
        startTime = (startTime / 1000) * 1000;
        injectEdge.setValue(startTime);
        EnvironmentEdgeManager.injectEdge(injectEdge);
    }

    private List<Object> generateRowKey(int rowIndex) {
        String id1 = String.format(ID1_FORMAT, rowIndex % 2);
        int id2 = rowIndex;
        return Lists.newArrayList(id1, id2);
    }

    private void populateTable(Connection conn,
                               SchemaBuilder schemaBuilder,
                               int rowCount) throws Exception {
        DataSupplier dataSupplier = new DataSupplier() {

            long startTime = EnvironmentEdgeManager.currentTimeMillis();
            @Override
            public List<Object> getValues(int rowIndex) throws Exception {
                // "CHAR(15), "SMALLINT", "DATE", "TIMESTAMP", "BOOLEAN", "JSON", "BSON"
                List<Object> pkCols = generateRowKey(rowIndex);
                String val0 = "val0_" + RAND.nextInt(MAX_ROWS);
                int val1 = RAND.nextInt(MAX_ROWS);
                Date val2 = new Date(startTime + RAND.nextInt(MAX_ROWS));
                Timestamp val3 = new Timestamp(val2.getTime());
                boolean val4 = false;
                List<Object> cols = Lists.newArrayList(
                        val0, val1, val2, val3, val4, null, null);
                List<Object> values = Lists.newArrayListWithExpectedSize(
                        pkCols.size() + cols.size());
                values.addAll(pkCols);
                values.addAll(cols);
                return values;
            }
        };
        DataWriter dataWriter = new PhoenixTestBuilder.BasicDataWriter();
        dataWriter.setConnection(conn);
        dataWriter.setDataSupplier(dataSupplier);
        String[] allCols = ArrayUtils.addAll(PK_COLUMNS, columnNames);
        dataWriter.setRowKeyColumns(Arrays.asList(PK_COLUMNS));
        dataWriter.setUpsertColumns(Arrays.asList(allCols));
        dataWriter.setTargetEntity(schemaBuilder.getEntityTableName());
        dataWriter.upsertRows(0, rowCount);
    }

    private void updateRow(Connection conn,
                           SchemaBuilder schemaBuilder,
                           int rowIndex,
                           List<Integer> colPositions,
                           List<Object> colValues) throws Exception {
        assert (colPositions.size() == colValues.size());
        DataSupplier dataSupplier = new DataSupplier() {

            @Override
            public List<Object> getValues(int rowIndex) throws Exception {
                return null;
            }
        };
    }
}
