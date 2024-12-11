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

import static org.apache.phoenix.exception.SQLExceptionCode.AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_TTL_EXPRESSION;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_SET_CONDITION_TTL_ON_TABLE_WITH_MULTIPLE_COLUMN_FAMILIES;
import static org.apache.phoenix.schema.PTableType.INDEX;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.retainSingleQuotes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.phoenix.compile.OrderByCompiler;
import org.apache.phoenix.compile.QueryCompilerTest;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Ignore;
import org.junit.Test;

public class ConditionTTLExpressionTest extends BaseConnectionlessQueryTest {

    private static void assertConditonTTL(Connection conn, String tableName, String ttlExpr) throws SQLException {
        TTLExpression expected = new ConditionTTLExpression(ttlExpr);
        assertTTL(conn, tableName, expected);
    }

    private static void assertTTL(Connection conn, String tableName, TTLExpression expected) throws SQLException {
        PTable table = conn.unwrap(PhoenixConnection.class).getTable(tableName);
        assertEquals(expected, table.getTTL());
    }

    private static void assertScanConditionTTL(Scan scan, Scan scanWithCondTTL) {
        assertEquals(scan.includeStartRow(), scanWithCondTTL.includeStartRow());
        assertArrayEquals(scan.getStartRow(), scanWithCondTTL.getStartRow());
        assertEquals(scan.includeStopRow(), scanWithCondTTL.includeStopRow());
        assertArrayEquals(scan.getStopRow(), scanWithCondTTL.getStopRow());
        Filter filter = scan.getFilter();
        Filter filterCondTTL = scanWithCondTTL.getFilter();
        assertNotNull(filter);
        assertNotNull(filterCondTTL);
        if (filter instanceof FilterList) {
            assertTrue(filterCondTTL instanceof FilterList);
            FilterList filterList = (FilterList) filter;
            FilterList filterListCondTTL = (FilterList) filterCondTTL;
            // ultimately compares the individual filters
            assertEquals(filterList, filterListCondTTL);
        } else {
            assertEquals(filter, filterCondTTL);
        }
    }

    private void compareScanWithCondTTL(Connection conn,
                                        String tableNoTTL,
                                        String tableWithTTL,
                                        String queryTemplate,
                                        String ttl) throws SQLException {
        compareScanWithCondTTL(conn, tableNoTTL, tableWithTTL, queryTemplate, ttl, false);
    }

    private void compareScanWithCondTTL(Connection conn,
                                        String tableNoTTL,
                                        String tableWithTTL,
                                        String queryTemplate,
                                        String ttl,
                                        boolean useIndex) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        String query;
        // Modify the query by adding the cond ttl expression explicitly to the WHERE clause
        if (queryTemplate.toUpperCase().contains(" WHERE ")) {
            // append the cond TTL expression to the WHERE clause
            query = String.format(queryTemplate + " AND NOT (%s)", tableNoTTL, ttl);
        } else {
            // add a WHERE clause with negative cond ttl expression
            query = String.format(queryTemplate + " WHERE NOT (%s)", tableNoTTL, ttl);
        }
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        if (useIndex) {
            assertTrue(plan.getTableRef().getTable().getType() == INDEX);
        }
        Scan scanNoTTL = plan.getContext().getScan();
        // now execute the same query with cond ttl expression implicitly used for masking
        query = String.format(queryTemplate, tableWithTTL);
        pstmt = new PhoenixPreparedStatement(pconn, query);
        plan = pstmt.optimizeQuery();
        if (useIndex) {
            assertTrue(plan.getTableRef().getTable().getType() == INDEX);
        }
        ResultIterator iterator = plan.iterator();
        Scan scanWithCondTTL = plan.getContext().getScan();
        assertScanConditionTTL(scanNoTTL, scanWithCondTTL);
    }

    private void validateScan(Connection conn,
                              String tableName,
                              String query,
                              String ttl,
                              boolean useIndex,
                              int expectedNonPKColsInTTLExpr) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        if (useIndex) {
            assertTrue(plan.getTableRef().getTable().getType() == INDEX);
        }
        plan.iterator(); // create the iterator to initialize the scan
        Scan scan = plan.getContext().getScan();
        Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
        PTable table = pconn.getTable(tableName);
        ConditionTTLExpression condTTL = (ConditionTTLExpression) table.getTTL();
        Set<ColumnReference> columnsReferenced = condTTL.getColumnsReferenced(pconn, table);
        assertEquals(expectedNonPKColsInTTLExpr, columnsReferenced.size());
        for (ColumnReference colRef : columnsReferenced) {
            NavigableSet<byte[]> set = familyMap.get(colRef.getFamily());
            assertNotNull(set);
            assertTrue(set.contains(colRef.getQualifier()));
        }
    }

    @Test
    public void testBasicExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k1 > 5 AND col1 < 'zzzzzz'";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            String query = String.format("SELECT count(*) from %s where k1 > 3", tableName);
            validateScan(conn, tableName, query, ttl, false, 1);
        }
    }

    @Test(expected = TypeMismatchException.class)
    public void testNotBooleanExpr() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k1 + 100";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
        }
    }

    @Test(expected = TypeMismatchException.class)
    public void testWrongArgumentValue() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k1 = ''abc''";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
        }
    }

    @Test(expected = PhoenixParserException.class)
    public void testParsingError() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k2 == 23";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
        }
    }

    @Test
    public void testMultipleColumnFamilyNotAllowed() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "A.col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "A.col1 = 'expired'";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(CANNOT_SET_CONDITION_TTL_ON_TABLE_WITH_MULTIPLE_COLUMN_FAMILIES.getErrorCode(),
                e.getErrorCode());
        } catch (Exception e) {
            fail("Unknown exception " + e);
        }
    }

    @Test
    public void testSingleNonDefaultColumnFamilyIsAllowed() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "A.col1 varchar, A.col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "A.col1 = 'expired' AND A.col2 + 10 > CURRENT_DATE()";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test
    public void testDefaultColumnFamily() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'," +
                "DEFAULT_COLUMN_FAMILY='cf'";
        String ttl = "col1 = 'expired' AND col2 + 10 > CURRENT_DATE()";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test
    public void testAggregateExpressionNotAllowed() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "SUM(k2) > 23";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_TTL_EXPRESSION.getErrorCode(),
                    e.getErrorCode());
        } catch (Exception e) {
            fail("Unknown exception " + e);
        }
    }

    @Test
    public void testNullExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String tableName = generateUniqueName();
        String ttl = "col1 is NULL AND col2 < CURRENT_DATE() + 30000";
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            String query = String.format("SELECT count(*) from %s", tableName);
            validateScan(conn, tableName, query, ttl, false, 2);
        }
    }

    @Test
    public void testBooleanColumn() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "val varchar, expired BOOLEAN constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String tableName = generateUniqueName();
        String indexName = "I_" + tableName;
        String indexTemplate = "create index %s on %s (val) include (expired)";
        String ttl = "expired";
        String query;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format(ddlTemplate, tableName, ttl);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);

            query = String.format("SELECT k1, k2 from %s where (k1,k2) IN ((1,2), (3,4))",
                    tableName);
            validateScan(conn, tableName, query, ttl, false, 1);

            ddl = String.format(indexTemplate, indexName, tableName);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, indexName, ttl);

            // validate the scan on index
            query = String.format("SELECT count(*) from %s", tableName);
            validateScan(conn, tableName, query, ttl, true, 1);
        }
    }

    @Test
    public void testNot() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, expired BOOLEAN " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "NOT expired";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test
    public void testPhoenixRowTimestamp() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String tableName = generateUniqueName();
        String ttl = "PHOENIX_ROW_TIMESTAMP() < CURRENT_DATE() - 100";
        String ddl = String.format(ddlTemplate, tableName, ttl);
        String query;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            query = String.format("select col1 from %s where k1 = 7 AND k2 > 12", tableName);
            validateScan(conn, tableName, query, ttl, false, 0);
        }
    }

    @Test
    public void testBooleanCaseExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, status char(1) " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "CASE WHEN status = ''E'' THEN TRUE ELSE FALSE END";
        String expectedTTLExpr = "CASE WHEN status = 'E' THEN TRUE ELSE FALSE END";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, expectedTTLExpr);
        }
    }

    @Test
    public void testCondTTLOnTopLevelView() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null primary key," +
                "k2 bigint, col1 varchar, status char(1))";
        String tableName = generateUniqueName();
        String viewName = generateUniqueName();
        String viewTemplate = "create view %s (k3 smallint) as select * from %s WHERE k1=7 TTL = '%s'";
        String ttl = "k2 = 34 and k3 = -1";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format(ddlTemplate, tableName);
            conn.createStatement().execute(ddl);
            ddl = String.format(viewTemplate, viewName, tableName, ttl);
            conn.createStatement().execute(ddl);
            assertTTL(conn, tableName, TTLExpression.TTL_EXPRESSION_NOT_DEFINED);
            assertConditonTTL(conn, viewName, ttl);
            String query = String.format("select k3 from %s", viewName);
            validateScan(conn, viewName, query, ttl, false, 2);
        }
    }

    @Test
    public void testCondTTLOnMultiLevelView() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null primary key," +
                "k2 bigint, col1 varchar, status char(1))";
        String tableName = generateUniqueName();
        String parentView = generateUniqueName();
        String childView = generateUniqueName();
        String parentViewTemplate = "create view %s (k3 smallint) as select * from %s WHERE k1=7";
        String childViewTemplate = "create view %s as select * from %s TTL = '%s'";
        String ttl = "k2 = 34 and k3 = -1";
        String ddl = String.format(ddlTemplate, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            ddl = String.format(parentViewTemplate, parentView, tableName);
            conn.createStatement().execute(ddl);
            ddl = String.format(childViewTemplate, childView, parentView, ttl);
            conn.createStatement().execute(ddl);
            assertTTL(conn, tableName, TTLExpression.TTL_EXPRESSION_NOT_DEFINED);
            assertTTL(conn, parentView, TTLExpression.TTL_EXPRESSION_NOT_DEFINED);
            assertConditonTTL(conn, childView, ttl);
        }
    }

    @Test
    public void testInListTTLExpr() throws Exception {
        String ddlTemplate = "create table %s (id varchar not null primary key, " +
                "col1 integer, col2 varchar) TTL = '%s'";
        String tableName = generateUniqueName();
        String ttl = "col2 IN ('expired', 'cancelled')";
        String query;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            query = String.format("select col1 from %s where id IN ('abc', 'fff')", tableName);
            validateScan(conn, tableName, query, ttl, false, 1);
        }
    }

    @Test
    public void testPartialIndex() throws Exception {
        String ddlTemplate = "create table %s (id varchar not null primary key, " +
                "col1 integer, col2 integer, col3 double, col4 varchar) TTL = '%s'";
        String tableName = generateUniqueName();
        String indexTemplate = "create index %s on %s (col1) " +
                "include (col2, col3, col4) where col1 > 50";
        String indexName = generateUniqueName();
        String ttl = "col2 > 100 AND col4='expired'";
        String query;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            ddl = String.format(indexTemplate, indexName, tableName);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, indexName, ttl);
            query = String.format("select col3 from %s where col1 > 60", tableName);
            validateScan(conn, tableName, query, ttl, false, 2);
        }
    }

    @Ignore
    public void testTableSelectionWithMultipleIndexes() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(
                    "CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                            "IMMUTABLE_ROWS=true,TTL='v2=''EXPIRED'''");
            conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
            PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = stmt.optimizeQuery("SELECT v1 FROM t WHERE v1 = 'bar'");
            // T is chosen because TTL expression is on v2 which is not present in index
            assertEquals("T", plan.getTableRef().getTable().getTableName().getString());
            Scan scan = plan.getContext().getScan();
            Filter filter = scan.getFilter();
            assertEquals(filter.toString(), "(V1 = 'bar' AND NOT (V2 = 'EXPIRED'))");
            conn.createStatement().execute("CREATE INDEX idx2 ON t(v1,v2)");
            plan = stmt.optimizeQuery("SELECT v1 FROM t WHERE v1 = 'bar'");
            // Now IDX2 should be chosen
            assertEquals("IDX2", plan.getTableRef().getTable().getTableName().getString());
            scan = plan.getContext().getScan();
            filter = scan.getFilter();
            assertEquals(filter.toString(), "NOT (\"V2\" = 'EXPIRED')");
        }
    }
}
