package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

@Category(ParallelStatsDisabledTest.class)
public class DemoIT extends ParallelStatsDisabledIT {

    @Test
    public void testDemo1() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = String.format(
            "CREATE TABLE %s (ID VARCHAR NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER)", dataTableFullName);

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
            String dml = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            final int NROWS = 5;
            try (PreparedStatement dataPreparedStatement = conn.prepareStatement(dml)) {
                for (int i = 1; i <= NROWS; i++) {
                    dataPreparedStatement.setString(1, "ROW_" + i);
                    dataPreparedStatement.setInt(2, i);
                    dataPreparedStatement.setInt(3, i * 2);
                    dataPreparedStatement.execute();
                }
            }
            conn.commit();
            TestUtil.dumpTable(conn, TableName.valueOf(dataTableFullName));

            String dql = String.format("SELECT COUNT(*) from %s WHERE VAL1 > 3", dataTableFullName);
            try (ResultSet rs = stmt.executeQuery(dql)) {
                Assert.assertTrue(rs.next());
                int actualRows = rs.getInt(1);
                int expectedRows = 2;
                Assert.assertEquals(expectedRows, actualRows);
                Assert.assertFalse(rs.next());
            }

            dql = String.format("SELECT * from %s WHERE VAL1 > 3", dataTableFullName);
            try (ResultSet rs = stmt.executeQuery(dql)) {
                while (rs.next()) {
                    System.out.println(String.format("%s %d %d",
                        rs.getString(1), rs.getInt(2), rs.getInt(3)));
                }
            }
        }
    }
}
