package org.apache.phoenix.end2end;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.SchemaExtractionTool;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;

import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

public class SchemaExtractionToolIT extends BaseTest {

    @BeforeClass
    public static void setup() throws Exception {
        Map<String, String> props = Collections.emptyMap();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testCreateTableStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_MIGRATION=true,DISABLE_TABLE_SOR=true,DISABLE_WAL=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                    + properties);
            conn.commit();
            String [] args = {"-tb", tableName, "-s", schemaName};

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);
            System.out.println(set.output);
            String actualProperties = set.output.substring(set.output.lastIndexOf(")")+1);
            Assert.assertEquals(5, actualProperties.split(",").length);
            Assert.assertEquals(properties, actualProperties);
        }
    }

    @Test
    public void testCreateIndexStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_MIGRATION=true,DISABLE_TABLE_SOR=true,DISABLE_WAL=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                    + properties);

            String createIndexStatement = "CREATE INDEX "+indexName + " ON "+pTableFullName+"(v1 DESC) INCLUDE (v2)";

            conn.createStatement().execute(createIndexStatement);
            conn.commit();
            String [] args = {"-tb", indexName, "-s", schemaName};

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);
            System.out.println(set.output);
            Assert.assertEquals(createIndexStatement.toUpperCase(), set.output.toUpperCase());

            String [] args2 = {"-tb", tableName, "-s", schemaName, "--show-tree"};
            set.run(args2);
            System.out.println(set.output);
        }
    }

    @Test
    public void testCreateViewStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String viewName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_MIGRATION=true,DISABLE_TABLE_SOR=true,DISABLE_WAL=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k BIGINT NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                    + properties);
            String viewFullName = SchemaUtil.getQualifiedTableName(schemaName, viewName);

            String createView = "CREATE VIEW "+viewFullName + "(id1 BIGINT, id2 BIGINT NOT NULL, id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) AS SELECT * FROM "+pTableFullName;

            conn.createStatement().execute(createView);
            conn.commit();
            String [] args = {"-tb", viewName, "-s", schemaName};

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);
            System.out.println(set.output);
            Assert.assertEquals(createView.toUpperCase(), set.output.toUpperCase());
        }
    }
}
