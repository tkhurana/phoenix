package org.apache.phoenix.benchmarks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.phoenix.util.PhoenixRuntime;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class InClauseParsing {

    public void parseInQuery(Connection conn, String query) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(query)) {
        }
    }

    @Benchmark
    public void testParsing(BenchmarkState state) throws SQLException {
        parseInQuery(state.conn, state.query);
    }

    @State(Scope.Thread)
    public static class BenchmarkState {
        Connection conn;
        String hashKeys;
        String tableName;
        String query;

        @Setup
        public void prepare() throws SQLException {
            String url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "localhost";
            conn = DriverManager.getConnection(url);
            tableName = "N0001";
            String ddl = "CREATE TABLE " + tableName + " (" +
                    " ORGANIZATION_ID CHAR(15) NOT NULL,\n" +
                    " DECISION_TABLE CHAR(15) NOT NULL,\n" +
                    " LAST_REFRESH_DATE BIGINT NOT NULL,\n" +
                    " HASH_KEY CHAR(32) NOT NULL,\n" +
                    " NON_HASHED_INPUT_FIELD_VALUE1 VARCHAR,\n" +
                    " NON_HASHED_OUTPUT_FIELD_VALUE1 VARCHAR,\n" +
                    " CONSTRAINT PK PRIMARY KEY (\n" +
                    "  ORGANIZATION_ID,\n" +
                    "  DECISION_TABLE,\n" +
                    "  LAST_REFRESH_DATE,\n" +
                    "  HASH_KEY\n" +
                    " )\n" +
                    ") VERSIONS=1, MULTI_TENANT=true, REPLICATION_SCOPE=0, DISABLE_BACKUP=true, SALT_BUCKETS=20, UPDATE_CACHE_FREQUENCY=172800000";
            conn.createStatement().execute(ddl);
            initializeQuery1();
        }

        private void initializeQuery1() {
            int keyCount = 4000;
            StringBuilder sb = new StringBuilder();
            String[] keys = new String[keyCount];
            for (int i = 0; i < keyCount; i++) {
                keys[i] = UUID.randomUUID().toString().substring(0, 32);
                sb.append("?,");
            }
            sb.deleteCharAt(sb.length() - 1);
            hashKeys = Arrays.stream(keys).collect(Collectors.joining("','", "('", "')"));
            query = String.format("" +
                    "SELECT NON_HASHED_INPUT_FIELD_VALUE1,NON_HASHED_OUTPUT_FIELD_VALUE1, HASH_KEY, DECISION_TABLE, LAST_REFRESH_DATE FROM "
                    + tableName + "  WHERE ORGANIZATION_ID=? AND " +
                    "DECISION_TABLE=? AND LAST_REFRESH_DATE=? AND HASH_KEY IN (%s)", sb);
        }

        @TearDown
        public void teardown() throws SQLException {
            String ddl = "DROP TABLE " + tableName;
            conn.createStatement().execute(ddl);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(InClauseParsing.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
