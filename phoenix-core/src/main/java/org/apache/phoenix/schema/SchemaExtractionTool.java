package org.apache.phoenix.schema;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static org.apache.hadoop.hbase.HColumnDescriptor.BLOOMFILTER;
import static org.apache.hadoop.hbase.HColumnDescriptor.COMPRESSION;
import static org.apache.hadoop.hbase.HColumnDescriptor.DATA_BLOCK_ENCODING;
import static org.apache.hadoop.hbase.HTableDescriptor.IS_META;
import static org.apache.phoenix.util.SchemaUtil.DEFAULT_DATA_BLOCK_ENCODING;

public class SchemaExtractionTool extends Configured implements Tool {

    private static final Logger LOGGER = Logger.getLogger(SchemaExtractionTool.class.getName());
    private static final Option HELP_OPTION = new Option("h", "help",
            false, "Help");
    private static final Option TABLE_OPTION = new Option("tb", "table", true,
            "[Required] Table name ex. table1");
    private static final Option SCHEMA_OPTION = new Option("s", "schema", true,
            "[Optional] Schema name ex. schema");
    private String pTableName;
    private String pSchemaName;
    private static final String CREATE_TABLE = "CREATE TABLE ";
    Configuration conf;
    Map<String, String> defaultProps = new HashMap<>();
    Map<String, String> definedProps = new HashMap<>();
    public String output;

    @Override
    public int run(String[] args) throws Exception {
        populateToolAttributes(args);
        PTable table = getPTable();
        ConnectionQueryServicesImpl cqsi = (ConnectionQueryServicesImpl) getCQSIObject();

        populateDefaultProperties(table, cqsi);

        String columnInfoString = getColumnInfoString(table, cqsi);

        setPTableProperties(table);
        HTableDescriptor htd = getHTableDescriptor(cqsi, table);

        setHTableProperties(table, htd);

        HColumnDescriptor hcd = htd.getFamily(SchemaUtil.getEmptyColumnFamily(table));
        setHColumnFamilyProperties(table, hcd);

        String propertiesString = convertPropertiesToString();

        output = generateOutputString(columnInfoString, propertiesString);
        return 0;
    }

    private String generateOutputString(String columnInfoString, String propertiesString) {
        StringBuilder outputBuilder = new StringBuilder(CREATE_TABLE);
        outputBuilder.append(columnInfoString).append(propertiesString);
        return outputBuilder.toString();
    }

    private void populateDefaultProperties(PTable table, ConnectionQueryServicesImpl cqsi)
            throws SQLException, IOException {
        HTableDescriptor htd = cqsi.getAdmin().getTableDescriptor(
                TableName.valueOf(table.getPhysicalName().getString()));
        HColumnDescriptor columnDescriptor = htd.getFamily(SchemaUtil.getEmptyColumnFamily(table));
        Map<String, String> propsMap = columnDescriptor.getDefaultValues();
        for (String key : propsMap.keySet()) {
            if(key.equalsIgnoreCase(BLOOMFILTER) || key.equalsIgnoreCase(COMPRESSION)) {
                defaultProps.put(key, "NONE");
                continue;
            }
            if(key.equalsIgnoreCase(DATA_BLOCK_ENCODING)) {
                defaultProps.put(key, String.valueOf(DEFAULT_DATA_BLOCK_ENCODING));
                continue;
            }
            defaultProps.put(key, propsMap.get(key));
        }
        defaultProps.putAll(table.getDefaultValues());
    }

    private void setHTableProperties(PTable table, HTableDescriptor htd) {
        Map<ImmutableBytesWritable, ImmutableBytesWritable> propsMap = htd.getValues();
        for (ImmutableBytesWritable entry : propsMap.keySet()) {
            if(Bytes.toString(entry.get()).contains("coprocessor") || Bytes.toString(entry.get()).contains(IS_META)) {
                continue;
            }
            defaultProps.put(Bytes.toString(entry.get()), "false");
            definedProps.put(Bytes.toString(entry.get()), Bytes.toString(propsMap.get(entry).get()));
        }
    }

    private void setHColumnFamilyProperties(PTable table, HColumnDescriptor columnDescriptor) {
        Map<ImmutableBytesWritable, ImmutableBytesWritable> propsMap = columnDescriptor.getValues();
        for (ImmutableBytesWritable entry : propsMap.keySet()) {
            definedProps.put(Bytes.toString(entry.get()), Bytes.toString(propsMap.get(entry).get()));
        }
    }

    private void setPTableProperties(PTable table) {
        //MULTI_TENANT/IMMUTABLE_ROWS/COLUMN_ENCODED_BYTES/TRANSACTION_PROVIDER/SALT_BUCKETS/DATA_BLOCK_ENCODING/DISABLE_WAL
        //UPDATE_CACHE_FREQUENCY/AUTO_PARTITION_SEQ/GUIDE_POSTS_WIDTH/IMMUTABLE_STORAGE_SCHEME/USE_STATS_FOR_PARALLELIZATION
        Map <String, String> map = table.getValues();
        for(String key : map.keySet()) {
            if(map.get(key) != null) {
                definedProps.put(key, map.get(key));
            }
        }
    }

    private HTableDescriptor getHTableDescriptor(ConnectionQueryServicesImpl cqsi, PTable table)
            throws SQLException, IOException {
        return cqsi.getAdmin().getTableDescriptor(
                TableName.valueOf(table.getPhysicalName().getString()));
    }

    private String convertPropertiesToString() {
        StringBuilder optionBuilder = new StringBuilder();

        for(String key : definedProps.keySet()) {

            if(definedProps.get(key)!=null && defaultProps.get(key) != null && !definedProps.get(key).equals(defaultProps.get(key))) {
                if (!(optionBuilder.length() == 0)) {
                    optionBuilder.append(",");
                }
                optionBuilder.append(key+"="+definedProps.get(key));
            }
        }
        return optionBuilder.toString();
    }

    private PTable getPTable() throws SQLException {
        conf = HBaseConfiguration.addHbaseResources(getConf());

        try (Connection conn = getConnection(conf)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(pSchemaName, pTableName);
            return PhoenixRuntime.getTable(conn, pTableFullName);
        }
    }

    private ConnectionQueryServices getCQSIObject() throws SQLException {
        try(Connection conn = getConnection(conf)) {
            return conn.unwrap(PhoenixConnection.class).getQueryServices();
        }
    }

    public static Connection getConnection(Configuration conf) throws SQLException {
        // in case we want to query sys cat
        setRpcRetriesAndTimeouts(conf);
        return ConnectionUtil.getInputConnection(conf);
    }

    private static void setRpcRetriesAndTimeouts(Configuration conf) {
        long indexRebuildQueryTimeoutMs =
                conf.getLong(QueryServices.INDEX_REBUILD_QUERY_TIMEOUT_ATTRIB,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_QUERY_TIMEOUT);
        long indexRebuildRPCTimeoutMs =
                conf.getLong(QueryServices.INDEX_REBUILD_RPC_TIMEOUT_ATTRIB,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_TIMEOUT);
        long indexRebuildClientScannerTimeOutMs =
                conf.getLong(QueryServices.INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT_ATTRIB,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT);
        int indexRebuildRpcRetriesCounter =
                conf.getInt(QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_RETRIES_COUNTER);

        // Set phoenix and hbase level timeouts and rpc retries
        conf.setLong(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, indexRebuildQueryTimeoutMs);
        conf.setLong(HConstants.HBASE_RPC_TIMEOUT_KEY, indexRebuildRPCTimeoutMs);
        conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
                indexRebuildClientScannerTimeOutMs);
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, indexRebuildRpcRetriesCounter);
    }

    private String getColumnInfoString(PTable table, ConnectionQueryServices cqsi) {
        return "( )";
    }

    private void populateToolAttributes(String[] args) {
        try {
            CommandLine cmdLine = parseOptions(args);
            pTableName = cmdLine.getOptionValue(TABLE_OPTION.getOpt());
            pSchemaName = cmdLine.getOptionValue(SCHEMA_OPTION.getOpt());
            LOGGER.info("Schema Extraction Tool initiated: " + StringUtils.join( args, ","));
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }
    }

    private CommandLine parseOptions(String[] args) {
        final Options options = getOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("severe parsing command line options: " + e.getMessage(),
                    options);
        }
        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }
        if (!(cmdLine.hasOption(TABLE_OPTION.getOpt()))) {
            throw new IllegalStateException("Table name should be passed "
                    +TABLE_OPTION.getLongOpt());
        }
        return cmdLine;
    }

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(TABLE_OPTION);
        SCHEMA_OPTION.setOptionalArg(true);
        options.addOption(SCHEMA_OPTION);
        return options;
    }

    private void printHelpAndExit(String severeMessage, Options options) {
        System.err.println(severeMessage);
        printHelpAndExit(options, 1);
    }

    private void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }

    public static void main (String[] args) throws Exception {
        int result = ToolRunner.run(new SchemaExtractionTool(), args);
        System.exit(result);
    }
}
