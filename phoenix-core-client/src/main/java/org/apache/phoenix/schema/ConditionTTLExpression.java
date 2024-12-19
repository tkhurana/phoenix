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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_TTL;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.schema.PTableType.VIEW;
import static org.apache.phoenix.util.SchemaUtil.isPKColumn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexStatementRewriter;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler.WhereExpressionCompiler;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ViewUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConditionTTLExpression extends TTLExpression {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConditionTTLExpression.class);

    private final String ttlExpr;
    private Expression compiledExpr;
    private Set<ColumnReference> conditionExprColumns;

    public ConditionTTLExpression(String ttlExpr) {
        this.ttlExpr = ttlExpr;
    }

    private ConditionTTLExpression(String ttlExpr,
                                   Expression compiledExpression,
                                   Set<ColumnReference> conditionExprColumns) {
        this.ttlExpr = ttlExpr;
        this.compiledExpr = compiledExpression;
        this.conditionExprColumns = conditionExprColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConditionTTLExpression that = (ConditionTTLExpression) o;
        return ttlExpr.equals(that.ttlExpr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ttlExpr);
    }

    @Override
    public String getTTLExpression() {
        return ttlExpr;
    }

    @Override
    public String toString() {
        return getTTLExpression();
    }

    /**
     * The cells of the row (i.e., result) read from HBase store are lexicographically ordered
     * for tables using the key part of the cells which includes row, family, qualifier,
     * timestamp and type. The cells belong of a column are ordered from the latest to
     * the oldest. The method leverages this ordering and groups the cells into their columns
     * based on the pair of family name and column qualifier.
     */
    private List<Cell> getLatestRowVersion(List<Cell> result) {
        List<Cell> latestRowVersion = new ArrayList<>();
        Cell currentColumnCell = null;
        long maxDeleteFamilyTS = 0;
        for (Cell cell : result) {
            if (currentColumnCell == null ||
                    !CellUtil.matchingColumn(cell, currentColumnCell)) {
                // found a new column cell which has the latest timestamp
                currentColumnCell = cell;
                if (currentColumnCell.getType() == Cell.Type.DeleteFamily ||
                        currentColumnCell.getType() == Cell.Type.DeleteFamilyVersion) {
                    // DeleteFamily will be first in the lexicographically ordering because
                    // it has no qualifier
                    maxDeleteFamilyTS = currentColumnCell.getTimestamp();
                    // no need to add the DeleteFamily cell since it can't be part of
                    // an expression
                    continue;
                }
                if (currentColumnCell.getTimestamp() > maxDeleteFamilyTS) {
                    // only add the cell if it is not masked by the DeleteFamily
                    latestRowVersion.add(currentColumnCell);
                }
            }
        }
        return latestRowVersion;
    }

    @Override
    /**
     * @param result row to be evaluated against the conditional ttl expression
     * @return DEFAULT_TTL (FOREVER) if the expression evaluates to False else 0
     * if the expression evaluates to true i.e. row is expired
     */
    public long getTTLForRow(List<Cell> result) {
        long ttl = DEFAULT_TTL;
        if (compiledExpr == null) {
            throw new RuntimeException(
                    String.format("Condition TTL Expression %s not compiled", this.ttlExpr));
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        List<Cell> latestRowVersion = getLatestRowVersion(result);
        if (latestRowVersion.isEmpty()) {
            return ttl;
        }
        MultiKeyValueTuple row = new MultiKeyValueTuple(latestRowVersion);
        if (compiledExpr.evaluate(row, ptr)) {
            Boolean isExpired = (Boolean) PBoolean.INSTANCE.toObject(ptr);
            ttl = isExpired ? 0 : DEFAULT_TTL;
        } else {
            LOGGER.info("Expression evaluation failed for expr {}", ttlExpr);
        }
        return ttl;
    }

    private static byte[] serializeExpression(Expression condTTLExpr) throws IOException {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            DataOutput output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, ExpressionType.valueOf(condTTLExpr).ordinal());
            condTTLExpr.write(output);
            return stream.toByteArray();
        }
    }

    private static Expression deSerializeExpression(byte[] serializedExpr) throws IOException {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(serializedExpr)) {
            DataInput input = new DataInputStream(stream);
            int expressionOrdinal = WritableUtils.readVInt(input);
            Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
            expression.readFields(input);
            return expression;
        }
    }

    public static ConditionTTLExpression createFromProto(PTableProtos.ConditionTTL condition)
            throws IOException {
        String ttlExpr = condition.getTtlExpression();
        Expression compiledExpression = deSerializeExpression(
                condition.getCompiledExpression().toByteArray());
        List<ServerCachingProtos.ColumnReference> exprColumnsList =
                condition.getTtlExpressionColumnsList();
        Set<ColumnReference> conditionExprColumns = new HashSet<>(exprColumnsList.size());
        for (ServerCachingProtos.ColumnReference colRefFromProto : exprColumnsList) {
            conditionExprColumns.add(new ColumnReference(
                    colRefFromProto.getFamily().toByteArray(),
                    colRefFromProto.getQualifier().toByteArray()));
        }
        return new ConditionTTLExpression(ttlExpr, compiledExpression, conditionExprColumns);
    }

    @Override
    public PTableProtos.TTLExpression toProto(PhoenixConnection connection,
                                              PTable table) throws SQLException, IOException {
        Pair<Expression, Set<ColumnReference>> exprAndCols = buildExpression(connection, table);
        PTableProtos.TTLExpression.Builder ttl = PTableProtos.TTLExpression.newBuilder();
        PTableProtos.ConditionTTL.Builder condition = PTableProtos.ConditionTTL.newBuilder();
        condition.setTtlExpression(ttlExpr);
        condition.setCompiledExpression(ByteStringer.wrap(
                serializeExpression(exprAndCols.getFirst())));
        for (ColumnReference colRef : exprAndCols.getSecond()) {
            ServerCachingProtos.ColumnReference.Builder cRefBuilder =
                    ServerCachingProtos.ColumnReference.newBuilder();
            cRefBuilder.setFamily(ByteStringer.wrap(colRef.getFamily()));
            cRefBuilder.setQualifier(ByteStringer.wrap(colRef.getQualifier()));
            condition.addTtlExpressionColumns(cRefBuilder.build());
        }
        ttl.setCondition(condition.build());
        return ttl.build();
    }

    private ParseNode parseExpression(PhoenixConnection connection,
                                      PTable table,
                                      PTable parent) throws SQLException {
        ParseNode ttlCondition = SQLParser.parseCondition(this.ttlExpr);
        return table.getType() != PTableType.INDEX ? ttlCondition
                : rewriteForIndex(connection, table, parent, ttlCondition);
    }

    private ParseNode rewriteForIndex(PhoenixConnection connection,
                                      PTable index,
                                      PTable parent,
                                      ParseNode ttlCondition) throws SQLException {
        if (parent == null) {
            parent = getParent(connection, index);
        }
        ColumnResolver parentResolver = FromCompiler.getResolver(new TableRef(parent));
        return IndexStatementRewriter.translate(ttlCondition, parentResolver);
    }

    private PTable getParent(PhoenixConnection connection,
                             PTable table) throws SQLException {
        for (Map.Entry<PTableKey, Long> entry :
                table.getAncestorLastDDLTimestampMap().entrySet()) {
            return connection.getTable(entry.getKey());
        }
        // TODO: Fix exception
        throw new SQLException("Parent not found");
    }

    @Override
    public void validateTTLOnCreation(PhoenixConnection conn,
                                      CreateTableStatement create,
                                      PTable parent,
                                      Map<String, Object> tableProps) throws SQLException {
        // Construct a PTable with just enough information to be able to compile the TTL expression
        PTable table = createTempPTable(conn, create, parent, tableProps);
        validateTTLExpression(conn, table, parent);
    }

    @Override
    public void validateTTLOnAlter(PhoenixConnection conn,
                                   PTable table) throws SQLException {
        validateTTLExpression(conn, table, null);
        // verify that the expression is covered by all the existing indexes
        for (PTable index : table.getIndexes()) {
            try {
                buildExpression(conn, index, table);
            } catch (ColumnNotFoundException | ColumnFamilyNotFoundException e) {
                throw new SQLException(String.format(
                        "Condition TTL expression %s not covered by index %s", ttlExpr,
                        index.getTableName()), e);
            }
        }
    }

    @Override
    public Expression compileTTLExpression(PhoenixConnection connection,
                                           PTable table) throws IOException {
        try {
            Pair<Expression, Set<ColumnReference>> expr = buildExpression(connection, table);
            compiledExpr = expr.getFirst();
            conditionExprColumns = expr.getSecond();
            return compiledExpr;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private Pair<Expression, Set<ColumnReference>> buildExpression(PhoenixConnection connection,
                                                                   PTable table,
                                                                   PTable parent) throws SQLException {
        ParseNode ttlCondition = parseExpression(connection, table, parent);
        ColumnResolver resolver = FromCompiler.getResolver(new TableRef(table));
        StatementContext context = new StatementContext(
                new PhoenixStatement(connection), resolver);
        WhereExpressionCompiler expressionCompiler = new WhereExpressionCompiler(context);
        Expression expr = ttlCondition.accept(expressionCompiler);
        Set<ColumnReference> exprCols =
                Sets.newHashSetWithExpectedSize(context.getWhereConditionColumns().size());
        for (Pair<byte[], byte[]> column : context.getWhereConditionColumns()) {
            exprCols.add(
                    new ColumnReference(column.getFirst(), column.getSecond()));
        }
        return new Pair<>(expr, exprCols);
    }

    private Pair<Expression, Set<ColumnReference>> buildExpression(PhoenixConnection connection,
                                                                   PTable table) throws SQLException {
        return buildExpression(connection, table, null);
    }

    public synchronized Set<ColumnReference> getColumnsReferenced(
            PhoenixConnection connection,
            PTable table) throws SQLException {
        if (conditionExprColumns == null) {
            conditionExprColumns = buildExpression(connection, table).getSecond();
        }
        return conditionExprColumns;
    }

    /**
     * We are still in the middle of executing the CreateTable statement, so we don't have
     * the PTable yet, but we need one for compiling the conditional TTL expression so let's
     * build the PTable object with just enough information to be able to compile the Conditional
     * TTL expression statement.
     * @param createStmt
     * @param parent
     * @param tableProps
     * @return
     * @throws SQLException
     */
    private PTable createTempPTable(PhoenixConnection conn,
                                    CreateTableStatement createStmt,
                                    PTable parent,
                                    Map<String, Object> tableProps) throws SQLException {
        final TableName tableNameNode = createStmt.getTableName();
        final PName schemaName = PNameFactory.newName(tableNameNode.getSchemaName());
        final PName tableName = PNameFactory.newName(tableNameNode.getTableName());
        PName fullName = SchemaUtil.getTableName(schemaName, tableName);
        final PName tenantId = conn.getTenantId();
        String defaultFamily = (String) TableProperty.DEFAULT_COLUMN_FAMILY.getValue(tableProps);
        List<PColumn> allCols = Lists.newArrayList();
        List<PColumn> pkCols = Lists.newArrayList();
        int pos = 0;
        for (ColumnDef colDef : createStmt.getColumnDefs()) {
            ColumnName columnDefName = colDef.getColumnDefName();
            String columnName = columnDefName.getColumnName();
            PName familyName = null;
            boolean isPK = isPKColumn(createStmt.getPrimaryKeyConstraint(), colDef);
            if (!isPK) {   // PK columns always have null column family
                String family = columnDefName.getFamilyName();
                if (family != null) {
                    familyName = PNameFactory.newName(family);
                } else {
                    familyName = PNameFactory.newName(defaultFamily == null ?
                            QueryConstants.DEFAULT_COLUMN_FAMILY : defaultFamily);
                }
            }
            PColumn pColumn = new PColumnImpl(PNameFactory.newName(columnName), familyName,
                    colDef.getDataType(), colDef.getMaxLength(), colDef.getScale(),
                    colDef.isNull(), pos++, colDef.getSortOrder(), colDef.getArraySize(), null,
                    false, colDef.getExpression(), colDef.isRowTimestamp(), false,
                    Bytes.toBytes(columnName), EnvironmentEdgeManager.currentTimeMillis());
            allCols.add(pColumn);
            if (isPK) {
                pkCols.add(pColumn);
            }
        }

        PTable table = new PTableImpl.Builder()
                .setName(fullName)
                .setKey(new PTableKey(tenantId, fullName.getString()))
                .setTenantId(tenantId)
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setParentSchemaName((parent == null) ? null : parent.getSchemaName())
                .setParentTableName((parent == null) ? null : parent.getTableName())
                .setPhysicalNames(Collections.EMPTY_LIST)
                .setType(createStmt.getTableType())
                .setImmutableStorageScheme(ONE_CELL_PER_COLUMN)
                .setQualifierEncodingScheme(NON_ENCODED_QUALIFIERS)
                .setDefaultFamilyName(PNameFactory.newName(defaultFamily))
                .setColumns(allCols)
                .setPkColumns(pkCols)
                .setIndexes(Collections.EMPTY_LIST)
                .build();

        if (parent != null) {
            // add dervied columns for views
            if (table.getType() == VIEW) {
                table = ViewUtil.addDerivedColumnsFromParent(conn, table, parent);
            }
        }
        return table;
    }

    private void validateTTLExpression(PhoenixConnection conn,
                                       PTable table,
                                       PTable parent) throws SQLException {

        if (table.getColumnFamilies().size() > 1) {
            throw new SQLExceptionInfo.Builder(
                SQLExceptionCode.CANNOT_SET_CONDITION_TTL_ON_TABLE_WITH_MULTIPLE_COLUMN_FAMILIES)
                .build().buildException();
        }
        ParseNode ttlCondition = parseExpression(conn, table, parent);
        ColumnResolver resolver = FromCompiler.getResolver(new TableRef(table));
        StatementContext context = new StatementContext(new PhoenixStatement(conn), resolver);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        try {
            Expression ttlExpression = ttlCondition.accept(expressionCompiler);
            if (expressionCompiler.isAggregate()) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_CONDITION_TTL)
                        .build().buildException();
            }
            if (ttlExpression.getDataType() != PBoolean.INSTANCE) {
                throw TypeMismatchException.newException(PBoolean.INSTANCE,
                        ttlExpression.getDataType(), ttlExpression.toString());
            }
        } catch (ColumnNotFoundException | ColumnFamilyNotFoundException e) {
                throw new SQLException(String.format(
                        "Condition TTL expression %s refers columns not in %s", ttlExpr,
                        table.getTableName()), e);
        }
    }
}
