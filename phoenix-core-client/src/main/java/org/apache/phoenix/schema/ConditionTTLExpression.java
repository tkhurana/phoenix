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
import static org.apache.phoenix.util.SchemaUtil.isPKColumn;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexStatementRewriter;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler.WhereExpressionCompiler;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class ConditionTTLExpression extends TTLExpression {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConditionTTLExpression.class);

    private final String ttlExpr;
    private Expression conditionExpr;

    public ConditionTTLExpression(String ttlExpr) {
        this.ttlExpr = ttlExpr;
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
        if (conditionExpr == null) {
            throw new RuntimeException(
                    String.format("Condition TTL Expression %s not compiled", this.ttlExpr));
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        List<Cell> latestRowVersion = getLatestRowVersion(result);
        if (latestRowVersion.isEmpty()) {
            return ttl;
        }
        MultiKeyValueTuple row = new MultiKeyValueTuple(latestRowVersion);
        if (conditionExpr.evaluate(row, ptr)) {
            Boolean isExpired = (Boolean) PBoolean.INSTANCE.toObject(ptr);
            ttl = isExpired ? 0 : DEFAULT_TTL;
        } else {
            LOGGER.info("Expression evaluation failed for expr {}", ttlExpr);
        }
        return ttl;
    }

    @Override
    public void validateTTLOnCreation(PhoenixConnection conn,
                                      CreateTableStatement create,
                                      Map<String, Object> tableProps) throws SQLException {
        validateFamilyCount(create, tableProps);
        ParseNode ttlCondition = SQLParser.parseCondition(this.ttlExpr);
        StatementContext ttlValidationContext = new StatementContext(new PhoenixStatement(conn));
        // Construct a PTable with just enough information to be able to compile the TTL expression
        PTable newTable = createTempPTable(conn, create);
        ttlValidationContext.setCurrentTable(new TableRef(newTable));
        VerifyCreateConditionalTTLExpression condTTLVisitor =
                new VerifyCreateConditionalTTLExpression(conn, ttlValidationContext, create);
        Expression ttlExpression = ttlCondition.accept(condTTLVisitor);
        validateTTLExpression(ttlExpression, condTTLVisitor);
    }

    @Override
    public void validateTTLOnAlter(PhoenixConnection conn, PTable table) throws SQLException {
        if (table.getColumnFamilies().size() > 1) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.CANNOT_SET_CONDITION_TTL_ON_TABLE_WITH_MULTIPLE_COLUMN_FAMILIES)
                    .build().buildException();
        }
        ParseNode ttlCondition = SQLParser.parseCondition(this.ttlExpr);
        ColumnResolver resolver = FromCompiler.getResolver(new TableRef(table));
        StatementContext context = new StatementContext(new PhoenixStatement(conn), resolver);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        Expression ttlExpression = ttlCondition.accept(expressionCompiler);
        validateTTLExpression(ttlExpression, expressionCompiler);
    }

    @Override
    public String getTTLForScanAttribute() {
        // Conditional TTL is not sent as a scan attribute
        // Masking is implemented using query re-write
        return null;
    }

    public ParseNode parseExpression(PhoenixConnection connection,
                                     PTable table) throws SQLException {
        ParseNode ttlCondition = SQLParser.parseCondition(this.ttlExpr);
        return table.getType() != PTableType.INDEX ? ttlCondition
                : rewriteForIndex(connection, table, ttlCondition);
    }

    private ParseNode rewriteForIndex(PhoenixConnection connection,
                                      PTable index,
                                      ParseNode ttlCondition) throws SQLException {
        for (Map.Entry<PTableKey, Long> entry : index.getAncestorLastDDLTimestampMap().entrySet()) {
            PTableKey parentKey = entry.getKey();
            PTable parent = connection.getTable(parentKey);
            ColumnResolver parentResolver = FromCompiler.getResolver(new TableRef(parent));
            return IndexStatementRewriter.translate(ttlCondition, parentResolver);
        }
        // TODO: Fix exception
        throw new SQLException("Parent not found");
    }

    private Expression getCompiledExpression(PhoenixConnection connection,
                                             PTable table,
                                             ParseNode ttlCondition) throws SQLException {
        ColumnResolver resolver = FromCompiler.getResolver(new TableRef(table));
        StatementContext context = new StatementContext(new PhoenixStatement(connection), resolver);
        WhereExpressionCompiler expressionCompiler = new WhereExpressionCompiler(context);
        return ttlCondition.accept(expressionCompiler);
    }

    @Override
    public Expression compileTTLExpression(PhoenixConnection connection, PTable table) throws IOException {
        try {
            ParseNode ttlCondition = parseExpression(connection, table);
            ColumnResolver resolver = FromCompiler.getResolver(new TableRef(table));
            StatementContext context = new StatementContext(new PhoenixStatement(connection), resolver);
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
            conditionExpr = ttlCondition.accept(expressionCompiler);
            return conditionExpr;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    /**
     * Validates that all the columns used in the conditional TTL expression are present in the table
     * or its parent table in case of view
     */
    private static class VerifyCreateConditionalTTLExpression extends ExpressionCompiler {
        private final CreateTableStatement create;
        private final ColumnResolver baseTableResolver;

        private VerifyCreateConditionalTTLExpression(PhoenixConnection conn,
                                                     StatementContext ttlExprValidationContext,
                                                     CreateTableStatement create) throws SQLException {
            super(ttlExprValidationContext);
            this.create = create;
            // Returns the resolver for base table if base table is not null (in case of views)
            // Else, returns FromCompiler#EMPTY_TABLE_RESOLVER which is a no-op resolver
            this.baseTableResolver = FromCompiler.getResolverForCreation(create, conn);
        }

        @Override
        public Expression visit(ColumnParseNode node) throws SQLException {
            // First check current table
            for (ColumnDef columnDef : create.getColumnDefs()) {
                ColumnName columnName = columnDef.getColumnDefName();
                // Takes family name into account
                if (columnName.toString().equals(node.getFullName())) {
                    String cf = columnName.getFamilyName();
                    String cq = columnName.getColumnName();
                    return new KeyValueColumnExpression( new PDatum() {
                        @Override
                        public boolean isNullable() {
                            return columnDef.isNull();
                        }
                        @Override
                        public PDataType getDataType() {
                            return columnDef.getDataType();
                        }
                        @Override
                        public Integer getMaxLength() {
                            return columnDef.getMaxLength();
                        }
                        @Override
                        public Integer getScale() {
                            return columnDef.getScale();
                        }
                        @Override
                        public SortOrder getSortOrder() {
                            return columnDef.getSortOrder();
                        }
                    }, cf != null ? Bytes.toBytes(cf) : null, Bytes.toBytes(cq));
                }
            }
            // Column used in TTL expression not found in current, check the parent
            ColumnRef columnRef = baseTableResolver.resolveColumn(
                    node.getSchemaName(), node.getTableName(), node.getName());
            return columnRef.newColumnExpression(node.isTableNameCaseSensitive(), node.isCaseSensitive());
        }
    }

    /**
     * We are still in the middle of executing the CreateTable statement, so we don't have
     * the PTable yet, but we need one for compiling the conditional TTL expression so let's
     * build the PTable object with just enough information to be able to compile the Conditional
     * TTL expression statement.
     * @param statement
     * @return
     * @throws SQLException
     */
    private PTable createTempPTable(PhoenixConnection conn, CreateTableStatement statement) throws SQLException {
        final TableName tableNameNode = statement.getTableName();
        final PName schemaName = PNameFactory.newName(tableNameNode.getSchemaName());
        final PName tableName = PNameFactory.newName(tableNameNode.getTableName());
        PName fullName = SchemaUtil.getTableName(schemaName, tableName);
        final PName tenantId = conn.getTenantId();
        return new PTableImpl.Builder()
                .setName(fullName)
                .setKey(new PTableKey(tenantId, fullName.getString()))
                .setTenantId(tenantId)
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setType(statement.getTableType())
                .setImmutableStorageScheme(ONE_CELL_PER_COLUMN)
                .setQualifierEncodingScheme(NON_ENCODED_QUALIFIERS)
                .setFamilies(Collections.EMPTY_LIST)
                .setIndexes(Collections.EMPTY_LIST)
                .build();
    }

    private void validateTTLExpression(Expression ttlExpression,
                                       ExpressionCompiler expressionCompiler) throws SQLException {

        if (expressionCompiler.isAggregate()) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_TTL_EXPRESSION)
                    .build().buildException();
        }

        if (ttlExpression.getDataType() != PBoolean.INSTANCE) {
            throw TypeMismatchException.newException(PBoolean.INSTANCE,
                    ttlExpression.getDataType(), ttlExpression.toString());
        }
    }

    private void validateFamilyCount(CreateTableStatement create,
                                     Map<String, Object> tableProps) throws SQLException {
        String defaultFamilyName = (String)
                TableProperty.DEFAULT_COLUMN_FAMILY.getValue(tableProps);
        defaultFamilyName = (defaultFamilyName == null) ? QueryConstants.DEFAULT_COLUMN_FAMILY
                : defaultFamilyName;
        Set<String> families = Sets.newHashSet();
        for (ColumnDef columnDef : create.getColumnDefs()) {
            if (isPKColumn(create.getPrimaryKeyConstraint(), columnDef)) {
                continue; // Ignore PK columns since they always have null column family
            }
            String familyName = columnDef.getColumnDefName().getFamilyName();
            if (familyName != null) {
                families.add(familyName);
            } else {
                families.add(defaultFamilyName);
            }
        }
        if (families.size() > 1) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.CANNOT_SET_CONDITION_TTL_ON_TABLE_WITH_MULTIPLE_COLUMN_FAMILIES)
                    .build().buildException();
        }
    }
}
