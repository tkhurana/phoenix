package org.apache.phoenix.compile;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.visitor.StatelessTraverseNoExpressionVisitor;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterators;
import org.apache.phoenix.util.ByteUtil;

import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

public class KeySpaceExpressionVisitor extends StatelessTraverseNoExpressionVisitor<List<KeySpace>> {

    private final PTable table;
    private final StatementContext context;
    private final int numPKCols;
    private BitSet pkColsSeen;

    public KeySpaceExpressionVisitor(StatementContext context, PTable table) {
        this.context = context;
        this.table = table;
        this.numPKCols = table.getPKColumns().size();
        this.pkColsSeen = new BitSet(this.numPKCols);
    }

    @Override
    public List<KeySpace> visit(RowKeyColumnExpression node) {
        this.pkColsSeen.set(node.getPosition());
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(ComparisonExpression node) {
        this.pkColsSeen.clear();
        return Iterators.singletonIterator(node.getChildren().get(0));
    }

    @Override
    public List<KeySpace> visitLeave(ComparisonExpression node, List<List<KeySpace>> childKeySpace) {
        Expression rhs = node.getChildren().get(1);
        int idx = pkColsSeen.nextSetBit(0);
        KeySpace ks;
        if (idx != -1) {
            PColumn pkCol = table.getPKColumns().get(idx);
            DimensionInterval interval = getKeyInterval(pkCol, node.getFilterOp(), rhs);
            ks = new KeySpace(this.numPKCols, ImmutableMap.of(idx, interval));
        } else {
            ks = KeySpace.getUniversalKeySpace(this.numPKCols);
        }
        this.pkColsSeen.clear();
        return Lists.newArrayList(ks);
    }

    private DimensionInterval getKeyInterval(PColumn pkCol, CompareOp op, Expression rhs) {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rhs.evaluate(null, ptr);
        // If the column is fixed width, fill is up to it's byte size
        PDataType type = pkCol.getDataType();
        if (type.isFixedWidth()) {
            Integer length = pkCol.getMaxLength();
            if (length != null) {
                // Go through type to pad as the fill character depends on the type.
                type.pad(ptr, length, SortOrder.ASC);
            }
        }
        byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
        return DimensionInterval.getKeyInterval(key, op);
    }

    @Override
    public Iterator<Expression> visitEnter(AndExpression node) {
        return node.getChildren().iterator();
    }

    @Override
    public List<KeySpace> visitLeave(AndExpression node, List<List<KeySpace>> children) {
        List<KeySpace> result = Lists.newArrayList(KeySpace.getUniversalKeySpace(this.numPKCols));
        for (List<KeySpace> child : children) {
            result = KeySpace.and(result, child);
        }
        return result;
    }

    @Override
    public Iterator<Expression> visitEnter(OrExpression node) {
        return node.getChildren().iterator();
    }

    @Override
    public List<KeySpace> visitLeave(OrExpression node, List<List<KeySpace>> children) {
        List<KeySpace> result = Lists.newArrayList(KeySpace.getUniversalKeySpace(this.numPKCols));
        for (List<KeySpace> child : children) {
            result = KeySpace.or(result, child);
        }
        return result;
    }

}
