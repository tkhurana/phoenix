package org.apache.phoenix.compile;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.CompareOperator;
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
        // nothing to do here
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(ComparisonExpression node) {
        // nothing to do here
        return null;
        //return Iterators.singletonIterator(node.getChildren().get(0));
    }

    @Override
    public List<KeySpace> visitLeave(ComparisonExpression node, List<List<KeySpace>> childKeySpace) {
        KeySpace ks = new KeySpace(table.getPKColumns());
        Expression lhs = node.getChildren().get(0);
        Expression rhs = node.getChildren().get(1);
        if (lhs instanceof RowKeyColumnExpression) {
            Dimension d = createDimension((RowKeyColumnExpression) lhs, rhs, node.getFilterOp());
            ks.setDimension(((RowKeyColumnExpression) lhs).getPosition(), d);
        } else {
            throw new IllegalArgumentException("Unexpected expr " + lhs);
        }
        return Lists.newArrayList(ks);
    }

    private Dimension createDimension(RowKeyColumnExpression lhs, Expression rhs, CompareOperator op) {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rhs.evaluate(null, ptr);
        int pkPos = lhs.getPosition();
        PColumn pkCol = table.getPKColumns().get(pkPos);
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
        return Dimension.create(pkCol, key, op);
    }

    @Override
    public Iterator<Expression> visitEnter(AndExpression node) {
        return node.getChildren().iterator();
    }

    @Override
    public List<KeySpace> visitLeave(AndExpression node, List<List<KeySpace>> children) {
        List<KeySpace> result = null;
        /*= Lists.newArrayList(KeySpace.getUniversalKeySpace(this.numPKCols));
        for (List<KeySpace> child : children) {
            result = KeySpace.and(result, child);
        }*/
        return result;
    }

    @Override
    public Iterator<Expression> visitEnter(OrExpression node) {
        return node.getChildren().iterator();
    }

    @Override
    public List<KeySpace> visitLeave(OrExpression node, List<List<KeySpace>> children) {
        List<KeySpace> result = null;
        /*Lists.newArrayList(KeySpace.getUniversalKeySpace(this.numPKCols));
        for (List<KeySpace> child : children) {
            result = KeySpace.or(result, child);
        }*/
        return result;
    }

}
