package org.apache.phoenix.compile;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.KeySpaces.KeySpace;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.visitor.StatelessTraverseNoExpressionVisitor;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterators;
import org.apache.phoenix.util.ByteUtil;

import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

public class KeySpaceExpressionVisitor extends StatelessTraverseNoExpressionVisitor<KeySpaces> {

    private final PTable table;
    private final StatementContext context;
    private BitSet pkColsSeen;

    public KeySpaceExpressionVisitor(StatementContext context, PTable table) {
        this.context = context;
        this.table = table;
        this.pkColsSeen = new BitSet(table.getPKColumns().size());
    }

    @Override
    public KeySpaces visit(RowKeyColumnExpression node) {
        this.pkColsSeen.set(node.getPosition());
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(ComparisonExpression node) {
        this.pkColsSeen.clear();
        return Iterators.singletonIterator(node.getChildren().get(0));
    }

    @Override
    public KeySpaces visitLeave(ComparisonExpression node, List<KeySpaces> childKeySpace) {
        Expression rhs = node.getChildren().get(1);
        KeySpaces keyspaces = new KeySpaces(table);
        KeySpace ks = keyspaces.addKeySpace();
        for (int i = pkColsSeen.nextSetBit(0); i >= 0; i = pkColsSeen.nextSetBit(i+1)) {
            PColumn pkCol = table.getPKColumns().get(i);
            KeyRange range = getKeyRange(pkCol, node.getFilterOp(), rhs);
            ks.updateDimension(i, range);
        }
        this.pkColsSeen.clear();
        return keyspaces;
    }

    private KeyRange getKeyRange(PColumn pkCol, CompareOp op, Expression rhs) {
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
        return ByteUtil.getKeyRange(key, op, type);
    }

    @Override
    public Iterator<Expression> visitEnter(AndExpression node) {
        return node.getChildren().iterator();
    }

    @Override
    public KeySpaces visitLeave(AndExpression node, List<KeySpaces> childKeySpaces) {
        KeySpaces mergedKeySpaces = new KeySpaces(table);
        for (KeySpaces keyspaces : childKeySpaces) {
            mergedKeySpaces.and(keyspaces);
        }
        return mergedKeySpaces;
    }

}
