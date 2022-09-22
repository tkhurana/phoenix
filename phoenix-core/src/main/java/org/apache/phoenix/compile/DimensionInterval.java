package org.apache.phoenix.compile;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class DimensionInterval {
    private byte[] lower;
    private boolean lowerInclusive;
    private byte[] upper;
    private boolean upperInclusive;

    private static final byte[] UNBOUND = new byte[0];
    private static final byte[] DEGENERATE_KEY = new byte[] {1};

    /**
     * KeyInterval that encompasses all values for a key space
     * Boolean equivalent to 1
     */
    public static final DimensionInterval UNIVERSAL = new DimensionInterval(UNBOUND, false, UNBOUND, false);

    /**
     * KeyInterval that represents an empty interval
     * Boolean equivalent to 0
     */
    public static final DimensionInterval EMPTY = new DimensionInterval(DEGENERATE_KEY, false, DEGENERATE_KEY, false);

    /**
     * @return -1 if lower < upper
     *          1 if lower > upper or  lower = upper and lower inclusive != upper inclusive
     *          0 lower = upper and lower inclusive = upper inclusive
     */
    public static int compareLowerToUpper(byte[] lower, boolean lowerInclusive, byte[] upper, boolean upperInclusive) {
        if (isUnbound(lower) || isUnbound(upper)) {
            return -1;
        }
        int cmp = Bytes.compareTo(lower, upper);
        if (cmp < 0) {
            // lower < upper
            return -1;
        } else if (cmp > 0) {
            // lower > upper
            return 1;
        } else {
            // lower == upper
            return lowerInclusive == upperInclusive ? 0 : 1;
        }
    }

    public static boolean isValid(byte[] lower, boolean lowerInclusive, byte[] upper, boolean upperInclusive) {
        return compareLowerToUpper(lower, lowerInclusive, upper, upperInclusive) <= 0;
    }

    public DimensionInterval(byte[] lower, boolean lowerInclusive, byte[] upper, boolean upperInclusive) {
        Preconditions.checkArgument(isValid(lower, lowerInclusive, upper, upperInclusive));
        this.lower = lower;
        this.lowerInclusive = lowerInclusive;
        this.upper = upper;
        this.upperInclusive = upperInclusive;
    }

    public static DimensionInterval getKeyInterval(byte[] key, CompareOp op) {
        switch (op) {
            case EQUAL:
                return new DimensionInterval(key, true, key, true);
            case GREATER:
                return new DimensionInterval(key, false, UNBOUND, false);
            case GREATER_OR_EQUAL:
                return new DimensionInterval(key, true, UNBOUND, false);
            case LESS:
                return new DimensionInterval(UNBOUND, false, key, false);
            case LESS_OR_EQUAL:
                return new DimensionInterval(UNBOUND, false, key, true);
            default:
                throw new IllegalArgumentException("Unknown operator " + op);
        }
    }

    public static DimensionInterval and(DimensionInterval lhs, DimensionInterval rhs) {
        byte[] newLower;
        byte[] newUpper;
        boolean newLowerInclusive;
        boolean newUpperInclusive;

        if (lhs.isLowerDegenerate() || rhs.isLowerDegenerate()) {
            // AND with 0 = 0
            newLower = DEGENERATE_KEY;
            newLowerInclusive = false;
        } else if (lhs.isLowerUnbound()) {
            newLower = rhs.lower;
            newLowerInclusive = rhs.lowerInclusive;
        } else if (rhs.isLowerUnbound()) {
            newLower = lhs.lower;
            newLowerInclusive = lhs.lowerInclusive;
        } else {
            int cmp = Bytes.compareTo(lhs.lower, rhs.lower);
            if (cmp < 0) {
                newLower = rhs.lower;
                newLowerInclusive = rhs.lowerInclusive;
            } else if (cmp == 0) {
                newLower = lhs.lower;
                newLowerInclusive = (lhs.lowerInclusive == rhs.lowerInclusive);
            } else {
                newLower = lhs.lower;
                newLowerInclusive = lhs.lowerInclusive;
            }
        }

        if (lhs.isUpperDegenerate() || rhs.isUpperDegenerate()) {
            // AND with 0 = 0
            newUpper = DEGENERATE_KEY;
            newUpperInclusive = false;
        } else if (lhs.isUpperUnbound()) {
            newUpper = rhs.upper;
            newUpperInclusive = rhs.upperInclusive;
        } else if (rhs.isUpperUnbound()) {
            newUpper = lhs.upper;
            newUpperInclusive = lhs.upperInclusive;
        } else {
            int cmp = Bytes.compareTo(lhs.upper, rhs.upper);
            if (cmp < 0) {
                newUpper = lhs.upper;
                newUpperInclusive = lhs.upperInclusive;
            } else if (cmp == 0) {
                newUpper = lhs.upper;
                newUpperInclusive = (lhs.upperInclusive == rhs.upperInclusive);
            } else {
                newUpper = rhs.upper;
                newUpperInclusive = rhs.upperInclusive;
            }
        }

        DimensionInterval result = new DimensionInterval(newLower, newLowerInclusive, newUpper, newUpperInclusive);
        return result.isValid() ? result : EMPTY;
    }

    public static List<DimensionInterval> or (DimensionInterval lhs, DimensionInterval rhs) {
        // Both lhs and rhs are valid by definition
        List<DimensionInterval> result = Lists.newArrayList();

        int cmp1 = compareLowerToUpper(lhs.lower, lhs.lowerInclusive, rhs.upper, rhs.upperInclusive);
        int cmp2 = compareLowerToUpper(rhs.lower, rhs.lowerInclusive, lhs.upper, lhs.upperInclusive);

        // 2 intervals overlap if lhs.lower <= rhs.upper and rhs.lower <= lhs.upper
        if (cmp1 <= 0 && cmp2 <= 0) {
            // Intervals overlap, merge the intervals
            result.add(mergeOverlapping(lhs, rhs));
        } else {
            // no overlap, maybe sort ??
            result.add(lhs);
            result.add(rhs);
        }

        return result;
    }

    // Assumes the intervals overlap
    private static DimensionInterval mergeOverlapping(DimensionInterval lhs, DimensionInterval rhs) {
        byte[] newLower;
        byte[] newUpper;
        boolean newLowerInclusive;
        boolean newUpperInclusive;

        if (lhs.isLowerUnbound() || rhs.isLowerUnbound()) {
            // OR with 1 = 1
            newLower = UNBOUND;
            newLowerInclusive = false;
        } else if (lhs.isLowerDegenerate()) {
            newLower = rhs.lower;
            newLowerInclusive = rhs.lowerInclusive;
        } else if (rhs.isLowerDegenerate()) {
            newLower = lhs.lower;
            newLowerInclusive = lhs.lowerInclusive;
        } else {
            // newLower = min(lhs.lower, rhs.lower)
            int cmp = Bytes.compareTo(lhs.lower, rhs.lower);
            if (cmp < 0) {
                newLower = lhs.lower;
                newLowerInclusive = lhs.lowerInclusive;
            } else if (cmp == 0) {
                newLower = lhs.lower;
                newLowerInclusive = lhs.lowerInclusive || rhs.lowerInclusive;
            } else {
                newLower = rhs.lower;
                newLowerInclusive = rhs.lowerInclusive;
            }
        }

        if (lhs.isUpperUnbound() || rhs.isUpperUnbound()) {
            // OR with 1 = 1
            newUpper = UNBOUND;
            newUpperInclusive = false;
        } else if (lhs.isUpperDegenerate()) {
            newUpper = rhs.upper;
            newUpperInclusive = rhs.upperInclusive;
        } else if (rhs.isUpperDegenerate()) {
            newUpper = rhs.upper;
            newUpperInclusive = rhs.upperInclusive;
        } else {
            // newUpper = max(lhs.upper, rhs.upper)
            int cmp = Bytes.compareTo(lhs.upper, rhs.upper);
            if (cmp < 0) {
                newUpper = rhs.upper;
                newUpperInclusive = rhs.upperInclusive;
            } else if (cmp == 0) {
                newUpper= rhs.upper;
                newUpperInclusive = lhs.upperInclusive || rhs.upperInclusive;
            } else {
                newUpper = lhs.upper;
                newUpperInclusive= lhs.upperInclusive;
            }
        }

        return new DimensionInterval(newLower, newLowerInclusive, newUpper, newUpperInclusive);
    }

    public boolean isValid() {
        return isValid(lower, lowerInclusive, upper, upperInclusive);
    }

    public boolean isUniversal() {
        return isLowerUnbound() && isUpperUnbound();
    }

    public boolean isEmpty() {
        return isLowerDegenerate() && isUpperDegenerate();
    }

    @Override
    public String toString() {
        return (lowerInclusive ? "[" :
            "(") + (isLowerUnbound() ? "*" :
            Bytes.toStringBinary(lower)) + " - " + (isUpperUnbound() ? "*" :
            Bytes.toStringBinary(upper)) + (upperInclusive ? "]" : ")" );
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DimensionInterval)) {
            return false;
        }
        DimensionInterval rhs = (DimensionInterval) o;
        return Bytes.compareTo(this.lower, rhs.lower) == 0 && this.lowerInclusive == rhs.lowerInclusive &&
            Bytes.compareTo(this.upper, rhs.upper) == 0 && this.upperInclusive == rhs.upperInclusive;
    }

    public static boolean isUnbound(byte[] value) {
        return value == UNBOUND;
    }

    public static boolean isDegenerate(byte[] value) {
        return value == DEGENERATE_KEY;
    }

    private boolean isLowerUnbound() {
        return isUnbound(lower);
    }

    private boolean isUpperUnbound() {
        return isUnbound(upper);
    }

    private boolean isLowerDegenerate() {
        return isDegenerate(lower);
    }

    private boolean isUpperDegenerate() {
        return isDegenerate(upper);
    }
}
