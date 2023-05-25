package org.apache.phoenix.compile;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PColumn;

import java.util.EnumSet;

public class Dimension {
    public static class Boundary {
        enum Flags {
            UNBOUND, // -inf, +inf
            EXCLUSIVE // open interval (,)
        }
        private byte[] value;
        private EnumSet<Flags> flags;

        public Boundary () {
            flags = EnumSet.of(Flags.UNBOUND, Flags.EXCLUSIVE);
        }

        public Boundary (byte[] value, boolean inclusive) {
            this.value = value;
            this.flags = EnumSet.noneOf(Flags.class);
            if (!inclusive) {
                flags.add(Flags.EXCLUSIVE);
            }
        }

        public boolean isUnbound() {
            return flags.contains(Flags.UNBOUND);
        }

        public boolean isExclusive() {
            return flags.contains(Flags.EXCLUSIVE);
        }

        public void bound(byte[] value) {
            this.value = value;
            flags.remove(Flags.UNBOUND);
        }
    }
    PColumn pkColumn;
    Boundary lower, upper;

    public Dimension(PColumn pkCol) {
        this.pkColumn = pkCol;
        this.lower = new Boundary();
        this.upper = new Boundary();
    }

    public Dimension(PColumn pkCol, Boundary lower, Boundary upper) {
        this.pkColumn = pkCol;
        this.lower = lower;
        this.upper = upper;
    }

    public static Dimension create(PColumn pkCol, byte[] key, CompareOperator op) {
        Boundary lower, upper;
        switch (op) {
            case EQUAL:
                lower = new Boundary(key, true);
                upper = new Boundary(key, true);
                break;
            case GREATER:
                lower = new Boundary(key, false);
                upper = new Boundary();
                break;
            case GREATER_OR_EQUAL:
                lower = new Boundary(key, true);
                upper = new Boundary();
                break;
            case LESS:
                lower = new Boundary();
                upper = new Boundary(key , false);
                break;
            case LESS_OR_EQUAL:
                lower = new Boundary();
                upper = new Boundary(key , true);
                break;
            default:
                throw new IllegalArgumentException("Unknown operator " + op);
        }
        return new Dimension(pkCol, lower, upper);
    }

    public boolean isLowerUnbound() {
        return lower.isUnbound();
    }

    public boolean isUpperUnbound() {
        return upper.isUnbound();
    }

    @Override
    public String toString() {
        return (lower.isExclusive() ? "(" : "[") +
                (isLowerUnbound() ? "*" : Bytes.toStringBinary(lower.value)) + " - " +
                (isUpperUnbound() ? "*" : Bytes.toStringBinary(upper.value)) +
                (upper.isExclusive() ? ")" : "]" );
    }
    /**
     * KeyInterval that encompasses all values for a key space
     * Boolean equivalent to 1
     */
    //public static final Dimension UNIVERSAL = new Dimension(UNBOUND, false, UNBOUND, false);

    /**
     * KeyInterval that represents an empty interval
     * Boolean equivalent to 0
     */
    //public static final Dimension EMPTY = new Dimension(DEGENERATE_KEY, false, DEGENERATE_KEY, false);

    /**
     * @return -1 if lower < upper
     *          1 if lower > upper or  lower = upper and lower inclusive != upper inclusive
     *          0 lower = upper and lower inclusive = upper inclusive
     */
    /*public static int compareLowerToUpper(byte[] lower, boolean lowerInclusive, byte[] upper, boolean upperInclusive) {
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
    }*/

    /*public static boolean isValid(byte[] lower, boolean lowerInclusive, byte[] upper, boolean upperInclusive) {
        return compareLowerToUpper(lower, lowerInclusive, upper, upperInclusive) <= 0;
    }*/

    /*public Dimension(byte[] lower, boolean lowerInclusive, byte[] upper, boolean upperInclusive) {
        Preconditions.checkArgument(isValid(lower, lowerInclusive, upper, upperInclusive));
        this.lower = lower;
        this.lowerInclusive = lowerInclusive;
        this.upper = upper;
        this.upperInclusive = upperInclusive;
    }*/

    /*public static Dimension getKeyInterval(byte[] key, CompareOp op) {
        switch (op) {
            case EQUAL:
                return new Dimension(key, true, key, true);
            case GREATER:
                return new Dimension(key, false, UNBOUND, false);
            case GREATER_OR_EQUAL:
                return new Dimension(key, true, UNBOUND, false);
            case LESS:
                return new Dimension(UNBOUND, false, key, false);
            case LESS_OR_EQUAL:
                return new Dimension(UNBOUND, false, key, true);
            default:
                throw new IllegalArgumentException("Unknown operator " + op);
        }
    }*/

    /*public static Dimension and(Dimension lhs, Dimension rhs) {
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

        Dimension result = new Dimension(newLower, newLowerInclusive, newUpper, newUpperInclusive);
        return result.isValid() ? result : EMPTY;
    }*/

    /*public static List<Dimension> or (Dimension lhs, Dimension rhs) {
        // Both lhs and rhs are valid by definition
        List<Dimension> result = Lists.newArrayList();

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
    }*/

    // Assumes the intervals overlap
    /*private static Dimension mergeOverlapping(Dimension lhs, Dimension rhs) {
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

        return new Dimension(newLower, newLowerInclusive, newUpper, newUpperInclusive);
    } */

    /*public boolean isValid() {
        return isValid(lower, lowerInclusive, upper, upperInclusive);
    }

    public boolean isUniversal() {
        return isLowerUnbound() && isUpperUnbound();
    }

    public boolean isEmpty() {
        return isLowerDegenerate() && isUpperDegenerate();
    }*/
}
