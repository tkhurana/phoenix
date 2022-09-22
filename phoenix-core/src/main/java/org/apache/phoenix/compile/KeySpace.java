package org.apache.phoenix.compile;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Immutable object
public class KeySpace {
    // Intervals for each column that is part of PK
    // columns are in the same order as PK definition
    private DimensionInterval[] dimensions;

    public KeySpace(int numPKCols) {
        dimensions = new DimensionInterval[numPKCols];
    }

    public KeySpace(int numPKCols, Map<Integer, DimensionInterval> colSpace) {
        this(numPKCols);
        for (int i = 0; i < this.getNumDimensions(); ++i) {
            this.dimensions[i] = colSpace.getOrDefault(i, DimensionInterval.UNIVERSAL);
        }
    }

    public static KeySpace getUniversalKeySpace(int numPKCols) {
        KeySpace ks = new KeySpace(numPKCols);
        for (int i = 0; i < ks.getNumDimensions(); ++i) {
            ks.dimensions[i] = DimensionInterval.UNIVERSAL;
        }
        return ks;
    }

    public int getNumDimensions() {
        return dimensions.length;
    }

    public static KeySpace and(KeySpace lhs, KeySpace rhs) {
        Preconditions.checkArgument(lhs.getNumDimensions() == rhs.getNumDimensions());
        KeySpace result = new KeySpace(lhs.getNumDimensions());
        for (int i = 0; i < lhs.getNumDimensions(); ++i) {
            result.dimensions[i] = DimensionInterval.and(lhs.dimensions[i], rhs.dimensions[i]);
        }
        return result;
    }

    public static List<KeySpace> or (KeySpace lhs, KeySpace rhs) {
        Preconditions.checkArgument(lhs.getNumDimensions() == rhs.getNumDimensions());
        // one list of intervals for each dimension
        List<List<DimensionInterval>> intervals = Lists.newArrayList();
        for (int i = 0; i < lhs.getNumDimensions(); ++i) {
            intervals.add(DimensionInterval.or(lhs.dimensions[i], rhs.dimensions[i]));
        }
        return generateKeySpaceFromIntervals(intervals);
    }

    private static List<KeySpace> generateKeySpaceFromIntervals(List<List<DimensionInterval>> intervals) {
        List<KeySpace> result = Lists.newArrayList();
        KeySpace ks = getUniversalKeySpace(intervals.size());
        generateKeySpaceFromIntervals(intervals, 0, ks, result);
        return result;
    }

    private static void generateKeySpaceFromIntervals(List<List<DimensionInterval>> intervals, int index, KeySpace ks, List<KeySpace> result) {
        if (index == intervals.size()) {
            // all dimensions evaluated
            result.add(ks);
        }
        List<DimensionInterval> dimensionIntervals = intervals.get(index);
        for (DimensionInterval interval : dimensionIntervals) {
            ks.dimensions[index] = interval;
            generateKeySpaceFromIntervals(intervals, index + 1, ks, result);
        }
    }

    public static List<KeySpace> and (List<KeySpace> lhsList, List<KeySpace> rhsList) {
        List<KeySpace> result = new ArrayList<>();
        for (KeySpace lhs : lhsList) {
            for (KeySpace rhs : rhsList) {
                result.add(KeySpace.and(lhs, rhs));
            }
        }
        return result;
    }

    public static List<KeySpace> or (List<KeySpace> lhsList, List<KeySpace> rhsList) {
        List<KeySpace> result = new ArrayList<>();
        for (KeySpace lhs : lhsList) {
            for (KeySpace rhs : rhsList) {
                result.addAll(KeySpace.or(lhs, rhs));
            }
        }
        return result;
    }
}

