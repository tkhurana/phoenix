package org.apache.phoenix.compile;

import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;

import java.util.ArrayList;
import java.util.List;

public class KeySpaces {

    public static class KeySpace {
        // Intervals for each column part of primary key
        // columns are in the same order as PK definition
        private KeyRange[] dimensions;

        private KeySpace(int numPKCols) {
            dimensions = new KeyRange[numPKCols];
            for (int i = 0; i < dimensions.length; ++i) {
                dimensions[i] = KeyRange.EVERYTHING_RANGE;
            }
        }

        public void updateDimension(int pkColPos, KeyRange keyRange) {
            dimensions[pkColPos] = keyRange;
        }

        public void and(KeySpace dest) {
        }
    }

    private List<KeySpace> keyspaces;
    private List<PColumn> pkCols;

    public KeySpaces(PTable table) {
        keyspaces = new ArrayList<>();
        pkCols = table.getPKColumns();
    }

    KeySpace addKeySpace() {
        keyspaces.add(new KeySpace(pkCols.size()));
        return keyspaces.get(keyspaces.size() - 1);
    }

    public void and(KeySpaces ks) {
        for (KeySpace src : this.keyspaces) {
            for (KeySpace dest : ks.keyspaces) {
                src.and(dest);
            }
        }
    }
}

