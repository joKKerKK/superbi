package com.flipkart.fdp.superbi.core.api.query.column;

import com.google.common.base.Preconditions;

public class FactColumnEqualityWrapper {
    protected FactColumn col;
    public String key; // used during dev
    public static FactColumnEqualityWrapper wrap(FactColumn col, boolean isSameFact) {
        FactColumnEqualityWrapper colWrapper = isSameFact ? new SameFactColumnEqualityWrapper() : new DiffFactColumnEqualityWrapper();
        colWrapper.col = col;
        colWrapper.key = isSameFact ? col.getEqualsKeyForSameFact() : col.getEqualsKeyForDiffFact();
        return colWrapper;
    }

    public FactColumn getCol() {
        return col;
    }

    public static class SameFactColumnEqualityWrapper extends FactColumnEqualityWrapper {

        @Override
        public boolean equals(Object obj) {
            SameFactColumnEqualityWrapper that = this;
            if (obj == null || !(obj instanceof SameFactColumnEqualityWrapper)) {
                return false;
            }
            SameFactColumnEqualityWrapper other = (SameFactColumnEqualityWrapper) obj;
            return that == other || (that.col.getEqualsKeyForSameFact().equals(other.col.getEqualsKeyForSameFact()));
        }

        @Override
        public int hashCode() {
            return col.getEqualsKeyForSameFact().hashCode();
        }
    }

    public static class DiffFactColumnEqualityWrapper extends FactColumnEqualityWrapper {


        @Override
        public boolean equals(Object obj) {

            DiffFactColumnEqualityWrapper that = this;
            if (obj == null || !(obj instanceof DiffFactColumnEqualityWrapper)) {
                return false;
            }
            DiffFactColumnEqualityWrapper other = (DiffFactColumnEqualityWrapper) obj;

            String thatColumnKey = that.col.getEqualsKeyForDiffFact();
            String otherColumnKey = other.col.getEqualsKeyForDiffFact();

            thatColumnKey = getColumnPart(thatColumnKey);
            otherColumnKey = getColumnPart(otherColumnKey);

            return that == other || (thatColumnKey.equals(otherColumnKey));

        }

        @Override
        public int hashCode() {
            String key = col.getEqualsKeyForDiffFact();
            key = getColumnPart(key);
            return key.hashCode();
        }
    }

    private static String getColumnPart(String key) {
        Preconditions.checkNotNull(key, "Key can not be null");
        String[] keyParts = key.split("\\.", 2);
        if(keyParts.length == 2) {
            return keyParts[1];
        }
        return key;
    }

}