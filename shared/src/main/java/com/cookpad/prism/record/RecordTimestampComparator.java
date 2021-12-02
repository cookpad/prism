package com.cookpad.prism.record;

import java.io.Serializable;
import java.util.Comparator;

import com.cookpad.prism.record.values.NonNullValue;
import com.cookpad.prism.record.values.PrimitiveValue;
import com.cookpad.prism.record.values.Value;
import com.cookpad.prism.record.values.PrimitiveValue.LongValue;

public class RecordTimestampComparator implements Comparator<Record>, Serializable {
    private static final long serialVersionUID = 1L;

    private long getUnixTimestamp(Record o) {
        Value tsValue = o.getValue(Schema.TIMESTAMP_INDEX);
        if (!(tsValue instanceof NonNullValue)) {
            throw new RuntimeException("value of timestamp column is null");
        }
        PrimitiveValue<?> pValue = ((NonNullValue)tsValue).getInner();
        if (!(pValue instanceof LongValue)) {
            throw new RuntimeException("value of timestamp column is not a LongValue");
        }
        return ((LongValue)pValue).getValue();
    }

    @Override
    public int compare(Record o1, Record o2) {
        // null is greater
        if (o1 == null) {
            if (o2 == null) {
                return 0;
            }
            return 1;
        }
        if (o2 == null) {
            return -1;
        }
        return Long.compare(this.getUnixTimestamp(o1), this.getUnixTimestamp(o2));
    }
}
