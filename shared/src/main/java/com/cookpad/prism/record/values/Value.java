package com.cookpad.prism.record.values;

import org.apache.parquet.io.api.RecordConsumer;

import lombok.Getter;

public interface Value {
    public void writeField(RecordConsumer consumer);

    @SuppressWarnings("serial")
    public static class WriteFieldException extends RuntimeException {
        @Getter
        private String columName = null;
        @Getter
        private Object value = null;

        public WriteFieldException(String message, Throwable cause) {
            super(message, cause);
        }

        public WriteFieldException(String columnName, Object value, Throwable cause) {
            super(String.format("Encountered problem in writing: value '%s' in column '%s'", value, columnName), cause);
            this.columName = columnName;
            this.value = value;
        }
    }
}
