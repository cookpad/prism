package com.cookpad.prism.batch.catalog;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DatabaseNameModifier {
    final private String prefix;
    final private String suffix;

    public String getDatabaseName(String schemaName) {
        return String.format("%s%s%s", this.prefix, schemaName, this.suffix);
    }
}
