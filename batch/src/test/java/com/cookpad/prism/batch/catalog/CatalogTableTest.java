package com.cookpad.prism.batch.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import com.cookpad.prism.SchemaBuilder;
import com.cookpad.prism.SchemaBuilder.BadColumnsError;
import com.cookpad.prism.objectstore.PrismTableLocatorFactory;
import com.cookpad.prism.dao.PrismTable;
import com.cookpad.prism.dao.StreamColumn;
import org.junit.jupiter.api.Test;

import lombok.val;

public class CatalogTableTest {
    @Test
    void getTablePrefix() throws BadColumnsError {
        val databaseModifier = new DatabaseNameModifier("prefix_", "_suffix");
        val prismTable = new PrismTable(200, null, null, "test_schema", "nanika_log", LocalDateTime.now(), 43200);
        val columns = List.of(new StreamColumn(0, "utc_event_time", null, "timestamp", null, "+00:00", "+09:00", null, true));
        val schema = new SchemaBuilder().build(prismTable, columns);
        val locatorFactory = new PrismTableLocatorFactory("prism-sandbox", "global-prefix/");
        val catalogTable = new CatalogTable.Factory(databaseModifier).build(schema, locatorFactory.build(prismTable));
        assertEquals("nanika_log", catalogTable.buildTableInput().getName());
        assertEquals("prefix_test_schema_suffix", catalogTable.buildDatabaseInput().getName());
    }

    @Test
    void getTablePrefixWhenPhysicalNameIsSpecified() throws BadColumnsError {
        val databaseModifier = new DatabaseNameModifier("prefix_", "_suffix");
        val prismTable = new PrismTable(200, "phy_test_schema", "phy_nanika_log", "test_schema", "nanika_log", LocalDateTime.now(), 43200);
        val columns = List.of(new StreamColumn(0, "utc_event_time", null, "timestamp", null, "+00:00", "+09:00", null, true));
        val schema = new SchemaBuilder().build(prismTable, columns);
        val locatorFactory = new PrismTableLocatorFactory("prism-sandbox", "global-prefix/");
        val catalogTable = new CatalogTable.Factory(databaseModifier).build(schema, locatorFactory.build(prismTable));
        assertEquals("nanika_log", catalogTable.buildTableInput().getName());
        assertEquals("prefix_test_schema_suffix", catalogTable.buildDatabaseInput().getName());
    }
}
