package com.cookpad.prism.objectstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;

import com.cookpad.prism.dao.PrismTable;
import org.junit.jupiter.api.Test;

import lombok.val;

public class S3TableLocatorTest {
    @Test
    void getTablePrefix() {
        val prismTable = new PrismTable(200, null, null, "test_schema", "nanika_log", LocalDateTime.now(), 43200);
        val locatorFactory = new PrismTableLocatorFactory("prism-sandbox", "global-prefix/");
        val tablePrefix = locatorFactory.build(prismTable).getTablePrefix();
        assertEquals("global-prefix/b601.test_schema.nanika_log/", tablePrefix);
    }

    @Test
    void getTablePrefixWithPhysicalName() {
        val prismTable = new PrismTable(200, "phy_test_schema", "phy_nanika_log", "test_schema", "nanika_log", LocalDateTime.now(), 43200);
        val locatorFactory = new PrismTableLocatorFactory("prism-sandbox", "global-prefix/");
        val tablePrefix = locatorFactory.build(prismTable).getTablePrefix();
        assertEquals("global-prefix/342b.phy_test_schema.phy_nanika_log/", tablePrefix);
    }
}
