package com.cookpad.prism.batch;

import java.io.IOException;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.DeleteTableRequest;

import com.cookpad.prism.batch.catalog.DatabaseNameModifier;
import com.cookpad.prism.dao.PrismTable;
import com.cookpad.prism.dao.PrismTableMapper;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Component
@Slf4j
public class UnlinkTableCmd {
    private final AWSGlue glue;
    private final PrismTableMapper tableMapper;
    private final DatabaseNameModifier databaseNameModifier;

    public void run(int tableId) throws IOException {
        PrismTable table = this.tableMapper.find(tableId);
        if (table == null) {
            throw new RuntimeException(String.format("No table found for id: %d", tableId));
        }

        String databaseName = this.databaseNameModifier.getDatabaseName(table.getLogicalSchemaName());
        String tableName = table.getLogicalTableName();
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
            .withDatabaseName(databaseName)
            .withName(tableName);
        log.info("Deleting table from Glue: {}.{}", databaseName, tableName);
        this.glue.deleteTable(deleteTableRequest);

        log.info("Unlinking table whose id is {}", tableId);
        this.tableMapper.unlink(tableId);
    }
}
