package com.cookpad.prism.batch;

import java.io.IOException;

import com.cookpad.prism.dao.PrismTable;
import com.cookpad.prism.dao.PrismTableMapper;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Component
@Slf4j
public class DropTableCmd {
    private final PrismTableMapper tableMapper;

    public void run(int tableId) throws IOException {
        PrismTable table = this.tableMapper.find(tableId);
        if (table == null) {
            throw new RuntimeException(String.format("No table found for id: %d", tableId));
        }
        log.info("Dropping table: {}.{}", table.getLogicalSchemaName(), table.getLogicalTableName());
        this.tableMapper.drop(tableId);
    }
}
