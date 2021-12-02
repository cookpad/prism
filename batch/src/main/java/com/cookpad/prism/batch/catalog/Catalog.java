package com.cookpad.prism.batch.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreatePartitionRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Component
@Slf4j
public class Catalog {
    final private AWSGlue glue;

    private List<Database> getDatabases() {
        List<Database> databases = new ArrayList<>();
        String nextToken = null;
        do {
            GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest()
                .withMaxResults(1000)
                .withNextToken(nextToken)
            ;
            log.debug("Catalog: getDatabases");
            GetDatabasesResult result = this.glue.getDatabases(getDatabasesRequest);
            databases.addAll(result.getDatabaseList());
            nextToken = result.getNextToken();
        } while (nextToken != null);
        return databases;
    }

    private List<Table> getTables(String databaseName) {
        List<Table> tables = new ArrayList<>();
        String nextToken = null;
        do {
            GetTablesRequest getTablesRequest = new GetTablesRequest()
                .withDatabaseName(databaseName)
                .withMaxResults(1000)
                .withNextToken(nextToken)
            ;
            log.debug("Catalog: getTables in {}", databaseName);
            GetTablesResult result = this.glue.getTables(getTablesRequest);
            tables.addAll(result.getTableList());
            nextToken = result.getNextToken();
        } while (nextToken != null);
        return tables;
    }

    private void updateDatabases(Set<DatabaseInput> databaseInputs) {
        List<Database> actualDatabases = this.getDatabases();
        Set<String> actualDatabaseNames = actualDatabases
            .stream()
            .map(Database::getName)
            .collect(Collectors.toSet())
        ;
        Map<String, DatabaseInput> desiredDatabaseInputs = databaseInputs
            .stream()
            .collect(Collectors.toMap(
                DatabaseInput::getName,
                Function.identity()
            ))
        ;

        for (Map.Entry<String, DatabaseInput> pair : desiredDatabaseInputs.entrySet()) {
            String newDatabaseName = pair.getKey();
            if (actualDatabaseNames.contains(newDatabaseName)) {
                // already exists
                continue;
            }
            CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest()
                .withDatabaseInput(pair.getValue())
            ;
            log.debug("Catalog: createDatabase in {}", newDatabaseName);
            this.glue.createDatabase(createDatabaseRequest);
        }
    }

    private boolean compareTableAndTableInput(Table table, TableInput tableInput) {
        StorageDescriptor actualSD = table.getStorageDescriptor();
        StorageDescriptor expectedSD = tableInput.getStorageDescriptor();
        if (!expectedSD.getColumns().equals(actualSD.getColumns())) {
            return false;
        }
        if (!expectedSD.getLocation().equals(actualSD.getLocation())) {
            return false;
        }
        if (!expectedSD.getInputFormat().equals(actualSD.getInputFormat())) {
            return false;
        }
        if (!expectedSD.getOutputFormat().equals(actualSD.getOutputFormat())) {
            return false;
        }
        if (!expectedSD.getSerdeInfo().equals(actualSD.getSerdeInfo())) {
            return false;
        }
        return true;
    }

    private void updateTablesInDatabase(String databaseName, List<TableInput> tableInputsInDatabase) {
        List<Table> actualTables = this.getTables(databaseName);
        Map<String, Table> actualNameToTable = actualTables
            .stream()
            .collect(Collectors.toMap(Table::getName, Function.identity()));
        List<UpdateTableRequest> updateTableRequests = new ArrayList<>();
        List<CreateTableRequest> createTableRequests = new ArrayList<>();
        for (TableInput tableInput : tableInputsInDatabase) {
            String tableName = tableInput.getName();
            if (actualNameToTable.containsKey(tableName)) {
                Table table = actualNameToTable.get(tableName);
                boolean isSame = this.compareTableAndTableInput(table, tableInput);
                if (isSame) {
                    log.debug("Catalog: skip updateTable");
                } else {
                    UpdateTableRequest updateTableRequest = new UpdateTableRequest()
                        .withDatabaseName(databaseName)
                        .withTableInput(tableInput)
                    ;
                    updateTableRequests.add(updateTableRequest);
                }
            } else {
                CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withDatabaseName(databaseName)
                    .withTableInput(tableInput)
                ;
                createTableRequests.add(createTableRequest);
            }
        }

        for (UpdateTableRequest updateTableRequest : updateTableRequests) {
            log.info("Catalog: updateTable {}", updateTableRequest);
            this.glue.updateTable(updateTableRequest);
        }
        for (CreateTableRequest createTableRequest : createTableRequests) {
            log.info("Catalog: createTable {}", createTableRequest);
            this.glue.createTable(createTableRequest);
        }
    }

    public void upsertTables(List<CatalogTable> catalogTables) {
        Map<DatabaseInput, List<TableInput>> databaseToTables = catalogTables.stream()
            .collect(Collectors.groupingBy(CatalogTable::buildDatabaseInput,
                Collectors.mapping(CatalogTable::buildTableInput, Collectors.toList())))
        ;
        this.updateDatabases(databaseToTables.keySet());
        for (Map.Entry<DatabaseInput, List<TableInput>> pairOfdatabaseAndTables : databaseToTables.entrySet()) {
            String databaseName = pairOfdatabaseAndTables.getKey().getName();
            List<TableInput> tableInputs = pairOfdatabaseAndTables.getValue();
            this.updateTablesInDatabase(databaseName, tableInputs);
        }
    }

    public void upsertPartition(UpsertPartitionRequest upsertPartitionRequest) {
        GetPartitionRequest getPartitionRequest = upsertPartitionRequest.buildGetPartitionRequest();
        try {
            log.debug("Catalog: getPartition {}", getPartitionRequest);
            this.glue.getPartition(getPartitionRequest);
            UpdatePartitionRequest updatePartitionRequest = upsertPartitionRequest.buildUpdatePartitionRequest();
            log.info("Catalog: updatePartition {}", updatePartitionRequest);
            this.glue.updatePartition(updatePartitionRequest);
        } catch (EntityNotFoundException e) {
            CreatePartitionRequest createPartitionRequest = upsertPartitionRequest.buildCreatePartitionRequest();
            log.info("Catalog: createPartition {}", createPartitionRequest);
            this.glue.createPartition(createPartitionRequest);
        }
    }
}
