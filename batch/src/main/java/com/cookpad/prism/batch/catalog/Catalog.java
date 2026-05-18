package com.cookpad.prism.batch.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Component
@Slf4j
public class Catalog {
    final private GlueClient glue;

    private List<Database> getDatabases() {
        List<Database> databases = new ArrayList<>();
        String nextToken = null;
        do {
            GetDatabasesRequest getDatabasesRequest = GetDatabasesRequest.builder()
                .maxResults(1000)
                .nextToken(nextToken)
                .build();
            log.debug("Catalog: getDatabases");
            GetDatabasesResponse result = this.glue.getDatabases(getDatabasesRequest);
            databases.addAll(result.databaseList());
            nextToken = result.nextToken();
        } while (nextToken != null);
        return databases;
    }

    private List<Table> getTables(String databaseName) {
        List<Table> tables = new ArrayList<>();
        String nextToken = null;
        do {
            GetTablesRequest getTablesRequest = GetTablesRequest.builder()
                .databaseName(databaseName)
                .maxResults(1000)
                .nextToken(nextToken)
                .build();
            log.debug("Catalog: getTables in {}", databaseName);
            GetTablesResponse result = this.glue.getTables(getTablesRequest);
            tables.addAll(result.tableList());
            nextToken = result.nextToken();
        } while (nextToken != null);
        return tables;
    }

    private void updateDatabases(Set<DatabaseInput> databaseInputs) {
        List<Database> actualDatabases = this.getDatabases();
        Set<String> actualDatabaseNames = actualDatabases
            .stream()
            .map(Database::name)
            .collect(Collectors.toSet())
        ;
        Map<String, DatabaseInput> desiredDatabaseInputs = databaseInputs
            .stream()
            .collect(Collectors.toMap(
                DatabaseInput::name,
                Function.identity()
            ))
        ;

        for (Map.Entry<String, DatabaseInput> pair : desiredDatabaseInputs.entrySet()) {
            String newDatabaseName = pair.getKey();
            if (actualDatabaseNames.contains(newDatabaseName)) {
                // already exists
                continue;
            }
            CreateDatabaseRequest createDatabaseRequest = CreateDatabaseRequest.builder()
                .databaseInput(pair.getValue())
                .build();
            log.debug("Catalog: createDatabase in {}", newDatabaseName);
            this.glue.createDatabase(createDatabaseRequest);
        }
    }

    private boolean compareTableAndTableInput(Table table, TableInput tableInput) {
        StorageDescriptor actualSD = table.storageDescriptor();
        StorageDescriptor expectedSD = tableInput.storageDescriptor();
        if (!expectedSD.columns().equals(actualSD.columns())) {
            return false;
        }
        if (!expectedSD.location().equals(actualSD.location())) {
            return false;
        }
        if (!expectedSD.inputFormat().equals(actualSD.inputFormat())) {
            return false;
        }
        if (!expectedSD.outputFormat().equals(actualSD.outputFormat())) {
            return false;
        }
        if (!expectedSD.serdeInfo().equals(actualSD.serdeInfo())) {
            return false;
        }
        return true;
    }

    private void updateTablesInDatabase(String databaseName, List<TableInput> tableInputsInDatabase) {
        List<Table> actualTables = this.getTables(databaseName);
        Map<String, Table> actualNameToTable = actualTables
            .stream()
            .collect(Collectors.toMap(Table::name, Function.identity()));
        List<UpdateTableRequest> updateTableRequests = new ArrayList<>();
        List<CreateTableRequest> createTableRequests = new ArrayList<>();
        for (TableInput tableInput : tableInputsInDatabase) {
            String tableName = tableInput.name();
            if (actualNameToTable.containsKey(tableName)) {
                Table table = actualNameToTable.get(tableName);
                boolean isSame = this.compareTableAndTableInput(table, tableInput);
                if (isSame) {
                    log.debug("Catalog: skip updateTable");
                } else {
                    UpdateTableRequest updateTableRequest = UpdateTableRequest.builder()
                        .databaseName(databaseName)
                        .tableInput(tableInput)
                        .build();
                    updateTableRequests.add(updateTableRequest);
                }
            } else {
                CreateTableRequest createTableRequest = CreateTableRequest.builder()
                    .databaseName(databaseName)
                    .tableInput(tableInput)
                    .build();
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
            String databaseName = pairOfdatabaseAndTables.getKey().name();
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
