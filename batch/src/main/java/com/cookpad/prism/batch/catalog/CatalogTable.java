package com.cookpad.prism.batch.catalog;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;

import com.cookpad.prism.objectstore.PrismTableLocator;
import com.cookpad.prism.record.Schema;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CatalogTable {
    private final Schema schema;
    @Getter
    private final String databaseName;
    private final PrismTableLocator tableLocator;

    final static private Map<String, String> SERDE_INFO_PARAMS = new HashMap<>();
    static {{
        SERDE_INFO_PARAMS.put("serialization.format", "1");
    }}

    final static private Map<String, String> TABLE_PARAMS = new HashMap<>();
    static {{
        TABLE_PARAMS.put("classification", "parquet");
    }}

    private StorageDescriptor buildStorageDescriptor(String location, List<Column> columns) {
        return new StorageDescriptor()
            .withColumns(columns)
            .withLocation(location)
            .withInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
            .withOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
            .withSerdeInfo(new SerDeInfo()
                .withSerializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .withParameters(SERDE_INFO_PARAMS)
            )
        ;
    }

    private StorageDescriptor buildTableStorageDescriptor() {
        List<Column> columns = this.schema.getColumns()
            .stream()
            .map(col -> new Column()
                .withName(col.getName())
                .withType(col.getValueType().toRedshiftTypeName())
            )
            .collect(Collectors.toList())
        ;
        String location = this.tableLocator.toFullUrl(this.tableLocator.getTablePrefix()).toString();
        return this.buildStorageDescriptor(location, columns);
    }

    private StorageDescriptor buildPartitionStorageDescriptor(String location) {
        List<Column> columns = new ArrayList<>();
        return this.buildStorageDescriptor(location, columns);
    }

    private StorageDescriptor buildLivePartitionStorageDescriptor(LocalDate dt) {
        String location = this.tableLocator.toFullUrl(this.tableLocator.getLiveObjectPartitionPrefix(dt)).toString();
        return this.buildPartitionStorageDescriptor(location);
    }

    private StorageDescriptor buildMergedPartitionStorageDescriptor(LocalDate dt, long manifestVersion) {
        String location = this.tableLocator.toFullUrl(this.tableLocator.getMergedPartitionManifestKey(dt, manifestVersion)).toString();
        return this.buildPartitionStorageDescriptor(location);
    }

    public String getTableName() {
        return this.schema.getTableName();
    }

    public TableInput buildTableInput() {
        StorageDescriptor storageDescriptor = this.buildTableStorageDescriptor();
        TableInput tableInput = new TableInput()
            .withName(schema.getTableName())
            .withPartitionKeys(new Column()
                .withName("dt")
                .withType("date")
            )
            .withStorageDescriptor(storageDescriptor)
            .withParameters(TABLE_PARAMS)
        ;
        return tableInput;
    }

    public PartitionInput buildPartitionInput(LocalDate dt, boolean isSwitched, long manifestVersion) {
        StorageDescriptor storageDescriptor;
        // 過去に登録されたが merge もされず catalog に反映も
        if (manifestVersion > 0 && isSwitched) {
            storageDescriptor = this.buildMergedPartitionStorageDescriptor(dt, manifestVersion);
        } else {
            storageDescriptor = this.buildLivePartitionStorageDescriptor(dt);
        }
        PartitionInput partitionInput = new PartitionInput()
            .withValues(this.tableLocator.formatDt(dt))
            .withStorageDescriptor(storageDescriptor)
        ;
        return partitionInput;
    }

    public DatabaseInput buildDatabaseInput() {
        DatabaseInput databaseInput = new DatabaseInput()
            .withName(this.getDatabaseName())
        ;
        return databaseInput;
    }

    public UpsertPartitionRequest buildUpsertPartitionRequest(LocalDate dt, boolean isSwitched, long manifestVersion) {
        return new UpsertPartitionRequest(this.getDatabaseName(), this.schema.getTableName(), this.buildPartitionInput(dt, isSwitched, manifestVersion));
    }

    @RequiredArgsConstructor
    @Component
    public static class Factory {
        private final DatabaseNameModifier databaseNameModifier;

        public CatalogTable build(Schema schema, PrismTableLocator tableLocator) {
            String databaseName = this.databaseNameModifier.getDatabaseName(schema.getSchemaName());
            return new CatalogTable(schema, databaseName, tableLocator);
        }
    }
}
