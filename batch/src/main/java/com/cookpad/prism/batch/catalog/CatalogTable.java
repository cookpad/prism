package com.cookpad.prism.batch.catalog;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

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
        return StorageDescriptor.builder()
            .columns(columns)
            .location(location)
            .inputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
            .outputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
            .serdeInfo(SerDeInfo.builder()
                .serializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .parameters(SERDE_INFO_PARAMS)
                .build()
            )
            .build();
    }

    private StorageDescriptor buildTableStorageDescriptor() {
        List<Column> columns = this.schema.getColumns()
            .stream()
            .map(col -> Column.builder()
                .name(col.getName())
                .type(col.getValueType().toRedshiftTypeName())
                .build()
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
        return TableInput.builder()
            .name(schema.getTableName())
            .partitionKeys(Column.builder()
                .name("dt")
                .type("date")
                .build()
            )
            .storageDescriptor(storageDescriptor)
            .parameters(TABLE_PARAMS)
            .build();
    }

    public PartitionInput buildPartitionInput(LocalDate dt, boolean isSwitched, long manifestVersion) {
        StorageDescriptor storageDescriptor;
        // 過去に登録されたが merge もされず catalog に反映も
        if (manifestVersion > 0 && isSwitched) {
            storageDescriptor = this.buildMergedPartitionStorageDescriptor(dt, manifestVersion);
        } else {
            storageDescriptor = this.buildLivePartitionStorageDescriptor(dt);
        }
        return PartitionInput.builder()
            .values(this.tableLocator.formatDt(dt))
            .storageDescriptor(storageDescriptor)
            .build();
    }

    public DatabaseInput buildDatabaseInput() {
        return DatabaseInput.builder()
            .name(this.getDatabaseName())
            .build();
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
